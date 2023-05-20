import os
from typing import Dict
from urllib.parse import urljoin

import fsspec
import nbconvert
import nbformat
from hera.shared import global_config
from hera.workflows import DAG, Workflow, script
from jupyter_scheduler.executors import CellExecutionError, ExecutionManager
from jupyter_scheduler.models import JobFeature
from jupyter_scheduler.parameterize import add_parameters
from nbconvert.preprocessors import ExecutePreprocessor

BASIC_LOGGING = "argo-workflows-executor : {}"


class ArgoExecutor(ExecutionManager):
    def execute(self):
        job = self.model

        print(BASIC_LOGGING.format(f"Input file from staging location: {self.staging_paths['input']}"))

        create_workflow(job, self.staging_paths)

    def supported_features(cls) -> Dict[JobFeature, bool]:
        # TODO: determine if all of these features are actually supported
        return {
            JobFeature.job_name: True,
            JobFeature.output_formats: True,
            JobFeature.job_definition: False,
            JobFeature.idempotency_token: False,
            JobFeature.tags: False,
            JobFeature.email_notifications: False,
            JobFeature.timeout_seconds: False,
            JobFeature.retry_on_timeout: False,
            JobFeature.max_retries: False,
            JobFeature.min_retry_interval_millis: False,
            JobFeature.output_filename_template: False,
            JobFeature.stop_job: True,
            JobFeature.delete_job: True,
        }

    def validate(cls, input_path: str) -> bool:
        # TODO: perform some actual validation
        return True


def authenticate():
    token = os.environ["ARGO_TOKEN"]
    if token.startswith("Bearer"):
        token = token.split(" ")[-1]

    base_href = os.environ["ARGO_BASE_HREF"]
    if not base_href.endswith("/"):
        base_href += "/"

    server = f"https://{os.environ['ARGO_SERVER']}"
    host = urljoin(server, base_href)

    # TODO: allow users to specify or pull in from elsewhere
    namespace = "dev"

    global_config.host = host
    global_config.token = token
    global_config.namespace = namespace


def create_workflow(job, staging_paths):
    authenticate()

    print(BASIC_LOGGING.format("creating workflow..."))

    with Workflow(generate_name="js-test-", entrypoint="D") as w:
        # TODO: replace DAG with Container or standalone script (currently not working)
        with DAG(name="D") as d:
            run_notebook(name="run-notebook", arguments={"job": job, "staging_paths": staging_paths})

    w.create()

    print(BASIC_LOGGING.format("workflow created"))


@script()
def run_notebook(job, staging_paths):
    # TODO: this is a currently just copy of the existing
    # Jupyter-Scheduler DefaultExecutor execute() method,
    # modify this function as needed

    with open(staging_paths["input"], encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    if job.parameters:
        nb = add_parameters(nb, job.parameters)

    ep = ExecutePreprocessor(
        kernel_name=nb.metadata.kernelspec["name"],
        store_widget_state=True,
    )

    try:
        ep.preprocess(nb)
    except CellExecutionError as e:
        raise e
    finally:
        for output_format in job.output_formats:
            cls = nbconvert.get_exporter(output_format)
            output, resources = cls().from_notebook_node(nb)
            with fsspec.open(staging_paths[output_format], "w", encoding="utf-8") as f:
                f.write(output)
