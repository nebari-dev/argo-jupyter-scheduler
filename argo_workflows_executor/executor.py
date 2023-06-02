import os
from typing import Dict, Union

from hera.workflows import Container, CronWorkflow, Step, Steps, Workflow
from hera.workflows.models import ContinueOn
from hera.workflows.service import WorkflowsService
from jupyter_scheduler.executors import ExecutionManager
from jupyter_scheduler.models import DescribeJob, JobFeature

from .utils import (
    BASIC_LOGGING,
    UPDATE_JOB_STATUS_FAILURE_SCRIPT,
    UPDATE_JOB_STATUS_SUCCESS_SCRIPT,
    authenticate,
    gen_cron_workflow_name,
    gen_papermill_command_input,
    gen_workflow_name,
)


class ArgoExecutor(ExecutionManager):
    def __init__(
        self,
        job_id: str,
        root_dir: str,
        db_url: str,
        staging_paths: Dict[str, str],
        job_definition_id: Union[str, None] = None,
        schedule: Union[str, None] = None,
        timezone: Union[str, None] = None,
        use_conda_store_env: bool = False,
    ):
        self.job_id = job_id
        self.staging_paths = staging_paths
        self.root_dir = root_dir
        self.db_url = db_url
        self.job_definition_id = job_definition_id
        self.schedule = schedule
        self.timezone = timezone
        self.use_conda_store_env = use_conda_store_env

    def execute(self):
        job = self.model
        schedule = self.schedule
        timezone = self.timezone

        print(BASIC_LOGGING.format(f"Input file from staging location: {self.staging_paths['input']}"))
        print(BASIC_LOGGING.format(f"Schedule: {schedule}"))
        print(BASIC_LOGGING.format(f"Timezone: {timezone}"))

        if schedule:
            self.create_cron_workflow(
                job,
                self.staging_paths,
                self.job_definition_id,
                schedule,
                timezone,
                db_url=self.db_url,
                use_conda_store_env=self.use_conda_store_env,
            )
        else:
            self.create_workflow(
                job, self.staging_paths, db_url=self.db_url, use_conda_store_env=self.use_conda_store_env
            )

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

    def on_complete(self):
        # Update status of job via Argo-Workflows script
        pass

    def create_workflow(self, job: DescribeJob, staging_paths: Dict, db_url: str, use_conda_store_env: bool = True):
        authenticate()

        print(BASIC_LOGGING.format("creating workflow..."))

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-id": job.job_id,
            "workflows.argoproj.io/creator-preferred-username": os.environ["PREFERRED_USERNAME"],
        }
        cmd_args = ["-c"] + gen_papermill_command_input(
            job.runtime_environment_name, staging_paths["input"], use_conda_store_env
        )

        main = Container(
            name="main",
            command=["/bin/sh"],
            args=cmd_args,
        )

        failure_script_args = f"""db_url = "{db_url}"; job_id = "{job.job_id}"; """ + UPDATE_JOB_STATUS_FAILURE_SCRIPT
        success_script_args = f"""db_url = "{db_url}"; job_id = "{job.job_id}"; """ + UPDATE_JOB_STATUS_SUCCESS_SCRIPT

        update_job_status_failure = Container(
            name="update-status-failure", command=["python"], args=["-c", failure_script_args]
        )
        update_job_status_success = Container(
            name="update-status-success", command=["python"], args=["-c", success_script_args]
        )

        failure = "{{steps.main.status}} == Failed"
        successful = "{{steps.main.status}} == Succeeded"

        with Workflow(name=gen_workflow_name(job.job_id), entrypoint="steps", labels=labels) as w:
            main_step = Step(name="main", template=main, continue_on=ContinueOn(failed=True))
            failure_step = Step(name="failure-step", template=update_job_status_failure, when=failure)
            success_step = Step(name="success-step", template=update_job_status_success, when=successful)
            Steps(name="steps", sub_steps=[main_step, success_step, failure_step])

        w.create()

        print(BASIC_LOGGING.format("workflow created"))

    def create_cron_workflow(
        self,
        job: DescribeJob,
        staging_paths: Dict,
        job_definition_id: str,
        schedule: str,
        timezone: str,
        db_url: str,
        use_conda_store_env: bool = True,
    ):
        authenticate()

        print(BASIC_LOGGING.format("creating cron workflow..."))

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-definition-id": job_definition_id,
            "workflows.argoproj.io/creator-preferred-username": os.environ["PREFERRED_USERNAME"],
        }
        cmd_args = ["-c"] + gen_papermill_command_input(
            job.runtime_environment_name, staging_paths["input"], use_conda_store_env
        )

        main = Container(
            name="main",
            command=["/bin/sh"],
            args=cmd_args,
        )

        failure_script_args = f"""db_url = "{db_url}"; job_id = "{job.job_id}"; """ + UPDATE_JOB_STATUS_FAILURE_SCRIPT
        success_script_args = f"""db_url = "{db_url}"; job_id = "{job.job_id}"; """ + UPDATE_JOB_STATUS_SUCCESS_SCRIPT

        update_job_status_failure = Container(
            name="update-status-failure", command=["python"], args=["-c", failure_script_args]
        )
        update_job_status_success = Container(
            name="update-status-success", command=["python"], args=["-c", success_script_args]
        )

        failure = "{{steps.main.status}} == Failed"
        successful = "{{steps.main.status}} == Succeeded"

        with CronWorkflow(
            name=gen_cron_workflow_name(job_definition_id),
            entrypoint="steps",
            schedule=schedule,
            timezone=timezone,
            starting_deadline_seconds=0,
            concurrency_policy="Replace",
            successful_jobs_history_limit=4,
            failed_jobs_history_limit=4,
            cron_suspend=False,
            labels=labels,
        ) as w:
            main_step = Step(name="main", template=main, continue_on=ContinueOn(failed=True))
            failure_step = Step(name="failure-step", template=update_job_status_failure, when=failure)
            success_step = Step(name="success-step", template=update_job_status_success, when=successful)
            Steps(name="steps", sub_steps=[main_step, success_step, failure_step])

        w.create()

        print(BASIC_LOGGING.format("cron workflow created"))

        return w


def delete_workflow(job_id: str):
    global_config = authenticate()

    print(BASIC_LOGGING.format("deleting workflow..."))

    try:
        wfs = WorkflowsService()
        wfs.delete_workflow(
            name=gen_workflow_name(job_id),
            namespace=global_config.namespace,
        )
    except Exception as e:
        # Hera-Workflows raises generic Exception for all errors :(
        if str(e).startswith("Server returned status code"):
            print(BASIC_LOGGING.format(e))
        else:
            raise e

    print(BASIC_LOGGING.format("workflow deleted"))


def delete_cron_workflow(job_definition_id: str):
    global_config = authenticate()

    print(BASIC_LOGGING.format("deleting cron workflow..."))

    try:
        wfs = WorkflowsService()
        wfs.delete_cron_workflow(
            name=gen_cron_workflow_name(job_definition_id),
            namespace=global_config.namespace,
        )
    except Exception as e:
        # Hera-Workflows raises generic Exception for all errors :(
        if str(e).startswith("Server returned status code"):
            print(BASIC_LOGGING.format(e))
        else:
            raise e

    print(BASIC_LOGGING.format("cron workflow deleted"))
