import json
import os
import shutil
from multiprocessing import Process
from pathlib import Path
from typing import Dict, Union
from urllib.parse import urljoin

import psutil
import urllib3
from hera.shared import global_config
from hera.workflows import Container, CronWorkflow, SecretEnv, Step, Steps, Workflow
from hera.workflows.models import ContinueOn
from hera.workflows.service import WorkflowsService
from jupyter_scheduler.exceptions import IdempotencyTokenError, InputUriError, SchedulerError
from jupyter_scheduler.executors import ExecutionManager
from jupyter_scheduler.models import (
    CreateJob,
    CreateJobDefinition,
    DescribeJob,
    DescribeJobDefinition,
    JobFeature,
    Status,
    UpdateJob,
    UpdateJobDefinition,
)
from jupyter_scheduler.orm import Job, JobDefinition
from jupyter_scheduler.scheduler import Scheduler
from jupyter_scheduler.task_runner import JobDefinitionTask, TaskRunner, UpdateJobDefinitionCache
from jupyter_server.transutils import _i18n
from traitlets import Bool, Instance
from traitlets import Type as TType
from urllib3.exceptions import ConnectionError

BASIC_LOGGING = "argo-workflows-executor : {}"
CONDA_STORE_TOKEN = "CONDA_STORE_TOKEN"
CONDA_STORE_SERVICE = "CONDA_STORE_SERVICE"


class ArgoTaskRunner(TaskRunner):
    def process_queue(self):
        print(BASIC_LOGGING.format("Start process_queue..."))
        self.log.debug(self.queue)
        while not self.queue.isempty():
            print(BASIC_LOGGING.format("** Processing queue **"))
            task = self.queue.peek()
            cache = self.cache.get(task.job_definition_id)

            if not cache:
                self.queue.pop()
                continue

            cache_run_time = cache.next_run_time
            queue_run_time = task.next_run_time

            if not cache.active or queue_run_time != cache_run_time:
                self.queue.pop()
                continue

            time_diff = self.compute_time_diff(queue_run_time, cache.timezone)

            # if run time is in future
            if time_diff < 0:
                break
            else:
                try:
                    # TODO: check that the Argo CronWorkflow is still running
                    pass
                except Exception as e:
                    self.log.exception(e)
                self.queue.pop()
                run_time = self.compute_next_run_time(cache.schedule, cache.timezone)
                self.cache.update(task.job_definition_id, UpdateJobDefinitionCache(next_run_time=run_time))
                self.queue.push(JobDefinitionTask(job_definition_id=task.job_definition_id, next_run_time=run_time))


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
            create_cron_workflow(
                job,
                self.staging_paths,
                self.job_definition_id,
                schedule,
                timezone,
                db_url=self.db_url,
                use_conda_store_env=self.use_conda_store_env,
            )
        else:
            create_workflow(job, self.staging_paths, db_url=self.db_url, use_conda_store_env=self.use_conda_store_env)

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

    return global_config


def gen_workflow_name(job_id: str):
    return f"js-wf-{job_id}"


def gen_cron_workflow_name(job_definition_id: str):
    return f"js-cwf-{job_definition_id}"


def gen_output_path(input_path: str):
    p = Path(input_path)
    return str(p.parent / "output.ipynb")


def gen_log_path(input_path: str):
    p = Path(input_path)
    return str(p.parent / "logs.txt")


def add_conda_store_envs():
    conda_store_token = SecretEnv(
        name=CONDA_STORE_TOKEN,
        secret_key="conda-store-api-token",
        secret_name="argo-workflows-conda-store-token",
    )

    conda_store_service = SecretEnv(
        name=CONDA_STORE_SERVICE,
        secret_key="conda-store-service-name",
        secret_name="argo-workflows-conda-store-token",
    )

    return [conda_store_token, conda_store_service]


def send_request(api_v1_endpoint):
    token = os.environ[CONDA_STORE_TOKEN]
    conda_store_service_name = os.environ[CONDA_STORE_SERVICE]

    conda_store_service_name, conda_store_service_port = conda_store_service_name.split(":")
    conda_store_endpoint = f"http://{conda_store_service_name}.dev.svc:{conda_store_service_port}/conda-store/api/v1/"
    url = urljoin(conda_store_endpoint, api_v1_endpoint)

    http = urllib3.PoolManager()
    response = http.request("GET", url, headers={"Authorization": f"Bearer {token}"})

    j = json.loads(response.data.decode("UTF-8"))

    try:
        return j["data"]
    except KeyError as e:
        raise ConnectionError(e)


def gen_conda_env_path(conda_env_name: str, use_conda_store_env: bool = True):
    # TODO: validate that `papermill` is in the specified conda environment
    CONDA_ENV_LOCATION = "/opt/conda/envs/{conda_env_name}"
    DEFAULT_ENV = "default"

    if use_conda_store_env:
        CONDA_STORE_ENV_LOCATION = "/home/conda/{env_namespace}/envs/{conda_env_name}"
        try:
            conda_store_envs = send_request("environment")

            available_ns = []
            available_env_names = []
            available_envs = []
            for i in conda_store_envs:
                available_ns.append(i["namespace"]["name"])
                available_env_names.append(i["name"])
                available_envs.append(f'{i["namespace"]["name"]}-{i["name"]}')

            def _valid_env(
                s, available_ns=available_ns, available_env_names=available_env_names, available_envs=available_envs
            ):
                parts = s.split('-')
                for i in range(1, len(parts)):
                    namespace = '-'.join(parts[:i])
                    name = '-'.join(parts[i:])
                    if namespace in available_ns and name in available_env_names and s in available_envs:
                        return namespace, name
                return False

            env_namespace_name = _valid_env(conda_env_name)
            if env_namespace_name:
                env_namespace, _ = env_namespace_name
                return CONDA_STORE_ENV_LOCATION.format(env_namespace=env_namespace, conda_env_name=conda_env_name)

        except ConnectionError as e:
            print(BASIC_LOGGING.format(f"Unable to connect to conda-store. Encountered error:\n{e}"))

    else:
        conda_env_path = Path(CONDA_ENV_LOCATION.format(conda_env_name=conda_env_name))
        if conda_env_path.exists():
            return conda_env_path

    print(BASIC_LOGGING.format(f"Conda environment `{conda_env_name}` not found, falling back to `{DEFAULT_ENV}`."))
    return CONDA_ENV_LOCATION.format(conda_env_name=DEFAULT_ENV)


def gen_papermill_command_input(conda_env_name: str, input_path: str, use_conda_store_env: bool = True):
    # TODO: allow overrides
    kernel_name = "python3"

    output_path = gen_output_path(input_path)
    log_path = gen_log_path(input_path)
    conda_env_path = gen_conda_env_path(conda_env_name, use_conda_store_env)

    print(BASIC_LOGGING.format(f"conda_env_name: {conda_env_name}"))
    print(BASIC_LOGGING.format(f"conda_env_path: {conda_env_path}"))
    print(BASIC_LOGGING.format(f"output_path: {output_path}"))
    print(BASIC_LOGGING.format(f"log_path: {log_path}"))

    return [f"conda run -p {conda_env_path} papermill -k {kernel_name} {input_path} {output_path}", "&>", log_path]


UPDATE_JOB_STATUS_FAILURE_SCRIPT = """
from jupyter_scheduler.orm import create_session, Job;
from jupyter_scheduler.utils import get_utc_timestamp;
from jupyter_scheduler.models import Status;

db_session = create_session(db_url);
with db_session() as session:
    session.query(Job).filter(Job.job_id == job_id).update(
        {"status": Status.FAILED, "status_message": "Workflow failed."}
    );
    session.commit();
"""

UPDATE_JOB_STATUS_SUCCESS_SCRIPT = """
from jupyter_scheduler.orm import create_session, Job;
from jupyter_scheduler.utils import get_utc_timestamp;
from jupyter_scheduler.models import Status;

db_session = create_session(db_url);
with db_session() as session:
    session.query(Job).filter(Job.job_id == job_id).update(
        {"status": Status.COMPLETED, "end_time": get_utc_timestamp()}
    );
    session.commit();
"""


def create_workflow(job: DescribeJob, staging_paths: Dict, db_url: str, use_conda_store_env: bool = True):
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
    env_vars = []
    if use_conda_store_env:
        env_vars.append(add_conda_store_envs())

    main = Container(
        name="main",
        command=["/bin/sh"],
        args=cmd_args,
        env=env_vars,
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
    env_vars = []
    if use_conda_store_env:
        env_vars.append(add_conda_store_envs())

    main = Container(
        name="main",
        command=["/bin/sh"],
        args=cmd_args,
        env=env_vars,
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


class ArgoScheduler(Scheduler):
    use_conda_store_env = Bool(
        default_value=False,
        config=True,
        help="Whether to attempt check if conda environment is available from conda-store.",
    )

    execution_manager_class = TType(
        allow_none=True,
        klass="jupyter_scheduler.executors.ExecutionManager",
        default_value=ArgoExecutor,
        config=True,
        help=_i18n("The execution manager class to use."),
    )

    task_runner_class = TType(
        allow_none=True,
        config=True,
        default_value=ArgoTaskRunner,
        klass="jupyter_scheduler.task_runner.BaseTaskRunner",
        help=_i18n("The class that handles the job creation of scheduled jobs from job definitions."),
    )

    task_runner = Instance(allow_none=True, klass="jupyter_scheduler.task_runner.BaseTaskRunner")

    def create_job(self, model: CreateJob) -> str:
        if not model.job_definition_id and not self.file_exists(model.input_uri):
            raise InputUriError(model.input_uri)

        input_path = os.path.join(self.root_dir, model.input_uri)
        if not self.execution_manager_class.validate(self.execution_manager_class, input_path):
            raise SchedulerError(
                """There is no kernel associated with the notebook. Please open
                    the notebook, select a kernel, and re-submit the job to execute.
                    """
            )

        with self.db_session() as session:
            if model.idempotency_token:
                job = session.query(Job).filter(Job.idempotency_token == model.idempotency_token).first()
                if job:
                    raise IdempotencyTokenError(model.idempotency_token)

            if not model.output_formats:
                model.output_formats = []

            job = Job(**model.dict(exclude_none=True, exclude={"input_uri"}))
            session.add(job)
            session.commit()

            staging_paths = self.get_staging_paths(DescribeJob.from_orm(job))
            self.copy_input_file(model.input_uri, staging_paths["input"])

            p = Process(
                target=self.execution_manager_class(
                    job_id=job.job_id,
                    staging_paths=staging_paths,
                    root_dir=self.root_dir,
                    db_url=self.db_url,
                    use_conda_store_env=self.use_conda_store_env,
                ).process
            )
            p.start()

            job.pid = p.pid
            session.commit()

            job_id = job.job_id

        return job_id

    def create_cron_job(self, model: CreateJob, job_definition_id: str, schedule: str, timezone: str) -> str:
        print(BASIC_LOGGING.format("ArgoScheduler.create_cron_job"))
        if not model.job_definition_id and not self.file_exists(model.input_uri):
            raise InputUriError(model.input_uri)

        input_path = os.path.join(self.root_dir, model.input_uri)
        if not self.execution_manager_class.validate(self.execution_manager_class, input_path):
            raise SchedulerError(
                """There is no kernel associated with the notebook. Please open
                    the notebook, select a kernel, and re-submit the job to execute.
                    """
            )

        with self.db_session() as session:
            if model.idempotency_token:
                job = session.query(Job).filter(Job.idempotency_token == model.idempotency_token).first()
                if job:
                    raise IdempotencyTokenError(model.idempotency_token)

            if not model.output_formats:
                model.output_formats = []

            job = Job(**model.dict(exclude_none=True, exclude={"input_uri"}))
            session.add(job)
            session.commit()

            staging_paths = self.get_staging_paths(DescribeJob.from_orm(job))
            self.copy_input_file(model.input_uri, staging_paths["input"])

            p = Process(
                target=self.execution_manager_class(
                    job_id=job.job_id,
                    staging_paths=staging_paths,
                    root_dir=self.root_dir,
                    db_url=self.db_url,
                    job_definition_id=job_definition_id,
                    schedule=schedule,
                    timezone=timezone,
                    use_conda_store_env=self.use_conda_store_env,
                ).process
            )
            p.start()

            job.tags = [" Cron-Job; to remove, delete Job Definition "]

            job.pid = p.pid
            session.commit()

            job_id = job.job_id

        return job_id

    def update_job(self, job_id: str, model: UpdateJob):
        print(BASIC_LOGGING.format("ArgoScheduler.update_job"))
        with self.db_session() as session:
            session.query(Job).filter(Job.job_id == job_id).update(model.dict(exclude_none=True))
            session.commit()

    def delete_job(self, job_id: str, delete_associated_workflow=True):
        print(BASIC_LOGGING.format("ArgoScheduler.delete_job"))
        with self.db_session() as session:
            job_record = session.query(Job).filter(Job.job_id == job_id).one()
            if Status(job_record.status) == Status.IN_PROGRESS:
                self.stop_job(job_id)

            staging_paths = self.get_staging_paths(DescribeJob.from_orm(job_record))
            if staging_paths:
                path = os.path.dirname(next(iter(staging_paths.values())))
                if os.path.exists(path):
                    shutil.rmtree(path)

            session.query(Job).filter(Job.job_id == job_id).delete()
            session.commit()

        if delete_associated_workflow:
            delete_workflow(job_id)

    def stop_job(self, job_id: str):
        print(BASIC_LOGGING.format("ArgoScheduler.stop_job"))
        with self.db_session() as session:
            job_record = session.query(Job).filter(Job.job_id == job_id).one()
            job = DescribeJob.from_orm(job_record)
            process_id = job_record.pid
            if process_id and job.status == Status.IN_PROGRESS:
                session.query(Job).filter(Job.job_id == job_id).update({"status": Status.STOPPING})
                session.commit()

                current_process = psutil.Process()
                children = current_process.children(recursive=True)
                for proc in children:
                    if process_id == proc.pid:
                        proc.kill()
                        session.query(Job).filter(Job.job_id == job_id).update({"status": Status.STOPPED})
                        session.commit()
                        break

    def create_job_definition(self, model: CreateJobDefinition) -> str:
        print(BASIC_LOGGING.format("ArgoScheduler.create_job_definition"))
        with self.db_session() as session:
            if not self.file_exists(model.input_uri):
                raise InputUriError(model.input_uri)

            job_definition = JobDefinition(**model.dict(exclude_none=True, exclude={"input_uri"}))

            job_definition.tags = [" Cron-Job; to remove, delete Job Definition "]

            session.add(job_definition)
            session.commit()

            job_definition_id = job_definition.job_definition_id

            staging_paths = self.get_staging_paths(DescribeJobDefinition.from_orm(job_definition))
            self.copy_input_file(model.input_uri, staging_paths["input"])

        if self.task_runner and job_definition.schedule:
            self.task_runner.add_job_definition(job_definition_id)

        self.create_cron_job_from_definition(job_definition_id)

        return job_definition_id

    def update_job_definition(self, job_definition_id: str, model: UpdateJobDefinition):
        print(BASIC_LOGGING.format("ArgoScheduler.update_job_definition"))
        with self.db_session() as session:
            filtered_query = session.query(JobDefinition).filter(JobDefinition.job_definition_id == job_definition_id)

            describe_job_definition = DescribeJobDefinition.from_orm(filtered_query.one())

            if (
                (
                    not model.input_uri
                    or (model.input_uri and describe_job_definition.input_filename == os.path.basename(model.input_uri))
                )
                and describe_job_definition.schedule == model.schedule
                and describe_job_definition.timezone == model.timezone
                and (model.active == None or describe_job_definition.active == model.active)
            ):
                return

            updates = model.dict(exclude_none=True, exclude={"input_uri"})

            if model.input_uri:
                new_input_filename = os.path.basename(model.input_uri)
                staging_paths = self.get_staging_paths(describe_job_definition)
                staging_directory = os.path.dirname(staging_paths["input"])
                self.copy_input_file(model.input_uri, os.path.join(staging_directory, new_input_filename))
                updates["input_filename"] = new_input_filename

            filtered_query.update(updates)
            session.commit()

            schedule = (
                session.query(JobDefinition.schedule)
                .filter(JobDefinition.job_definition_id == job_definition_id)
                .scalar()
            )

        if self.task_runner and schedule:
            self.task_runner.update_job_definition(job_definition_id, model)

    def delete_job_definition(self, job_definition_id: str):
        print(BASIC_LOGGING.format("ArgoScheduler.delete_job_definition"))
        with self.db_session() as session:
            jobs = session.query(Job).filter(Job.job_definition_id == job_definition_id)
            for job in jobs:
                # Deleting the CronWorkflow below we delete all associated workflows at once
                self.delete_job(job.job_id, delete_associated_workflow=False)

            schedule = (
                session.query(JobDefinition.schedule)
                .filter(JobDefinition.job_definition_id == job_definition_id)
                .scalar()
            )

            session.query(JobDefinition).filter(JobDefinition.job_definition_id == job_definition_id).delete()
            session.commit()

        if self.task_runner and schedule:
            self.task_runner.delete_job_definition(job_definition_id)

        delete_cron_workflow(job_definition_id)

    def create_cron_job_from_definition(self, job_definition_id: str) -> str:
        print(BASIC_LOGGING.format("ArgoScheduler.create_cron_job_from_definition"))
        job_id = None
        definition = self.get_job_definition(job_definition_id)
        schedule = definition.schedule
        timezone = definition.timezone
        if definition:
            input_uri = self.get_staging_paths(definition)["input"]
            attributes = definition.dict(exclude={"schedule", "timezone"}, exclude_none=True)
            attributes = {**attributes, "input_uri": input_uri}
            job_id = self.create_cron_job(CreateJob(**attributes), job_definition_id, schedule, timezone)

        return job_id
