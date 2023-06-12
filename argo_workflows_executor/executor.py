import os
from typing import Dict, Union

from hera.workflows import Container, CronWorkflow, Step, Steps, Workflow, script
from hera.workflows.models import ContinueOn, WorkflowStopRequest
from hera.workflows.service import WorkflowsService
from jupyter_scheduler.executors import ExecutionManager
from jupyter_scheduler.models import CreateJob, DescribeJob, DescribeJobDefinition, JobFeature, Status
from jupyter_scheduler.orm import Job, JobDefinition
from jupyter_scheduler.utils import get_utc_timestamp

from .utils import (
    BASIC_LOGGING,
    WorkflowActionsEnum,
    authenticate,
    gen_cron_workflow_name,
    gen_papermill_command_input,
    gen_workflow_name,
)


class ArgoExecutor(ExecutionManager):
    def __init__(
        self,
        action: str,
        db_url: str,
        root_dir: Union[str, None] = None,
        staging_paths: Union[Dict[str, str], None] = None,
        job_id: Union[str, None] = None,
        job_definition_id: Union[str, None] = None,
        schedule: Union[str, None] = None,
        timezone: Union[str, None] = None,
        use_conda_store_env: bool = False,
    ):
        self.root_dir = root_dir
        self.db_url = db_url
        self.staging_paths = staging_paths
        self.action = action
        self.job_id = job_id
        self.job_definition_id = job_definition_id
        self.schedule = schedule
        self.timezone = timezone
        self.use_conda_store_env = use_conda_store_env

        if not self.job_id and not self.job_definition_id or self.job_id and self.job_definition_id:
            raise ValueError("Must provide either `job_id` or `job_definition_id`, not both or neither.")

    @property
    def model(self):
        if self._model is None:
            if self.job_id:
                with self.db_session() as session:
                    job = session.query(Job).filter(Job.job_id == self.job_id).first()
                    self._model = DescribeJob.from_orm(job)
            elif self.job_definition_id:
                with self.db_session() as session:
                    job_definition = (
                        session.query(JobDefinition)
                        .filter(JobDefinition.job_definition_id == self.job_definition_id)
                        .first()
                    )
                    self._model = DescribeJobDefinition.from_orm(job_definition)
        return self._model

    def execute(self):
        model = self.model

        if self.job_id:
            if self.action == WorkflowActionsEnum.create:
                self.create_workflow(
                    model, self.staging_paths, db_url=self.db_url, use_conda_store_env=self.use_conda_store_env
                )

            elif self.action == WorkflowActionsEnum.delete:
                self.delete_workflow(self.job_id)

            elif self.action == WorkflowActionsEnum.stop:
                self.stop_workflow(self.job_id)

            else:
                ValueError(f"The action `{self.action}` is invalid for workflows.")

        elif self.job_definition_id:
            if self.action == WorkflowActionsEnum.create:
                self.create_cron_workflow(
                    model,
                    self.staging_paths,
                    self.job_definition_id,
                    self.schedule,
                    self.timezone,
                    db_url=self.db_url,
                    use_conda_store_env=self.use_conda_store_env,
                )
            elif self.action == WorkflowActionsEnum.update:
                pass

            elif self.action == WorkflowActionsEnum.delete:
                self.delete_cron_workflow(self.job_definition_id)

            else:
                ValueError(f"The action `{self.action}` is invalid for cron-workflows.")

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

    def before_start(self):
        """Called before start of execute"""
        job = self.model
        if self.job_id:
            with self.db_session() as session:
                session.query(Job).filter(Job.job_id == job.job_id).update(
                    {"start_time": get_utc_timestamp(), "status": Status.IN_PROGRESS}
                )
                session.commit()

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

        failure = "{{steps.main.status}} == Failed"
        successful = "{{steps.main.status}} == Succeeded"

        with Workflow(name=gen_workflow_name(job.job_id), entrypoint="steps", labels=labels) as w:
            main_step = Step(name="main", template=main, continue_on=ContinueOn(failed=True))
            failure_script = update_job_status_failure(
                name="failure", arguments={"db_url": db_url, "job_id": job.job_id}, when=failure
            )
            success_script = update_job_status_success(
                name="success", arguments={"db_url": db_url, "job_id": job.job_id}, when=successful
            )

            Steps(name="steps", sub_steps=[main_step, failure_script, success_script])

        w.create()

        print(BASIC_LOGGING.format("workflow created"))

    def delete_workflow(self, job_id: str):
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

    def stop_workflow(self, job_id):
        global_config = authenticate()

        print(BASIC_LOGGING.format("stopping workflow..."))

        try:
            req = WorkflowStopRequest(
                name=gen_workflow_name(job_id),
                namespace=global_config.namespace,
            )

            wfs = WorkflowsService()
            wfs.stop_workflow(
                name=gen_workflow_name(job_id),
                req=req,
                namespace=global_config.namespace,
            )
        except Exception as e:
            # Hera-Workflows raises generic Exception for all errors :(
            if str(e).startswith("Server returned status code"):
                print(BASIC_LOGGING.format(e))
            else:
                raise e

        print(BASIC_LOGGING.format("workflow stopped"))

    def create_cron_workflow(
        self,
        job: DescribeJobDefinition,
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

        # mimics internals of the `scheduler.create_job_from_definition` method
        attributes = {
            **job.dict(exclude={"schedule", "timezone"}, exclude_none=True),
            "input_uri": staging_paths["input"],
        }
        model = CreateJob(**attributes)

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
            create_job_record_script = create_job_record(
                name="create-job-id",
                arguments={
                    "model": model,
                    "db_url": db_url,
                    "job_definition_id": job_definition_id,
                },
            )
            main_step = Step(name="main", template=main, continue_on=ContinueOn(failed=True))
            failure_script = update_job_status_failure(
                name="failure", arguments={"db_url": db_url, "job_definition_id": job_definition_id}, when=failure
            )
            success_script = update_job_status_success(
                name="success", arguments={"db_url": db_url, "job_definition_id": job_definition_id}, when=successful
            )

            Steps(
                name="steps",
                sub_steps=[
                    create_job_record_script,
                    main_step,
                    failure_script,
                    success_script,
                ],
            )

        w.create()

        print(BASIC_LOGGING.format("cron workflow created"))

        return w

    def delete_cron_workflow(self, job_definition_id: str):
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


@script()
def update_job_status_failure(db_url, job_id=None, job_definition_id=None):
    from jupyter_scheduler.models import Status
    from jupyter_scheduler.orm import Job, create_session
    from sqlalchemy import desc

    db_session = create_session(db_url)
    with db_session() as session:
        if job_definition_id:
            # for cron-workflows, update each associated job record
            job = (
                session.query(Job)
                .filter_by(Job.job_definition_id == job_definition_id)
                .order_by(desc(Job.start_time))
                .first()
            )
            job_id = job.id

        session.query(Job).filter(Job.job_id == job_id).update(
            {"status": Status.FAILED, "status_message": "Workflow failed."}
        )
        session.commit()


@script()
def update_job_status_success(db_url, job_id=None, job_definition_id=None):
    from jupyter_scheduler.models import Status
    from jupyter_scheduler.orm import Job, create_session
    from jupyter_scheduler.utils import get_utc_timestamp
    from sqlalchemy import desc

    db_session = create_session(db_url)
    with db_session() as session:
        if job_definition_id:
            # for cron-workflows, update each associated job record
            job = (
                session.query(Job)
                .filter_by(Job.job_definition_id == job_definition_id)
                .order_by(desc(Job.start_time))
                .first()
            )
            job_id = job.id

        session.query(Job).filter(Job.job_id == job_id).update(
            {"status": Status.COMPLETED, "end_time": get_utc_timestamp()}
        )
        session.commit()


@script()
def create_job_record(
    model,
    db_url,
    job_definition_id,
):
    from jupyter_scheduler.exceptions import IdempotencyTokenError
    from jupyter_scheduler.models import CreateJob
    from jupyter_scheduler.orm import Job, create_session

    model = CreateJob(**model)

    print(model)
    print(db_url)
    print(job_definition_id)

    db_session = create_session(db_url)
    with db_session() as session:
        if model.idempotency_token:
            job = session.query(Job).filter(Job.idempotency_token == model.idempotency_token).first()
            if job:
                raise IdempotencyTokenError(model.idempotency_token)

        if not model.output_formats:
            model.output_formats = []

        job = Job(**model.dict(exclude_none=True, exclude={"input_uri"}))
        job.job_definition_id = job_definition_id

        session.add(job)
        session.commit()
