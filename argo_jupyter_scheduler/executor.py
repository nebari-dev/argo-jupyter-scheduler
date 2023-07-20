import os
from typing import Dict, Union

from hera.workflows import Container, CronWorkflow, Env, Step, Steps, Workflow, script
from hera.workflows.models import ContinueOn, TTLStrategy, WorkflowStopRequest
from hera.workflows.service import WorkflowsService
from jupyter_scheduler.executors import ExecutionManager
from jupyter_scheduler.models import (
    CreateJob,
    DescribeJob,
    DescribeJobDefinition,
    JobFeature,
    Status,
)
from jupyter_scheduler.orm import Job, JobDefinition, create_session
from jupyter_scheduler.utils import get_utc_timestamp

from argo_jupyter_scheduler.utils import (
    WorkflowActionsEnum,
    authenticate,
    gen_cron_workflow_name,
    gen_papermill_command_input,
    gen_workflow_name,
    sanitize_label,
    setup_logger,
)

logger = setup_logger(__name__)

DEFAULT_TTL = 600


class ArgoExecutor(ExecutionManager):
    def __init__(
        self,
        action: str,
        db_url: str,
        root_dir: Union[str, None] = None,
        staging_paths: Union[Dict[str, str], None] = None,
        job_id: Union[str, None] = None,
        job_definition_id: Union[str, None] = None,
        parameters: Union[Dict[str, str], None] = None,
        schedule: Union[str, None] = None,
        timezone: Union[str, None] = None,
        active: bool = True,
        use_conda_store_env: bool = False,
    ):
        self.root_dir = root_dir
        self.db_url = db_url
        self.staging_paths = staging_paths
        self.action = action
        self.job_id = job_id
        self.job_definition_id = job_definition_id
        self.parameters = parameters
        self.schedule = schedule
        self.timezone = timezone
        self.active = active
        self.use_conda_store_env = use_conda_store_env

        if (
            not self.job_id
            and not self.job_definition_id
            or self.job_id
            and self.job_definition_id
        ):
            msg = "Must provide either `job_id` or `job_definition_id`, not both or neither."
            raise ValueError(msg)

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
                        .filter(
                            JobDefinition.job_definition_id == self.job_definition_id
                        )
                        .first()
                    )
                    self._model = DescribeJobDefinition.from_orm(job_definition)
        return self._model

    def execute(self):
        model = self.model

        if self.job_id:
            if self.action == WorkflowActionsEnum.create:
                self.create_workflow(
                    job=model,
                    parameters=self.parameters,
                    staging_paths=self.staging_paths,
                    db_url=self.db_url,
                    use_conda_store_env=self.use_conda_store_env,
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
                    job=model,
                    staging_paths=self.staging_paths,
                    job_definition_id=self.job_definition_id,
                    parameters=self.parameters,
                    schedule=self.schedule,
                    timezone=self.timezone,
                    db_url=self.db_url,
                    use_conda_store_env=self.use_conda_store_env,
                )
            elif self.action == WorkflowActionsEnum.update:
                self.update_cron_workflow(
                    job=model,
                    staging_paths=self.staging_paths,
                    job_definition_id=self.job_definition_id,
                    schedule=self.schedule,
                    timezone=self.timezone,
                    active=self.active,
                    db_url=self.db_url,
                    use_conda_store_env=self.use_conda_store_env,
                )

            elif self.action == WorkflowActionsEnum.delete:
                self.delete_cron_workflow(self.job_definition_id)

            else:
                ValueError(f"The action `{self.action}` is invalid for cron-workflows.")

    def supported_features(cls) -> Dict[JobFeature, bool]:  # noqa: N805
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

    def validate(cls, input_path: str) -> bool:  # noqa: N805, ARG002
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

    def create_workflow(
        self,
        job: DescribeJob,
        parameters: Dict[str, str],
        staging_paths: Dict,
        db_url: str,
        use_conda_store_env: bool = True,
    ):
        authenticate()

        logger.info("creating workflow...")

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-id": job.job_id,
            "workflows.argoproj.io/creator-preferred-username": sanitize_label(
                os.environ["PREFERRED_USERNAME"]
            ),
        }
        cmd_args = [
            "-c",
            *gen_papermill_command_input(
                job.runtime_environment_name,
                staging_paths["input"],
                use_conda_store_env,
            ),
        ]
        envs = []
        if parameters:
            for key, value in parameters.items():
                envs.append(Env(name=key, value=value))

        main = Container(
            name="main",
            command=["/bin/sh"],
            args=cmd_args,
            env=envs,
        )

        ttl_strategy = TTLStrategy(
            seconds_after_completion=DEFAULT_TTL,
            seconds_after_success=DEFAULT_TTL,
            seconds_after_failure=DEFAULT_TTL,
        )

        failure = "{{steps.main.status}} == Failed"
        successful = "{{steps.main.status}} == Succeeded"

        with Workflow(
            name=gen_workflow_name(job.job_id),
            entrypoint="steps",
            labels=labels,
            ttl_strategy=ttl_strategy,
        ) as w:
            with Steps(name="steps"):
                Step(name="main", template=main, continue_on=ContinueOn(failed=True))
                update_job_status_failure(
                    name="failure",
                    arguments={"db_url": db_url, "job_id": job.job_id},
                    when=failure,
                )
                update_job_status_success(
                    name="success",
                    arguments={"db_url": db_url, "job_id": job.job_id},
                    when=successful,
                )

        w.create()

        logger.info("workflow created")

    def delete_workflow(self, job_id: str):
        global_config = authenticate()

        logger.info("deleting workflow...")

        try:
            wfs = WorkflowsService()
            wfs.delete_workflow(
                name=gen_workflow_name(job_id),
                namespace=global_config.namespace,
            )
        except Exception as e:
            # Hera-Workflows raises generic Exception for all errors :(
            if str(e).startswith("Server returned status code"):
                logger.info(e)
            else:
                raise e

        logger.info("workflow deleted")

    def stop_workflow(self, job_id):
        global_config = authenticate()

        logger.info("stopping workflow...")

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
                logger.info(e)
            else:
                raise e

        logger.info("workflow stopped")

    def _create_cwf_oject(
        self,
        job: DescribeJobDefinition,
        parameters: Dict[str, str],
        staging_paths: Dict,
        job_definition_id: str,
        schedule: str,
        timezone: str,
        db_url: str,
        active: bool = True,
        use_conda_store_env: bool = True,
    ):
        # Argo-Workflow verbage vs Jupyter-Scheduler verbage
        suspend = not active

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-definition-id": job_definition_id,
            "workflows.argoproj.io/creator-preferred-username": sanitize_label(
                os.environ["PREFERRED_USERNAME"]
            ),
        }
        cmd_args = [
            "-c",
            *gen_papermill_command_input(
                job.runtime_environment_name,
                staging_paths["input"],
                use_conda_store_env,
            ),
        ]
        envs = []
        if parameters:
            for key, value in parameters.items():
                envs.append(Env(name=key, value=value))

        main = Container(
            name="main",
            command=["/bin/sh"],
            args=cmd_args,
            env=envs,
        )
        ttl_strategy = TTLStrategy(
            seconds_after_completion=DEFAULT_TTL,
            seconds_after_success=DEFAULT_TTL,
            seconds_after_failure=DEFAULT_TTL,
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
            cron_suspend=suspend,
            labels=labels,
            ttl_strategy=ttl_strategy,
        ) as cwf:
            with Steps(name="steps"):
                create_job_record(
                    name="create-job-id",
                    arguments={
                        "model": model,
                        "db_url": db_url,
                        "job_definition_id": job_definition_id,
                    },
                )
                Step(name="main", template=main, continue_on=ContinueOn(failed=True))
                update_job_status_failure(
                    name="failure",
                    arguments={
                        "db_url": db_url,
                        "job_definition_id": job_definition_id,
                    },
                    when=failure,
                )
                update_job_status_success(
                    name="success",
                    arguments={
                        "db_url": db_url,
                        "job_definition_id": job_definition_id,
                    },
                    when=successful,
                )

        return cwf

    def create_cron_workflow(
        self,
        job: DescribeJobDefinition,
        staging_paths: Dict,
        job_definition_id: str,
        parameters: Dict[str, str],
        schedule: str,
        timezone: str,
        db_url: str,
        use_conda_store_env: bool = True,
    ):
        authenticate()

        logger.info("creating cron workflow...")

        w = self._create_cwf_oject(
            job=job,
            parameters=parameters,
            staging_paths=staging_paths,
            job_definition_id=job_definition_id,
            schedule=schedule,
            timezone=timezone,
            db_url=db_url,
            use_conda_store_env=use_conda_store_env,
        )

        w.create()

        logger.info("cron workflow created")

    def delete_cron_workflow(self, job_definition_id: str):
        global_config = authenticate()

        logger.info("deleting cron workflow...")

        try:
            wfs = WorkflowsService()
            wfs.delete_cron_workflow(
                name=gen_cron_workflow_name(job_definition_id),
                namespace=global_config.namespace,
            )
        except Exception as e:
            # Hera-Workflows raises generic Exception for all errors :(
            if str(e).startswith("Server returned status code"):
                logger.info(e)
            else:
                raise e

        logger.info("cron workflow deleted")

    def update_cron_workflow(
        self,
        job: DescribeJobDefinition,
        staging_paths: Dict,
        job_definition_id: str,
        schedule: str,
        timezone: str,
        active: bool,
        db_url: str,
        use_conda_store_env: bool = True,
    ):
        authenticate()

        logger.info("updating cron workflow...")

        # when the job definition is paused/resumed, schedule and timezone are not provided
        if schedule is None and timezone is None:
            db_session = create_session(db_url)
            with db_session() as session:
                job_definition = (
                    session.query(JobDefinition)
                    .filter(JobDefinition.job_definition_id == job_definition_id)
                    .first()
                )
                schedule = job_definition.schedule
                timezone = job_definition.timezone

        w = self._create_cwf_oject(
            job=job,
            parameters=self.parameters,
            staging_paths=staging_paths,
            job_definition_id=job_definition_id,
            schedule=schedule,
            timezone=timezone,
            db_url=db_url,
            active=active,
            use_conda_store_env=use_conda_store_env,
        )

        try:
            w.update()
        except Exception as e:
            # Hera-Workflows raises generic Exception for all errors :(
            if str(e).startswith("Server returned status code"):
                logger.info(e)
            else:
                raise e

        logger.info("cron workflow updated")


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
                .filter(Job.job_definition_id == job_definition_id)
                .order_by(desc(Job.start_time))
                .first()
            )
            job_id = job.job_id

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
                .filter(Job.job_definition_id == job_definition_id)
                .order_by(desc(Job.start_time))
                .first()
            )
            job_id = job.job_id

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
    from jupyter_scheduler.models import CreateJob, Status
    from jupyter_scheduler.orm import Job, create_session
    from jupyter_scheduler.utils import get_utc_timestamp

    model = CreateJob(**model)

    db_session = create_session(db_url)
    with db_session() as session:
        if model.idempotency_token:
            job = (
                session.query(Job)
                .filter(Job.idempotency_token == model.idempotency_token)
                .first()
            )
            if job:
                raise IdempotencyTokenError(model.idempotency_token)

        if not model.output_formats:
            model.output_formats = []

        job = Job(**model.dict(exclude_none=True, exclude={"input_uri"}))
        job.job_definition_id = job_definition_id
        job.status = Status.IN_PROGRESS
        job.start_time = get_utc_timestamp()

        session.add(job)
        session.commit()
