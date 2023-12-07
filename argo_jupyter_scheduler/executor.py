import os
from typing import Dict, Union

from hera.workflows import (
    Container,
    CronWorkflow,
    Env,
    Parameter,
    Steps,
    Workflow,
    script,
)
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
    add_file_logger,
    authenticate,
    gen_cron_workflow_name,
    gen_html_path,
    gen_log_path,
    gen_output_path,
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
        input_path = staging_paths["input"]
        log_path = gen_log_path(input_path)

        # Configure logging to file first
        add_file_logger(logger, log_path)

        authenticate()

        logger.info("creating workflow...")
        logger.info(f"create time: {job.create_time}")
        logger.info(f"staging paths: {staging_paths}")

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-id": job.job_id,
            "workflows.argoproj.io/creator-preferred-username": sanitize_label(
                os.environ["PREFERRED_USERNAME"]
            ),
        }
        envs = []
        if parameters:
            for key, value in parameters.items():
                envs.append(Env(name=key, value=value))
        else:
            parameters = {}

        def main(input_path, log_path):
            output_path = "{{inputs.parameters.output_path}}"
            html_path = "{{inputs.parameters.html_path}}"

            cmd_args = gen_papermill_command_input(
                conda_env_name=job.runtime_environment_name,
                input_path=input_path,
                output_path=output_path,
                html_path=html_path,
                log_path=log_path,
                use_conda_store_env=use_conda_store_env,
            )
            main = Container(
                name="main",
                inputs=[Parameter(name="output_path"), Parameter(name="html_path")],
                command=["/bin/sh"],
                args=["-c", cmd_args],
                env=envs,
            )
            return main

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
            main = main(input_path, log_path)

            with Steps(name="steps"):
                start_time = job.create_time

                output_path = gen_output_path(input_path, start_time)
                html_path = gen_html_path(input_path, start_time)

                main(
                    name="main",
                    arguments=[
                        Parameter(name="output_path", value=output_path),
                        Parameter(name="html_path", value=html_path),
                    ],
                    continue_on=ContinueOn(failed=True),
                )

                token, channel = get_slack_token_channel(parameters)
                if token is not None and channel is not None:
                    send_to_slack(
                        name="send-to-slack",
                        arguments={
                            "token": token,
                            "channel": channel,
                            "file_path": html_path,
                            "log_path": log_path,
                        },
                        when=successful,
                        continue_on=ContinueOn(failed=True),
                    )
                    failure += " || {{steps.send-to-slack.status}} == Failed"
                    successful += " && {{steps.send-to-slack.status}} == Succeeded"

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
        input_path = staging_paths["input"]
        log_path = gen_log_path(input_path)

        # Configure logging to file first
        add_file_logger(logger, log_path)

        # Argo-Workflow verbage vs Jupyter-Scheduler verbage
        suspend = not active

        logger.info(f"create time: {job.create_time}")
        logger.info(f"staging paths: {staging_paths}")

        labels = {
            "jupyterflow-override": "true",
            "jupyter-scheduler-job-definition-id": job_definition_id,
            "workflows.argoproj.io/creator-preferred-username": sanitize_label(
                os.environ["PREFERRED_USERNAME"]
            ),
        }
        envs = []
        if parameters:
            for key, value in parameters.items():
                envs.append(Env(name=key, value=value))
        else:
            parameters = {}

        def main(input_path, log_path):
            output_path = "{{inputs.parameters.output_path}}"
            html_path = "{{inputs.parameters.html_path}}"

            cmd_args = gen_papermill_command_input(
                conda_env_name=job.runtime_environment_name,
                input_path=input_path,
                output_path=output_path,
                html_path=html_path,
                log_path=log_path,
                use_conda_store_env=use_conda_store_env,
            )
            main = Container(
                name="main",
                inputs=[Parameter(name="output_path"), Parameter(name="html_path")],
                command=["/bin/sh"],
                args=["-c", cmd_args],
                env=envs,
            )
            return main

        ttl_strategy = TTLStrategy(
            seconds_after_completion=DEFAULT_TTL,
            seconds_after_success=DEFAULT_TTL,
            seconds_after_failure=DEFAULT_TTL,
        )

        # mimics internals of the `scheduler.create_job_from_definition` method
        attributes = {
            **job.dict(exclude={"schedule", "timezone"}, exclude_none=True),
            "input_uri": input_path,
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
            main = main(input_path, log_path)

            with Steps(name="steps"):
                start_time = get_utc_timestamp()

                output_path = gen_output_path(input_path, start_time)
                html_path = gen_html_path(input_path, start_time)

                create_job_record(
                    name="create-job-id",
                    arguments={
                        "model": model,
                        "db_url": db_url,
                        "job_definition_id": job_definition_id,
                        "start_time": start_time,
                    },
                )

                main(
                    name="main",
                    arguments=[
                        Parameter(name="output_path", value=output_path),
                        Parameter(name="html_path", value=html_path),
                    ],
                    continue_on=ContinueOn(failed=True),
                )

                token, channel = get_slack_token_channel(parameters)
                if token is not None and channel is not None:
                    send_to_slack(
                        name="send-to-slack",
                        arguments={
                            "token": token,
                            "channel": channel,
                            "file_path": html_path,
                            "log_path": log_path,
                        },
                        when=successful,
                        continue_on=ContinueOn(failed=True),
                    )
                    failure += " || {{steps.send-to-slack.status}} == Failed"
                    successful += " && {{steps.send-to-slack.status}} == Succeeded"

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
    start_time,
):
    from jupyter_scheduler.exceptions import IdempotencyTokenError
    from jupyter_scheduler.models import CreateJob, Status
    from jupyter_scheduler.orm import Job, create_session

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
        job.start_time = start_time

        session.add(job)
        session.commit()


def get_slack_token_channel(parameters):
    token = parameters.get("SLACK_TOKEN")
    channel = parameters.get("SLACK_CHANNEL")
    return token, channel


@script()
def send_to_slack(token, channel, file_path, log_path):
    import json

    import requests

    from argo_jupyter_scheduler.utils import add_file_logger, setup_logger

    try:
        logger = setup_logger("send_to_slack")
        add_file_logger(logger, log_path)

        url = "https://slack.com/api/files.upload"

        files = {"file": (os.path.basename(file_path), open(file_path, "rb"))}

        data = {
            "initial_comment": "Attaching new file",
            "channels": channel,
        }

        headers = {
            "Authorization": f"Bearer {token}",
        }

        logger.info(f"Sending to Slack: file: {file_path}, channel: {channel}")
        response = requests.post(
            url, files=files, data=data, headers=headers, timeout=30
        )
        response.raise_for_status()

        response = response.text
        logger.info(f"Slack response: {response}")

        if not json.loads(response).get("ok"):
            msg = "Unexpected response when sending to Slack"
            logger.info(msg)
            raise Exception(msg)

        logger.info("Successfully sent to Slack")

    except Exception as e:
        msg = "Failed to send to Slack"
        logger.info(msg)
        raise Exception(msg) from e
