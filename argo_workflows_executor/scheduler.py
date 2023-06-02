import os
import shutil
from multiprocessing import Process

import psutil
from jupyter_scheduler.exceptions import IdempotencyTokenError, InputUriError, SchedulerError
from jupyter_scheduler.models import (
    CreateJob,
    CreateJobDefinition,
    DescribeJob,
    DescribeJobDefinition,
    Status,
    UpdateJob,
    UpdateJobDefinition,
)
from jupyter_scheduler.orm import Job, JobDefinition
from jupyter_scheduler.scheduler import Scheduler
from jupyter_server.transutils import _i18n
from traitlets import Bool, Instance
from traitlets import Type as TType

from .executor import ArgoExecutor, delete_cron_workflow, delete_workflow
from .task_runner import ArgoTaskRunner
from .utils import BASIC_LOGGING


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
