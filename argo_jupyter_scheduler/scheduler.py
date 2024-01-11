import os
import shutil
from multiprocessing import Process

import psutil
from jupyter_scheduler.exceptions import (
    IdempotencyTokenError,
    InputUriError,
    SchedulerError,
)
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

from argo_jupyter_scheduler.executor import ArgoExecutor
from argo_jupyter_scheduler.task_runner import ArgoTaskRunner
from argo_jupyter_scheduler.utils import WorkflowActionsEnum, setup_logger

logger = setup_logger(__name__)


class ArgoScheduler(Scheduler):
    use_conda_store_env = Bool(
        default_value=False,
        config=True,
        help="Whether to check if conda environment is available from conda-store.",
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
        help=_i18n(
            "The class that handles the job creation of scheduled jobs from job definitions."
        ),
    )

    task_runner = Instance(
        allow_none=True, klass="jupyter_scheduler.task_runner.BaseTaskRunner"
    )

    def create_job(self, model: CreateJob) -> str:
        if not model.job_definition_id and not self.file_exists(model.input_uri):
            raise InputUriError(model.input_uri)

        input_path = os.path.join(self.root_dir, model.input_uri)
        if not self.execution_manager_class.validate(
            self.execution_manager_class, input_path
        ):
            msg = """
            There is no kernel associated with the notebook. Please open\n
            the notebook, select a kernel, and re-submit the job to execute.\n
            """
            raise SchedulerError(msg)

        with self.db_session() as session:
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
            session.add(job)
            session.commit()

            staging_paths = self.get_staging_paths(DescribeJob.from_orm(job))
            self.copy_input_file(model.input_uri, staging_paths["input"])

            p = Process(
                target=self.execution_manager_class(
                    action=WorkflowActionsEnum.create,
                    job_id=job.job_id,
                    parameters=job.parameters,
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

    def update_job(self, job_id: str, model: UpdateJob):
        logger.info("ArgoScheduler.update_job")
        with self.db_session() as session:
            session.query(Job).filter(Job.job_id == job_id).update(
                model.dict(exclude_none=True)
            )
            session.commit()

    def delete_job(self, job_id: str, delete_workflow: bool = True):
        logger.info("ArgoScheduler.delete_job")
        with self.db_session() as session:
            job_record = session.query(Job).filter(Job.job_id == job_id).one()
            if Status(job_record.status) == Status.IN_PROGRESS:
                self.stop_job(job_id)

            staging_paths = self.get_staging_paths(DescribeJob.from_orm(job_record))
            if staging_paths:
                path = os.path.dirname(next(iter(staging_paths.values())))
                if os.path.exists(path):
                    if os.path.islink(path):
                        realpath = os.path.realpath(path)
                        # For cron jobs, job directories in the staging dir are
                        # symlinks that point to the real staging job directory,
                        # which is created when the job is first scheduled.
                        # Since we only have access to the current job
                        # directory, which is a symlink, we first need to find
                        # the other symlinks pointing to the same real directory
                        # and remove them
                        basedir = os.path.dirname(path)
                        for f in next(os.walk(basedir))[1]:
                            f = os.path.join(basedir, f)  # noqa: PLW2901
                            if os.path.islink(f) and os.path.realpath(f) == realpath:
                                os.unlink(f)
                        # Then we remove the real staging job directory, where
                        # these symlinks used to point to
                        shutil.rmtree(realpath)
                    else:
                        shutil.rmtree(path)

            if delete_workflow:
                p = Process(
                    target=self.execution_manager_class(
                        action=WorkflowActionsEnum.delete,
                        job_id=job_id,
                        db_url=self.db_url,
                    ).process
                )
                p.start()
                p.join()

            session.query(Job).filter(Job.job_id == job_id).delete()
            session.commit()

    def stop_job(self, job_id: str):
        logger.info("ArgoScheduler.stop_job")
        with self.db_session() as session:
            job_record = session.query(Job).filter(Job.job_id == job_id).one()
            job = DescribeJob.from_orm(job_record)
            process_id = job_record.pid
            if process_id and job.status == Status.IN_PROGRESS:
                session.query(Job).filter(Job.job_id == job_id).update(
                    {"status": Status.STOPPING}
                )
                session.commit()

                current_process = psutil.Process()
                children = current_process.children(recursive=True)
                for proc in children:
                    if process_id == proc.pid:
                        proc.kill()

                        p = Process(
                            target=self.execution_manager_class(
                                action=WorkflowActionsEnum.stop,
                                job_id=job_id,
                                db_url=self.db_url,
                            ).process
                        )
                        p.start()
                        p.join()

                        session.query(Job).filter(Job.job_id == job_id).update(
                            {"status": Status.STOPPED}
                        )
                        session.commit()
                        break

    def create_job_definition(self, model: CreateJobDefinition) -> str:
        logger.info("ArgoScheduler.create_job_definition")
        with self.db_session() as session:
            if not self.file_exists(model.input_uri):
                raise InputUriError(model.input_uri)

            job_definition = JobDefinition(
                **model.dict(exclude_none=True, exclude={"input_uri"})
            )
            job_definition.tags = [" Cron-Workflow "]

            session.add(job_definition)
            session.commit()

            job_definition_id = job_definition.job_definition_id

            staging_paths = self.get_staging_paths(
                DescribeJobDefinition.from_orm(job_definition)
            )
            self.copy_input_file(model.input_uri, staging_paths["input"])

            if not model.output_formats:
                model.output_formats = []

            p = Process(
                target=self.execution_manager_class(
                    action=WorkflowActionsEnum.create,
                    staging_paths=staging_paths,
                    root_dir=self.root_dir,
                    db_url=self.db_url,
                    job_definition_id=job_definition_id,
                    parameters=job_definition.parameters,
                    schedule=job_definition.schedule,
                    timezone=job_definition.timezone,
                    use_conda_store_env=self.use_conda_store_env,
                ).process
            )
            p.start()

        if self.task_runner and job_definition.schedule:
            self.task_runner.add_job_definition(job_definition_id)

        return job_definition_id

    def update_job_definition(self, job_definition_id: str, model: UpdateJobDefinition):
        logger.info("ArgoScheduler.update_job_definition")
        with self.db_session() as session:
            filtered_query = session.query(JobDefinition).filter(
                JobDefinition.job_definition_id == job_definition_id
            )

            describe_job_definition = DescribeJobDefinition.from_orm(
                filtered_query.one()
            )
            # update schedule/timezone, keep active status if not provided
            if model.active is None:
                model.active = True

            job_definition = JobDefinition(
                **model.dict(exclude_none=True, exclude={"input_uri"})
            )
            job_definition.tags = [" Cron-Workflow "]

            staging_paths = self.get_staging_paths(describe_job_definition)

            if (
                (
                    not model.input_uri
                    or (
                        model.input_uri
                        and describe_job_definition.input_filename
                        == os.path.basename(model.input_uri)
                    )
                )
                and describe_job_definition.schedule == model.schedule
                and describe_job_definition.timezone == model.timezone
                and (
                    model.active is None
                    or describe_job_definition.active == model.active
                )
            ):
                return

            updates = model.dict(exclude_none=True, exclude={"input_uri"})

            if model.input_uri:
                new_input_filename = os.path.basename(model.input_uri)
                staging_paths = self.get_staging_paths(describe_job_definition)
                staging_directory = os.path.dirname(staging_paths["input"])
                self.copy_input_file(
                    model.input_uri, os.path.join(staging_directory, new_input_filename)
                )
                updates["input_filename"] = new_input_filename

            p = Process(
                target=self.execution_manager_class(
                    action=WorkflowActionsEnum.update,
                    staging_paths=staging_paths,
                    root_dir=self.root_dir,
                    db_url=self.db_url,
                    job_definition_id=job_definition_id,
                    schedule=job_definition.schedule,
                    timezone=job_definition.timezone,
                    active=job_definition.active,
                    use_conda_store_env=self.use_conda_store_env,
                ).process
            )
            p.start()

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
        logger.info("ArgoScheduler.delete_job_definition")
        with self.db_session() as session:
            jobs = session.query(Job).filter(Job.job_definition_id == job_definition_id)
            for job in jobs:
                # Deleting the CronWorkflow below we delete all associated workflows at once
                self.delete_job(job.job_id, delete_workflow=False)

            schedule = (
                session.query(JobDefinition.schedule)
                .filter(JobDefinition.job_definition_id == job_definition_id)
                .scalar()
            )

            p = Process(
                target=self.execution_manager_class(
                    action=WorkflowActionsEnum.delete,
                    db_url=self.db_url,
                    job_definition_id=job_definition_id,
                ).process
            )
            p.start()
            p.join()

            session.query(JobDefinition).filter(
                JobDefinition.job_definition_id == job_definition_id
            ).delete()
            session.commit()

        if self.task_runner and schedule:
            self.task_runner.delete_job_definition(job_definition_id)
