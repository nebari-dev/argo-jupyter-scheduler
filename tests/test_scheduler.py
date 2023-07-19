from unittest.mock import patch

from jupyter_scheduler.models import CreateJobDefinition
from jupyter_scheduler.orm import JobDefinition

from argo_jupyter_scheduler.executor import ArgoExecutor
from argo_jupyter_scheduler.scheduler import ArgoScheduler
from argo_jupyter_scheduler.task_runner import ArgoTaskRunner


def test_create_argo_scheduler(jp_scheduler, setup_db):
    assert isinstance(jp_scheduler, ArgoScheduler)
    assert isinstance(jp_scheduler.task_runner, ArgoTaskRunner)
    assert jp_scheduler.execution_manager_class.__name__ == ArgoExecutor.__name__
    assert jp_scheduler.db_url == setup_db


def test_create_job_definition(jp_scheduler):
    # Taken from Jupyter-Scheduler tests/test_scheduler.py
    # TODO: test the Process running in the background from the jp_scheduler.execution_manager_class.process()

    with patch("jupyter_scheduler.scheduler.fsspec"):
        with patch(
            "jupyter_scheduler.scheduler.Scheduler.file_exists"
        ) as mock_file_exists:
            mock_file_exists.return_value = True
            job_definition_id = jp_scheduler.create_job_definition(
                CreateJobDefinition(
                    input_uri="helloworld.ipynb",
                    runtime_environment_name="default",
                    name="hello world",
                    output_formats=["ipynb"],
                )
            )

    with jp_scheduler.db_session() as session:
        definitions = session.query(JobDefinition).all()
        assert 1 == len(definitions)
        definition = definitions[0]
        assert job_definition_id
        assert job_definition_id == definition.job_definition_id
        assert "helloworld.ipynb" == definition.input_filename
        assert "default" == definition.runtime_environment_name
        assert "hello world" == definition.name
