import pytest
from jupyter_scheduler.orm import create_tables
from jupyter_scheduler.tests.mocks import MockEnvironmentManager

from argo_jupyter_scheduler.scheduler import ArgoScheduler

pytest_plugins = ("jupyter_server.pytest_plugin",)


@pytest.fixture(autouse=True)
def setup_db(tmpdir):
    db_path = tmpdir.join("temp_db.sqlite")
    db_url = f"sqlite:///{db_path}"
    create_tables(db_url, True)

    yield db_url


@pytest.fixture
def jp_scheduler(jp_data_dir, setup_db):
    db_url = setup_db
    return ArgoScheduler(
        db_url=db_url,
        root_dir=str(jp_data_dir),
        environments_manager=MockEnvironmentManager(),
    )
