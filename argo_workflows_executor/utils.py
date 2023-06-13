import json
import os
from enum import Enum
from pathlib import Path
from urllib.parse import urljoin

import urllib3
from hera.shared import global_config
from urllib3.exceptions import ConnectionError

# TODO: determine how to properly send logs to Jupyter server
BASIC_LOGGING = "argo-workflows-executor : {}"

CONDA_STORE_TOKEN = "CONDA_STORE_TOKEN"
CONDA_STORE_SERVICE = "CONDA_STORE_SERVICE"


class WorkflowActionsEnum(Enum):
    create = "create"
    update = "update"
    delete = "delete"
    stop = "stop"


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
    return f"job-{job_id}"


def gen_cron_workflow_name(job_definition_id: str):
    return f"job-def-{job_definition_id}"


def gen_output_path(input_path: str):
    p = Path(input_path)
    return str(p.parent / "output.ipynb")


def gen_log_path(input_path: str):
    p = Path(input_path)
    return str(p.parent / "logs.txt")


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
