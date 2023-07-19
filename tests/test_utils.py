from unittest.mock import patch

import pytest

from argo_jupyter_scheduler.utils import authenticate


@pytest.mark.parametrize(
    "env, expected_namespace, expected_token, expected_host, exception",
    [
        # Test with missing env variables
        ({}, None, None, None, KeyError),
        # Test with valid env variables
        (
            {
                "ARGO_NAMESPACE": "test_namespace",
                "ARGO_TOKEN": "Bearer mytoken",
                "ARGO_BASE_HREF": "my_base_href",
                "ARGO_SERVER": "my_server",
            },
            "test_namespace",
            "mytoken",
            "https://my_server/my_base_href/",
            None,
        ),
        # Test with edge cases
        (
            {
                "ARGO_NAMESPACE": "",
                "ARGO_TOKEN": "mytoken",
                "ARGO_BASE_HREF": "my_base_href_without_slash",
                "ARGO_SERVER": "my_server",
            },
            "dev",
            "mytoken",
            "https://my_server/my_base_href_without_slash/",
            None,
        ),
        # Test without bearer in token
        (
            {
                "ARGO_NAMESPACE": "test_namespace",
                "ARGO_TOKEN": "mytoken_without_bearer",
                "ARGO_BASE_HREF": "my_base_href",
                "ARGO_SERVER": "my_server",
            },
            "test_namespace",
            "mytoken_without_bearer",
            "https://my_server/my_base_href/",
            None,
        ),
    ],
)
def test_authenticate(
    env, expected_namespace, expected_token, expected_host, exception
):
    with patch.dict("os.environ", env):
        if exception:
            with pytest.raises(exception):
                authenticate()
        else:
            config = authenticate()
            assert config.namespace == expected_namespace
            assert config.token == expected_token
            assert config.host == expected_host
