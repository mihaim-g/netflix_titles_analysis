import os
import pytest
from unittest.mock import patch
from src.utils.environment_utils import EnvUtils


@pytest.fixture
def mock_env_vars():
    with patch.dict(os.environ, {
        'AWS_PROFILE': 'mock_aws_profile',
        'TITLES_FILE_PATH': 'mock_titles_file_path',
        'DATAFRAME_DESTINATION': 'mock_dataframe_destination',
        'OVERWRITE_FILE_MODE': 'mock_overwrite_file_mode',
        'ENVIRONMENT': 'mock_environment',
        'S3_ENDPOINT': 'mock_s3_endpoint'
    }):
        yield


def test_init_with_all_env_vars(mock_env_vars):
    obj = EnvUtils()

    assert obj._aws_profile == 'mock_aws_profile'
    assert obj._dataset_file_name == 'mock_titles_file_path'
    assert obj._dataframe_destination == 'mock_dataframe_destination'
    assert obj._dataframe_writing_mode == 'mock_overwrite_file_mode'
    assert obj._env == 'mock_environment'
    assert obj._s3_endpoint == 'mock_s3_endpoint'
