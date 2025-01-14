import json
from datetime import datetime
from logging import getLogger
from os import listdir, remove
from os.path import isfile
from unittest.mock import MagicMock, patch

import pytest

from app.adapter.repository.decomp import DECOMP
from app.adapter.repository.newave import NEWAVE
from app.models.runstatus import RunStatus
from app.utils.constants import (
    METADATA_FILE,
    METADATA_MODEL_NAME,
    METADATA_MODEL_VERSION,
    METADATA_PARENT_ID,
    METADATA_PARENT_STARTING_DATE,
    METADATA_STATUS,
    METADATA_STUDY_STARTING_DATE,
)

TEST_VERSION = "1.0"
TEST_BUCKET = "my-bucket"
TEST_INPUT = "deck.zip"
TEST_PARENT_ID = "parent-id"
TEST_DATE = datetime(2025, 1, 1)

EXECUTABLE_FILES = [
    METADATA_FILE,
    DECOMP.LICENSE_FILENAME,
    DECOMP.NAMECAST_PROGRAM_NAME,
]

INPUT_FILES = [
    METADATA_FILE,
    NEWAVE.CUT_FILE,
]


@pytest.fixture
def fetching_executables():
    for file in EXECUTABLE_FILES:
        with open(file, "w") as f:
            f.write("{ }")
    yield EXECUTABLE_FILES
    for file in EXECUTABLE_FILES:
        if isfile(file):
            remove(file)


@pytest.fixture
def fetching_inputs():
    for file in INPUT_FILES:
        with open(file, "w") as f:
            f.write("{ }")
    yield INPUT_FILES
    for file in INPUT_FILES:
        if isfile(file):
            remove(file)


def _model_obj() -> DECOMP:
    return DECOMP(logger=getLogger("test"))


@patch(
    "app.adapter.repository.decomp.check_and_download_bucket_items",
    MagicMock(return_value=EXECUTABLE_FILES),
)
def test_decomp_check_and_fetch_executables(fetching_executables):
    model = _model_obj()
    model.check_and_fetch_executables(version=TEST_VERSION, bucket=TEST_BUCKET)
    assert METADATA_FILE in listdir()
    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)
    assert METADATA_MODEL_NAME in metadata
    assert METADATA_MODEL_VERSION in metadata
    assert metadata[METADATA_MODEL_NAME] == DECOMP.MODEL_NAME.upper()
    assert metadata[METADATA_MODEL_VERSION] == TEST_VERSION
    assert all([f in listdir() for f in fetching_executables])


@patch(
    "app.adapter.repository.decomp.check_and_download_bucket_items",
    MagicMock(return_value=INPUT_FILES),
)
@patch(
    "app.adapter.repository.decomp.check_and_delete_bucket_item",
    MagicMock(return_value=None),
)
@patch(
    "app.adapter.repository.decomp.check_and_get_bucket_item",
    lambda bucket, filepath, logger: json.dumps({
        METADATA_MODEL_NAME: NEWAVE.MODEL_NAME.upper(),
        METADATA_STATUS: RunStatus.SUCCESS.value,
        METADATA_STUDY_STARTING_DATE: TEST_DATE.isoformat(),
    }),
)
def test_decomp_check_and_fetch_inputs(fetching_inputs):
    model = _model_obj()
    model.check_and_fetch_inputs(
        filename=TEST_INPUT,
        bucket=TEST_BUCKET,
        parent_id=TEST_PARENT_ID,
        delete=True,
    )
    assert METADATA_FILE in listdir()
    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)
    assert METADATA_PARENT_ID in metadata
    assert METADATA_PARENT_STARTING_DATE in metadata
    assert metadata[METADATA_PARENT_ID] == TEST_PARENT_ID
    assert metadata[METADATA_PARENT_STARTING_DATE] == TEST_DATE.isoformat()
    assert all([f in listdir() for f in fetching_inputs])
