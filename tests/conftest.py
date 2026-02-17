from pathlib import Path

import pytest

from financial_data_collector.repository import Repository


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def repo(db_path: Path) -> Repository:
    r = Repository(db_path.as_posix())
    r.init_schema()
    return r

