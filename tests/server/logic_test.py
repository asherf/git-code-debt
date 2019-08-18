from __future__ import absolute_import
from __future__ import unicode_literals

from git_code_debt.metric import Metric
from git_code_debt.repo_parser import Commit


def test_no_data_returns_zero(sandbox):
    with sandbox.db_logic() as db_logic:
        assert db_logic.get_first_data_timestamp('PythonImportCount') == 0


def insert(db_logic, sha, timestamp, value, has_data=True):
    metric_mapping = db_logic.get_metric_mapping()
    db_logic.insert_metric_values(
        {metric_mapping['PythonImportCount']: value},
        {metric_mapping['PythonImportCount']: has_data},
        Commit(sha, timestamp),
    )


def insert_metric_changes(db_logic, sha, change):
    metric_mapping = db_logic.get_metric_mapping()
    db_logic.insert_metric_changes(
        [Metric('PythonImportCount', change)],
        metric_mapping,
        Commit(sha, None),
    )


def test_some_data_returns_first_timestamp(sandbox):
    with sandbox.db_logic(writeable=True) as db_logic:
        insert(db_logic, '1' * 40, 1111, 0, has_data=False)
        assert db_logic.get_first_data_timestamp('PythonImportCount') == 0


def test_some_data_returns_last_zero_before_data(sandbox):
    with sandbox.db_logic(writeable=True) as db_logic:
        insert(db_logic, '1' * 40, 1111, 0, has_data=False)
        insert(db_logic, '2' * 40, 2222, 0, has_data=False)
        insert(db_logic, '3' * 40, 3333, 1)
        insert_metric_changes(db_logic, '3' * 40, 1)
        assert db_logic.get_first_data_timestamp('PythonImportCount') == 3333


def test_first_commit_introduces_data(sandbox):
    with sandbox.db_logic(writeable=True) as db_logic:
        insert(db_logic, '1' * 40, 1111, 1)
        insert_metric_changes(db_logic, '1' * 40, 1)
        insert(db_logic, '2' * 40, 2222, 2)
        insert_metric_changes(db_logic, '2' * 40, 1)
        assert db_logic.get_first_data_timestamp('PythonImportCount') == 1111
