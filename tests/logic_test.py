from __future__ import absolute_import
from __future__ import unicode_literals

from git_code_debt.discovery import get_metric_parsers
from git_code_debt.generate import get_metrics_info
from git_code_debt.repo_parser import Commit


def test_get_metric_mapping(sandbox):
    with sandbox.db_logic() as db_logic:
        ret = db_logic.get_metric_mapping()

        expected = {m.name for m in get_metrics_info(get_metric_parsers())}
        assert set(ret) == expected


def test_get_previous_sha_no_previous_sha(sandbox):
    with sandbox.db_logic() as db_logic:
        ret = db_logic.get_previous_sha()
        assert ret is None


def insert_fake_metrics(db_logic):
    metric_mapping = db_logic.get_metric_mapping()
    has_data = dict.fromkeys(metric_mapping.values(), True)
    for v, sha_part in enumerate('abc', 1):
        metric_values = dict.fromkeys(metric_mapping.values(), v)
        commit = Commit(sha_part * 40, 1)
        db_logic.insert_metric_values(metric_values, has_data, commit)


def test_get_previous_sha_previous_existing_sha(sandbox):
    with sandbox.db_logic(writeable=True) as db_logic:
        insert_fake_metrics(db_logic)
        ret = db_logic.get_previous_sha()
        assert ret == 'c' * 40


def test_insert_and_get_metric_values(sandbox):
    with sandbox.db_logic(writeable=True) as db_logic:
        fake_metrics = dict.fromkeys(db_logic.get_metric_mapping().values(), 2)
        insert_fake_metrics(db_logic)
        assert fake_metrics == db_logic.get_metric_values('b' * 40)
