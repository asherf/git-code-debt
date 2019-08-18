from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import os
import sqlite3

import pkg_resources

Metric = collections.namedtuple('Metric', ('value', 'date'))
MetricInfo = collections.namedtuple('MetricInfo', ('id', 'description'))


class DatabaseLogic:

    @classmethod
    def for_tests(cls, tmpdir):
        return cls.for_sqlite(tmpdir.join('db.db').strpath)

    @classmethod
    def for_sqlite(cls, sql_file):
        return cls(sqlite3.connect(sql_file))

    def __init__(self, db):
        self._db = db

    def __enter__(self):
        self._db.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._db.__exit__(exc_type, exc_value, traceback)

    def close(self):
        self._db.close()

    def _fetch_one(self, sql, values=tuple()):
        return self._db.execute(sql, values).fetchone()

    def _fetch_all(self, sql, values=tuple()):
        return self._db.execute(sql, values).fetchall()

    def get_metric_ids(self):
        query = 'SELECT name FROM metric_names WHERE has_data=1 ORDER BY name'
        res = self._fetch_all(query)
        return [name for name, in res]

    def get_metric_info(self, metric_name):
        query = 'SELECT id, description FROM metric_names WHERE name = ?'
        res = self._fetch_one(query, (metric_name,))
        return MetricInfo(*res)

    def get_latest_sha(self):
        query = 'SELECT sha FROM metric_data ORDER BY timestamp DESC LIMIT 1'
        result = self._fetch_one(query)

        # If there is no data result will be None
        return result[0] if result else None

    def get_sha_for_date(self, date):
        result = self._fetch_one(
            '\n'.join((
                'SELECT',
                '    sha',
                'FROM metric_data',
                'WHERE',
                '    timestamp <= ?',
                'ORDER BY timestamp DESC',
                'LIMIT 1',
            )),
            [date],
        )
        # If the date is too far in the past (before data) there won't be a result
        return result[0] if result else None

    def get_metrics_for_sha(self, sha):
        # For no sha, we default all metrics to 0
        if not sha:
            return collections.defaultdict(int)

        result = self._fetch_all(
            'SELECT\n'
            '    metric_names.name,\n'
            '    metric_data.running_value\n'
            'FROM metric_data\n'
            'INNER JOIN metric_names ON\n'
            '    metric_names.id = metric_data.metric_id AND\n'
            '    metric_names.has_data = 1\n'
            'WHERE\n'
            '    metric_data.sha = ?\n',
            [sha],
        )
        return collections.defaultdict(int, result)

    def metrics_for_dates(self, metric_id, dates):
        def get_metric_for_timestamp(timestamp):
            result = self._fetch_one(
                'SELECT running_value, timestamp\n'
                'FROM metric_data\n'
                'WHERE metric_id = ? AND timestamp < ?\n'
                'ORDER BY timestamp DESC\n'
                'LIMIT 1\n',
                (metric_id, timestamp),
            )
            return Metric(*result) if result else Metric(0, timestamp)
        return [get_metric_for_timestamp(date) for date in dates]

    def get_first_data_timestamp(self, metric_name):

        # Find the first change for that metric
        first_timestamp = self._fetch_one(
            'SELECT timestamp\n'
            'FROM metric_data\n'
            'INNER JOIN metric_names ON metric_names.id = metric_data.metric_id\n'
            'WHERE metric_names.name = ?\n'
            'ORDER BY metric_data.ROWID ASC\n'
            'LIMIT 1\n',
            (metric_name,),
        )
        if not first_timestamp:
            return 0
        else:
            return first_timestamp[0]

    def get_metric_changes(self, sha):
        return self._fetch_all(
            '\n'.join((
                'SELECT',
                '    metric_names.name,',
                '    metric_changes.value',
                'FROM metric_changes',
                'INNER JOIN metric_names',
                '    ON metric_changes.metric_id = metric_names.id',
                'WHERE metric_changes.sha = ?',
            )),
            [sha],
        )

    def get_major_changes_for_metric(
            self, start_timestamp, end_timestamp, metric_id,
    ):
        return self._fetch_all(
            '\n'.join((
                'SELECT',
                '    metric_data.timestamp,',
                '    metric_data.sha,',
                '    metric_changes.value',
                'FROM metric_changes',
                'INNER JOIN metric_data ON',
                '    metric_changes.sha = metric_data.sha AND',
                '    metric_changes.metric_id = metric_data.metric_id',
                'WHERE',
                '    metric_data.timestamp >= ? AND',
                '    metric_data.timestamp < ? AND',
                '    metric_changes.metric_id = ?',
                'ORDER BY ABS(metric_changes.value) DESC',
                'LIMIT 50',
            )),
            (start_timestamp, end_timestamp, metric_id),
        )

    def get_metric_mapping(self):
        """Gets a mapping from metric_name to metric_id."""
        results = self._fetch_all('SELECT name, id FROM metric_names')
        return dict(results)

    def get_metric_has_data(self):
        res = self._fetch_all('SELECT id, has_data FROM metric_names')
        return {k: bool(v) for k, v in res}

    def get_previous_sha(self):
        """Gets the latest inserted SHA."""
        result = self._fetch_one(
            # Use ROWID as a free, auto-incrementing, primary key.
            'SELECT sha FROM metric_data ORDER BY ROWID DESC LIMIT 1',
        )
        return result[0] if result else None

    def get_metric_values(self, sha):
        """Gets the metric values from a specific commit.

        :param db: Database object
        :param text sha: A sha representing a single commit
        """
        results = self._fetch_all(
            'SELECT metric_id, running_value FROM metric_data WHERE sha = ?', (sha,),
        )
        return dict(results)


class WriteableDatabaseLogic(DatabaseLogic):

    def _executemany(self, sql, values):
        self._db.executemany(sql, values)

    def _execute(self, sql, values):
        self._db.execute(sql, values)

    def create_schema(self):
        """Creates the database schema."""
        schema_dir = pkg_resources.resource_filename('git_code_debt', 'schema')
        schema_files = os.listdir(schema_dir)

        for sql_file in schema_files:
            resource_filename = os.path.join(schema_dir, sql_file)
            with open(resource_filename, 'r') as resource:
                self._db.executescript(resource.read())

    def insert_metric_values(self, metric_values, has_data, commit):
        values = [
            (commit.sha, metric_id, commit.date, value)
            for metric_id, value in metric_values.items()
            if has_data[metric_id]
        ]
        self._executemany(
            'INSERT INTO metric_data (sha, metric_id, timestamp, running_value)\n'
            'VALUES (?, ?, ?, ?)\n',
            values,
        )

    def update_has_data(self, metrics, metric_mapping, has_data):
        query = 'UPDATE metric_names SET has_data=1 WHERE id = ?'
        for metric_id in [metric_mapping[m.name] for m in metrics if m.value]:
            if not has_data[metric_id]:
                has_data[metric_id] = True
                self._execute(query, (metric_id,))

    def insert_metrics_info(self, metrics_info):
        query = 'INSERT INTO metric_names (name, description) VALUES (?, ?)'
        self._executemany(query, metrics_info)

    def insert_metric_changes(self, metrics, metric_mapping, commit):
        """Insert into the metric_changes tables.

        :param metrics: `list` of `Metric` objects
        :param dict metric_mapping: Maps metric names to ids
        :param Commit commit:
        """
        values = [
            [commit.sha, metric_mapping[metric.name], metric.value]
            for metric in metrics
            if metric.value != 0
        ]
        self._executemany(
            'INSERT INTO metric_changes (sha, metric_id, value) VALUES (?, ?, ?)',
            values,
        )
