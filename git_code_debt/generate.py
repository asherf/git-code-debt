from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import contextlib
import io
import itertools
import multiprocessing.pool
import os.path

import six

from git_code_debt import options
from git_code_debt.database import WriteableDatabaseLogic
from git_code_debt.discovery import get_metric_parsers_from_args
from git_code_debt.file_diff_stat import get_file_diff_stats_from_output
from git_code_debt.generate_config import GenerateOptions
from git_code_debt.repo_parser import RepoParser
from git_code_debt.util import yaml


def get_metrics(commit, diff, metric_parsers, exclude):
    def get_all_metrics(file_diff_stats):
        for metric_parser_cls in metric_parsers:
            metric_parser = metric_parser_cls()
            for metric in metric_parser.get_metrics_from_stat(
                commit, file_diff_stats,
            ):
                yield metric

    file_diff_stats = get_file_diff_stats_from_output(diff)
    file_diff_stats = tuple(
        x for x in file_diff_stats
        if not exclude.search(x.path)
    )
    return tuple(get_all_metrics(file_diff_stats))


def increment_metrics(metric_values, metric_mapping, metrics):
    metric_values.update({metric_mapping[m.name]: m.value for m in metrics})


def _get_metrics_inner(mp_args):
    compare_commit, commit, repo_parser, metric_parsers, exclude = mp_args
    if compare_commit is None:
        diff = repo_parser.get_original_commit(commit.sha)
    else:
        diff = repo_parser.get_commit_diff(compare_commit.sha, commit.sha)
    return get_metrics(commit, diff, metric_parsers, exclude)


@contextlib.contextmanager
def mapper(jobs):
    if jobs == 1:
        yield map
    else:
        with contextlib.closing(multiprocessing.Pool(jobs)) as pool:
            yield pool.imap


def load_data(
        database_file,
        repo,
        package_names,
        skip_defaults,
        exclude,
        jobs,
):
    metric_parsers = get_metric_parsers_from_args(package_names, skip_defaults)

    with WriteableDatabaseLogic.for_sqlite(database_file) as db_logic:
        metric_mapping = db_logic.get_metric_mapping()
        has_data = db_logic.get_metric_has_data()

        repo_parser = RepoParser(repo)

        with repo_parser.repo_checked_out():
            previous_sha = db_logic.get_previous_sha()
            commits = repo_parser.get_commits(since_sha=previous_sha)

            # If there is nothing to check gtfo
            if len(commits) == 1 and previous_sha is not None:
                return

            # Maps metric_id to a running value
            metric_values = collections.Counter()

            # Grab the state of our metrics at the last place
            compare_commit = None
            if previous_sha is not None:
                compare_commit = commits.pop(0)
                metric_values.update(db_logic.get_metric_values(compare_commit.sha))

            mp_args = six.moves.zip(
                [compare_commit] + commits,
                commits,
                itertools.repeat(repo_parser),
                itertools.repeat(metric_parsers),
                itertools.repeat(exclude),
            )
            with mapper(jobs) as do_map:
                for commit, metrics in six.moves.zip(
                        commits, do_map(_get_metrics_inner, mp_args),
                ):
                    db_logic.update_has_data(metrics, metric_mapping, has_data)
                    increment_metrics(metric_values, metric_mapping, metrics)
                    db_logic.insert_metric_values(metric_values, has_data, commit)
                    db_logic.insert_metric_changes(metrics, metric_mapping, commit)


def get_metrics_info(metric_parsers):
    metrics_info = set()
    for metric_parser_cls in metric_parsers:
        metrics_info.update(metric_parser_cls().get_metrics_info())
    return sorted(metrics_info)


def populate_metric_ids(db_logic, package_names, skip_defaults):
    metric_parsers = get_metric_parsers_from_args(package_names, skip_defaults)
    metrics_info = get_metrics_info(metric_parsers)
    db_logic.insert_metrics_info(metrics_info)


def create_database(args):
    with WriteableDatabaseLogic.for_sqlite(args.database) as db_logic:
        db_logic.create_schema()
        populate_metric_ids(
            db_logic,
            args.metric_package_names,
            args.skip_default_metrics,
        )


def get_options_from_config(config_filename):
    if not os.path.exists(config_filename):
        print('config file not found {}'.format(config_filename))
        exit(1)

    with io.open(config_filename) as config_file:
        return GenerateOptions.from_yaml(yaml.load(config_file))


def main(argv=None):
    parser = argparse.ArgumentParser()
    options.add_generate_config_filename(parser)
    parser.add_argument(
        '-j', '--jobs', type=int, default=multiprocessing.cpu_count(),
    )
    parsed_args = parser.parse_args(argv)
    args = get_options_from_config(parsed_args.config_filename)

    if not os.path.exists(args.database):
        create_database(args)

    load_data(
        args.database,
        args.repo,
        args.metric_package_names,
        args.skip_default_metrics,
        args.exclude,
        parsed_args.jobs,
    )


if __name__ == '__main__':
    exit(main())
