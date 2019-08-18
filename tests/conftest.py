from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import contextlib
import io
import os.path
import subprocess

import pytest
import six

from git_code_debt.database import DatabaseLogic
from git_code_debt.database import WriteableDatabaseLogic
from git_code_debt.generate import populate_metric_ids
from git_code_debt.repo_parser import Commit
from git_code_debt.repo_parser import COMMIT_FORMAT
from git_code_debt.util import yaml
from git_code_debt.util.subprocess import cmd_output
from testing.utilities.auto_namedtuple import auto_namedtuple
from testing.utilities.cwd import cwd


@pytest.fixture
def tempdir_factory(tmpdir):
    class TmpdirFactory(object):
        def __init__(self):
            self.tmpdir_count = 0

        def get(self):
            path = tmpdir.join(six.text_type(self.tmpdir_count)).strpath
            self.tmpdir_count += 1
            os.mkdir(path)
            return path

    yield TmpdirFactory()


class Sandbox(collections.namedtuple('Sandbox', ['directory'])):
    __slots__ = ()

    @property
    def db_path(self):
        return os.path.join(self.directory, 'db.db')

    def gen_config(self, **data):
        path = os.path.join(self.directory, 'generate_config.yaml')
        with io.open(path, 'w') as f:
            yaml.dump(
                dict({'database': self.db_path}, **data),
                f,
                encoding=None,
                default_flow_style=False,
            )
        return path

    @contextlib.contextmanager
    def db_logic(self, writeable=False):
        db_logic_cls = WriteableDatabaseLogic if writeable else DatabaseLogic
        with db_logic_cls.for_sqlite(self.db_path) as db_logic:
            yield db_logic


@pytest.fixture
def sandbox(tempdir_factory):
    ret = Sandbox(tempdir_factory.get())
    with ret.db_logic(writeable=True) as db_logic:
        db_logic.create_schema()
        populate_metric_ids(db_logic, (), False)

    yield ret


@pytest.fixture
def cloneable(tempdir_factory):
    repo_path = tempdir_factory.get()
    with cwd(repo_path):
        subprocess.check_call(('git', 'init', '.'))
        subprocess.check_call(('git', 'commit', '-m', 'foo', '--allow-empty'))

    yield repo_path


@pytest.fixture
def cloneable_with_commits(cloneable):
    commits = []

    def append_commit():
        output = cmd_output('git', 'show', COMMIT_FORMAT)
        sha, date = output.splitlines()[:2]
        commits.append(Commit(sha, int(date)))

    def make_commit(filename, contents):
        # Make the graph tests more deterministic
        # import time; time.sleep(2)
        with io.open(filename, 'w') as file_obj:
            file_obj.write(contents)

        subprocess.check_call(('git', 'add', filename))
        subprocess.check_call((
            'git', 'commit', '-m', 'Add {}'.format(filename),
        ))
        append_commit()

    with cwd(cloneable):
        # Append a commit for the inital commit
        append_commit()
        make_commit('bar.py', '')
        make_commit('baz.py', '')
        make_commit('test.py', 'import foo\nimport bar\n')
        make_commit('foo.tmpl', '#import foo\n#import bar\n')

    yield auto_namedtuple(path=cloneable, commits=commits)
