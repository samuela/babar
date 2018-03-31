from pathlib import Path
import pickle
import sys
import time
import warnings

import sqlite3

from .git import is_git_repo, archive_git_repo

# TODO CLI
# TODO logging
# TODO capture stdout/stderr
# TODO web CLI
# TODO matplotlib integration
# TODO pytorch seriealization

def find_babar_root_directory(path):
  for p in [path] + list(path.parents):
    if (p / '.babar.json').exists():
      return p

  # Nothing found, return None
  return None

class MetaHandler(object):
  def freeze(self):
    pass

  def revive(self, val):
    pass

class NumpySeedMetaHandler(MetaHandler):
  def freeze(self):
    if 'numpy' in sys.modules:
      import numpy as np
      return np.random.get_state()
    else:
      return None

  def revive(self, val):
    if val != None:
      import numpy as np
      np.random.set_state(val)

class PythonSeedMetaHandler(MetaHandler):
  def freeze(self):
    if 'random' in sys.modules:
      import random
      return random.getstate()
    else:
      return None

  def revive(self, val):
    if val != None:
      import random
      random.setstate(val)

class PytorchSeedMetaHandler(MetaHandler):
  def freeze(self):
    if 'torch' in sys.modules:
      import torch
      return torch.get_rng_state()
    else:
      return None

  def revive(self, val):
    if val != None:
      import torch
      torch.set_rng_state(val)

class PythonVersionMetaHandler(MetaHandler):
  def freeze(self):
    return sys.version

class PythonArgvMetaHandler(MetaHandler):
  def freeze(self):
    return sys.argv

# class TimestampMetaHandler(MetaHandler):
#   def freeze(self):
#     return time.time()

class BabarServer(object):
  def __init__(self, root_directory):
    self.root_directory = root_directory
    assert (self.root_directory / '.babar.json').exists()

    self.db_conn = self._get_db_conn()

  def _get_db_conn(self):
    # Make the project .babar director if it doesn't already exist.
    (self.root_directory / '.babar').mkdir(exist_ok=True)
    db_path = self.root_directory / '.babar' / 'db.sqlite'

    # If the database file doesn't exist before opening the connection then we
    # need to set it up.
    db_existed = db_path.exists()
    conn = sqlite3.connect(str(db_path))
    if not db_existed:
      self._setup_database(conn)
    return conn

  def _setup_database(self, conn):
    # Timestamps are all in seconds since UTC epoch.
    conn.cursor().executescript('''
    CREATE TABLE sessions (
      _id INTEGER PRIMARY KEY NOT NULL,
      timestamp REAL NOT NULL,
      name TEXT,
      current_script TEXT NOT NULL,
      serialized_initial_props BLOB NOT NULL
    );
    CREATE TABLE checkpoints (
      _id INTEGER PRIMARY KEY NOT NULL,
      timestamp REAL NOT NULL,
      serialized_data BLOB NOT NULL,
      session_id INTEGER NOT NULL,
      FOREIGN KEY(session_id) REFERENCES sessions(_id) ON DELETE CASCADE ON UPDATE CASCADE
    );
    ''')
    conn.commit()

  def insert_session(
      self,
      timestamp,
      name,
      current_script,
      serialized_initial_props
  ):
    """Insert a new session into the database.

    Parameters
    ==========
    timestamp : float
    name : str
    current_script : str
    serialized_initial_props : byte string or anything BLOB-able

    Returns
    =======
    The inserted row id.
    """
    command = '''
    INSERT INTO sessions (timestamp, name, current_script, serialized_initial_props)
    VALUES (?, ?, ?, ?);
    '''
    cursor = self.db_conn.cursor()
    cursor.execute(
      command,
      (timestamp, name, current_script, serialized_initial_props)
    )
    self.db_conn.commit()
    return cursor.lastrowid

  def insert_checkpoint(self, timestamp, serialized_data, session_id):
    command = '''
    INSERT INTO checkpoints (timestamp, serialized_data, session_id)
    VALUES (?, ?, ?);
    '''
    cursor = self.db_conn.cursor()
    cursor.execute(command, (timestamp, serialized_data, session_id))
    self.db_conn.commit()
    return cursor.lastrowid

  def get_checkpoint(self, checkpoint_id):
    command = '''
    SELECT serialized_data, session_id FROM checkpoints WHERE _id=?
    '''
    cursor = self.db_conn.cursor()
    cursor.execute(command, (checkpoint_id,))
    return cursor.fetchone()

class Session(object):
  STATUS_INIT = 'INIT'
  STATUS_BEGUN = 'BEGUN'
  STATUS_REVIVED = 'REVIVED'

  def __init__(self, name=None, meta_handlers=None, pickle_module=pickle):
    super(Session, self).__init__()

    # Lambdas support arbitrary attributes, so they're handy here.
    super(Session, self).__setattr__('_internals', lambda: None)

    _self = self._internals
    _self.name = name
    _self.pickle_module = pickle_module

    if meta_handlers is None:
      _self.meta_handlers = {
        'numpy_seed': NumpySeedMetaHandler(),
        'python_seed': PythonSeedMetaHandler(),
        'pytorch_seed': PytorchSeedMetaHandler(),
        'sys.version': PythonVersionMetaHandler(),
        'sys.argv': PythonArgvMetaHandler()
      }
    else:
      _self.meta_handlers = meta_handlers

    # Our status starts out in the INIT state.
    _self.status = Session.STATUS_INIT

    _self.current_script = Path(sys.argv[0])

    # Root directory will be /.../my_project/ not /.../my_project/.babar/
    root_directory = find_babar_root_directory(Path.cwd())
    if root_directory is None:
      raise Exception(
        'Hmm, I wasn\'t able to find a `.babar.json` configuration file in the '
        'current working directory or any of its parents. Make sure to set up '
        'babar before starting any Sessions!'
      )
    else:
      _self.root_directory = root_directory
    _self.server = BabarServer(_self.root_directory)

  def begin(self, **props):
    _self = self._internals
    if _self.status == Session.STATUS_INIT:
      _self.props = props
      _self.state = {}

      # Insert into the database and get the resulting id.
      _self.session_id = _self.server.insert_session(
        time.time(),
        _self.name,
        str(_self.current_script),
        _self.pickle_module.dumps(_self.props)
      )

      # TODO capture stdout, stderr

      # Set up the directory for this session.
      self._get_session_directory().mkdir(parents=True)

      # archive project state
      # TODO add config option to disable this
      if is_git_repo(_self.root_directory):
        archive_git_repo(
          _self.root_directory,
          self._get_session_directory() / 'project_archived.zip'
        )

      # Finally update our status.
      _self.status = Session.STATUS_BEGUN

      # Support convenient chaining of this call along with the constructor.
      return self
    elif _self.status == Session.STATUS_BEGUN:
      raise Exception('You cannot re-begin a Session!')
    elif _self.status == Session.STATUS_REVIVED:
      raise Exception(
        'You cannot begin a Session after reviving from a previous checkpoint!'
      )

  def revive(self, checkpoint_id):
    # Can revive from any state, but cannot escape revival status.
    _self = self._internals

    # We clear these values in case users attempt to do something silly like
    # revive and then ask for the session name.
    _self.name = None
    _self.current_script = None

    blob, session_id = _self.server.get_checkpoint(checkpoint_id)
    _self.session_id = session_id
    prev = _self.pickle_module.loads(blob)

    # Revive all of the meta states
    for k, v in self.meta_handlers.items():
      v.revive(prev['meta'][k])

    # We need to change these even though they're props because we need the
    # identities of objects to be consistent across props and state. For
    # example, if there's a prop which is a torch.nn.Module and an optimizer
    # in the state, then we need to unpack them together.
    _self.props = prev['payload']['props']
    _self.state = prev['payload']['state']

  def checkpoint(self):
    _self = self._internals
    if _self.status == Session.STATUS_INIT:
      raise Exception(
        'You\'ll need to formally `.begin()` this Session before you can '
        'checkpoint results!'
      )
    elif _self.status == Session.STATUS_BEGUN:
      return _self.server.insert_checkpoint(
        time.time(),
        _self.pickle_module.dumps(self._get_stuff()),
        _self.session_id
      )
    elif _self.status == Session.STATUS_REVIVED:
      warnings.warn(
        'Checkpointing after time traveling a Session to a previous checkpoint '
        'with `.revive()` has no effect!'
      )
      return None

  def __setattr__(self, name, value):
    _self = self._internals
    if _self.status == Session.STATUS_INIT:
      raise Exception(
        'Assigning values to a Session is only allowed after `.begin()`ing or '
        '`.revive()`ing it!'
      )
    else:
      if name in _self.props:
        raise Exception(
          'You cannot assign to a prop after starting a Session! Perhaps '
          'you\'d like to use a state variable instead?'
        )
      _self.state[name] = value

  def __getattr__(self, name):
    # __getattr__ is only called only after `name` is not found normally.
    _self = self._internals
    if _self.status == Session.STATUS_INIT:
      raise Exception(
        'You cannot get props or state variables until `.begin()`ing or '
        '`.revive()`ing a Session!'
      )
    else:
      if name in _self.props:
        return _self.props[name]
      elif name in _self.state:
        return _self.state[name]
      else:
        raise KeyError(
          f'Couldn\'t find a prop or state named `{name}`. Make sure to assign '
          'your state variables before accessing them!'
        )

  def __dir__(self):
    _self = self._internals
    if _self.status == Session.STATUS_INIT:
      return []
    else:
      return list(_self.props.keys()) + list(_self.state.keys())

  def _get_session_directory(self):
    _self = self._internals
    return (
      _self.root_directory / '.babar' / 'sessions' / str(_self.session_id)
    )

  def _get_stuff(self):
    _self = self._internals
    return {
      'meta': {k: v.freeze() for k, v in _self.meta_handlers.items()},
      'payload': {'props': _self.props, 'state': _self.state}
    }
