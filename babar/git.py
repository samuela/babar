import os
from pathlib import Path
import subprocess
import zipfile


def is_git_repo(repo_path):
  gs = subprocess.run(['git', 'status'], cwd=repo_path)
  return gs.returncode == 0

def ls_files(repo_path):
  """List all of the current files in the git repository at `repo_path`. Returns
  relative paths."""
  # Use universal_newlines=True to force subprocess to use the default encoding
  # when working with stdout and stderr. Otherwise they come out as byte
  # strings.
  res = subprocess.run(
    ['git', 'ls-files'],
    cwd=repo_path,
    check=True,
    stdout=subprocess.PIPE,
    universal_newlines=True
  )
  return res.stdout.split()

# Checking individual files is kinda slow. We should really do these in batch.
# Future TODO for someone.
def is_path_ignored(repo_path, path):
  res = subprocess.run(
    ['git', 'check-ignore', '--quiet', str(path)],
    cwd=repo_path
  )
  assert (res.returncode == 0) or (res.returncode == 1)
  return res.returncode == 0

def all_unignored_files(repo_path):
  unignored_files = []
  for dirpath, dirnames, filenames in os.walk(repo_path, topdown=True):
    # Add unignored files
    for fn in filenames:
      if not is_path_ignored(repo_path, os.path.join(dirpath, fn)):
        unignored_files.append(os.path.join(dirpath, fn))

    # Prune ignored directories. For some reason `git check-ignore` does not
    # ignore the .git directory so we simply avoid all directories named '.git'
    # here.
    dirnames[:] = [
      dn for dn in dirnames
      if (not is_path_ignored(repo_path, os.path.join(dirpath, dn)))
      and (dn != '.git')
    ]

  return unignored_files

def archive_git_repo(repo_path, outpath):
  """
  Arguments
  =========
  repo_path : Path
  outpath : Path
  """

  # Using `git archive` doesn't work for untracked files unfortunately...

  # We add both `git ls-files` and all of the unignored files together because
  # it is technically possible to have a both tracked and ignored file. The
  # `ls_files` paths are relative and the `all_unignored_files` paths are
  # absolute so we have to put them on the same footing here.
  file_paths = set(
    [repo_path / p for p in ls_files(repo_path)] +
    [Path(p) for p in all_unignored_files(repo_path)]
  )

  with zipfile.ZipFile(outpath, mode='w') as archive:
    for fp in file_paths:
      archive.write(fp, arcname=str(fp.relative_to(repo_path)))
