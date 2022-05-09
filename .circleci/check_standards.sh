#!/bin/bash
set -ue

if (git diff --name-only HEAD main | grep .py); then
  echo "Some files were updated and they will be reviewed"
  git diff --name-only HEAD main | grep .py | xargs black --check
else
  echo "No python files were updated"
fi
