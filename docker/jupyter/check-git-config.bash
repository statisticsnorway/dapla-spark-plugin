#!/usr/bin/env bash

GIT_CONFIG=~/.gitconfig

if [ ! -f $GIT_CONFIG ]; then
  cat << EOF
Your Git account is not configured.

To configure:

  run: git-config.sh

EOF
fi
