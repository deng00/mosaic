#!/bin/bash
# Wrapper used by launchd / make run / dev shell.
# Sources .env (legacy single-bot mode) only when config.yaml is absent.
set -e
cd "$(dirname "$0")"
if [ ! -f config.yaml ] && [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi
exec ./mosaic
