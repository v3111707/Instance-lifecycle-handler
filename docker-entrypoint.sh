#!/bin/sh
echo "[entrypoint] Starting: python -m ${PYTHON_MODULE_NAME}.main $*"
exec python -m "${PYTHON_MODULE_NAME}.main" "$@"
