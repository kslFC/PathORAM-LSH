#!/bin/sh
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT" || exit 1

# Match Roost ORAM's default bucket storage location.
mkdir -p /tmp/serverStorage

java -cp "$SCRIPT_DIR:$SCRIPT_DIR/lib/guava-33.0.0-jre.jar" com.client.Client "$@"
