#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BIN_DIR="$SCRIPT_DIR/../bin"

for package in server pathoram client example; do
	SOURCE_DIR="$SCRIPT_DIR/com/$package"
	DEST_DIR="$BIN_DIR/com/$package"

	if [ ! -d "$SOURCE_DIR" ]; then
		echo "copy.sh: source directory not found: $SOURCE_DIR" >&2
		exit 1
	fi

	set -- "$SOURCE_DIR"/*.class
	if [ ! -f "$1" ]; then
		echo "copy.sh: no compiled .class files in $SOURCE_DIR (run compile.sh first)" >&2
		exit 1
	fi

	mkdir -p "$DEST_DIR"
	cp "$SOURCE_DIR"/*.class "$DEST_DIR/"
done
