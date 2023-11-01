#!/bin/sh

command -v rg >/dev/null 2>&1 || { echo >&2 "ripgrep not found. Aborting."; exit 1; }

if rg -Ul --debug '[^\n]\z'; then
    echo "Files must end in a newline character" >&2
    exit 1
else
    echo "All files end in a newline character"
    exit 0
fi
