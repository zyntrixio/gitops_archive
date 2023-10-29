#!/bin/sh

command -v rg >/dev/null 2>&1 || { echo >&2 "ripgrep not found. Aborting."; exit 1; }

if rg -Ul '[^\n]\z'; then
    echo "Files must end in a newline character" >&2
    exit 1
else
    exit 0
fi
