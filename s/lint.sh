#!/bin/sh

command -v rg >/dev/null 2>&1 || { echo >&2 "ripgrep not found. Aborting."; exit 1; }

function find_newlines {
    if rg -Ul '[^\n]\z' .; then
        echo "Files must end in a newline character" >&2
        return 1
    else
        echo "All files end in a newline character"
        return 0
    fi
}

function find_trailing_whitespace {
    if rg -Ul '[ \t]+$' .; then
        echo "Files must not end in trailing whitespace" >&2
        return 1
    else
        echo "All files end in trailing whitespace"
        return 0
    fi
}

echo 'Running lint checks...'
echo 'Running newline check...'
find_newlines
echo 'Running trailing whitespace check...'
find_trailing_whitespace
