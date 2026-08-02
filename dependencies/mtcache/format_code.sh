#!/bin/bash

set -e -o pipefail

usage() {
  echo "
Usage: $0 <options>
  Optional options:
    --scope [all, local, origin]
      Set the format scope. This option must be set.
    -h, --help
      Print this usage.

The scope argument speficies how the code will be formated. Its value can be:
    all                format all code
    local              format the code diff from local master
    origin             format the code diff from origin master

Eg.
    $0 --scope all                format all code
  "
}

OPTS=$(getopt \
  -n $0 \
  -o 'h' \
  -l 'scope:' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

FORMAT_SCOPE=

if [ $# == 1 ] ; then
  # no arguments
  usage
  exit 1
else
  while true; do
    case "$1" in
      --scope) FORMAT_SCOPE=$2 ; shift 2 ;;
      -h|--help) usage ; exit 0 ;;
      --) shift ;  break ;;
      *) usage; exit 1 ;;
    esac
  done
fi

case "$FORMAT_SCOPE" in
  "all"|"local"|"origin")
    ;;
  *)
    usage
    exit 1
esac

if [ "$FORMAT_SCOPE" == "all" ]; then
  git ls-files | egrep '.*\.(h|cc|cpp|inl)' | xargs -r clang-format -style=file -i
elif [ "$FORMAT_SCOPE" == "local" ]; then
  git diff master..HEAD --name-only --diff-filter=ACMRT | egrep '.*\.(h|cc|cpp|inl)' | xargs -r clang-format -style=file -i
elif [ "$FORMAT_SCOPE" == "origin" ]; then
  git diff origin/master --name-only --diff-filter=ACMRT | egrep '.*\.(h|cc|cpp|inl)' | xargs -r clang-format -style=file -i
else
  usage
  exit 1
fi
