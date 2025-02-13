#!/usr/bin/env sh

trap 'echo "Cancel"; exit 1' INT TERM

if [ "$#" -eq 1 ]; then
  flags="-run $1"
elif [ "$#" -gt 1 ]; then
  echo "Usage: $0 [test-regex]"
  exit 1
else
  flags=""
fi

test -d log || mkdir log
rm -rf log/*

errori=0

for t in $(seq 1 50); do
  echo "start test $t"
  go test -race $flags > "log/error-${errori}.log" 2>&1 || errori=$((errori + 1))
done

exit $errori

