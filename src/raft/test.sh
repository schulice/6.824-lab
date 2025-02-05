#!/usr/bin/env sh

errori=0

if [ "$#" -ne 1 ]; then
  flags="-run $*"
fi

for t in $(seq 1 50); do
  echo "start test $t"
  go test -race "$flags" > "log/error-${errori}.log" 2>&1 || errori=$((errori + 1))
done

