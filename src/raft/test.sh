#!/usr/bin/env sh

trap 'exit 1' INT TERM

echo_red() {
  printf "\033[31m%s\033[0m" "$*"
}

echo_green() {
  printf "\033[32m%s\033[0m\n" "$*"
}

if [ "$#" -eq 2 ]; then
  flags="-run $2"
elif [ "$#" -gt 2 ]; then
  echo "Usage: $0 [test-times] [test-regex]"
  exit 1
else
  flags=""
fi

test -d log || mkdir log
rm -rf log/*

errori=0

for t in $(seq 0 "$1"); do
  printf "test%s..." "$t"
  if go test -race $flags >"log/error-${errori}.log" 2>&1; then
    echo_green "OK"
  else
    errori=$((errori + 1))
    echo_red "FAIL"
  fi
done

exit $errori
