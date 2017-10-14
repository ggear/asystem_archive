#!/usr/bin/env bash

declare -a HOST_ID=("2627" "5029" "4078")
declare -a HOST_NAME=("per1.speedtest.telstra.net" "nyc.speedtest.sbcglobal.net" "if0-0.speedtest.lon.vorboss.net")

VERBOSE=false
LATENCY=false
THROUGHPUT=false
PING_FAIL=false
HOST_COUNT=${#HOST_ID[@]}
PING_FAIL_FILE=/tmp/speedtest.failed
POSTURL="http://127.0.0.1:8091/rest/?sources=speedtest&targets="

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -v|--verbose)
      VERBOSE=true
      ;;
      -l|--latency)
      LATENCY=true
      ;;
      -t|--throughput)
      THROUGHPUT=true
      ;;
      *)
      ;;
  esac
  shift
done

if ! ${LATENCY} && ! ${THROUGHPUT}; then
  echo "Usage: $0 [-v, --verbose] [-l, --latency] [-t, --throughput]"
fi

if ${LATENCY}; then
  for (( i=0; i<${HOST_COUNT}; i++ )); do
    PING=$(ping -c 1 -t 30 ${HOST_NAME[$i]} | sed -ne '/.*time=/{;s///;s/ .*//;p;}' | tr -d '\n')
    JSON="{\"ping-icmp\":"${PING}",\"server\":{\"id\": \""${HOST_ID[$i]}"\"}}"
    ${VERBOSE} && echo -n "Latency ["${HOST_NAME[$i]}"]: " && echo -n ${JSON} && echo ""
    curl -H "Content-Type: application/json" -X POST -d "${JSON}" "${POSTURL}${HOST_ID[$i]}"
    if [ ! -n "${PING}" ]; then
      PING_FAIL=true
    fi
  done
fi

if ${PING_FAIL}; then
  touch ${PING_FAIL_FILE}
else
  [ -f ${PING_FAIL_FILE} ] && THROUGHPUT=true
  rm -rf ${PING_FAIL_FILE}
fi

if ${THROUGHPUT}; then
  for (( i=0; i<${HOST_COUNT}; i++ )); do
    JSON=$(speedtest --json --bytes --timeout 30 --server ${HOST_ID[$i]} | tr '\n' ' ')
    ${VERBOSE} && echo -n "Throughput ["${HOST_NAME[$i]}"]: " && echo -n ${JSON} && echo ""
    curl -H "Content-Type: application/json" -X POST -d "${JSON}" "${POSTURL}${HOST_ID[$i]}"
  done
fi
