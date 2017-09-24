#!/usr/bin/env bash

VERBOSE=false
LATENCY=false
THROUGHPUT=false
POSTURL="http://127.0.0.1:8091/rest/?sources=speedtest&targets="

declare -a HOST_ID=("2627" "5029" "4078")
declare -a HOST_NAME=("per1.speedtest.telstra.net" "nyc.speedtest.sbcglobal.net" "if0-0.speedtest.lon.vorboss.net")
HOST_COUNT=${#HOST_ID[@]}

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
  for (( i=1; i<${HOST_COUNT}+1; i++ )); do
    JSON="{\"ping-icmp\":"$(ping -c 1 -t 30 ${HOST_NAME[$i-1]} | sed -ne '/.*time=/{;s///;s/ .*//;p;}' | \
      tr '\n' ',')"\"server\":{\"id\": \""${HOST_ID[$i-1]}"\"}}"
    ${VERBOSE} && echo -n "Latency ["${HOST_NAME[$i-1]}"]: " && echo -n ${JSON} && echo ""
    curl -H "Content-Type: application/json" -X POST -d "${JSON}" "${POSTURL}${HOST_ID[$i-1]}"
  done
fi

if ${THROUGHPUT}; then
  for (( i=1; i<${HOST_COUNT}+1; i++ )); do
    JSON=$(speedtest --json --bytes --timeout 30 --server ${HOST_ID[$i-1]} | tr '\n' ' ')
    ${VERBOSE} && echo -n "Throughput ["${HOST_NAME[$i-1]}"]: " && echo -n ${JSON} && echo ""
    curl -H "Content-Type: application/json" -X POST -d "${JSON}" "${POSTURL}${HOST_ID[$i-1]}"
  done
fi
