#!/usr/bin/env bash

NAME="$1@127.0.0.1"
shift
NODES=("${@/%/@127.0.0.1\'}")
NODES=$(IFS=","; echo "${NODES[*]/#/\'}")
erl -s nezha_counter_app -name "${NAME}" -nezha_counter nodes "[$NODES]"
