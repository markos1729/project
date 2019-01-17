#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "${1}" = "small" ]; then
	WORKLOAD_DIR="$DIR/../workloads/small"
elif [ "${1}" = "public" ]; then
	WORKLOAD_DIR="$DIR/../workloads/public"
else
	WORKLOAD_DIR=${1-$DIR/../workloads/small}
fi
WORKLOAD_DIR=$(echo $WORKLOAD_DIR | sed 's:/*$::')

cd $WORKLOAD_DIR

WORKLOAD=$(basename "$PWD")
echo execute $WORKLOAD ...
$DIR/../harness/harness *.init *.work *.result ../../submission/run.sh
