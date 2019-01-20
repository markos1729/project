#!/bin/bash
workload="small"
if [ "$#" -gt 0 ]; then
	workload="$1"
fi
echo "Running 10 instances of" $workload "..."
let sum=0
for((i=0 ; i<10 ; i++)); do
	let time=$(./harness/runTestharness.sh $workload | sed -n 2p)
	echo $time
	let sum=sum+time
done
let avg=sum/10
echo "Average is:" $avg "ms"
