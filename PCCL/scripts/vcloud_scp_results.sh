#!/bin/bash
# RES_FILE --> Name of the result file.
#
USERNAME=wangjunkai
HOSTS="$1"
count=0
IDENTITY="~/aws.pem"

rm ./results/*

for HOSTNAME in ${HOSTS}; do
	if [ $count -le 128 -o $count -ge 128 -a $count -le 136 ]; then
		scp -i ${IDENTITY} -o StrictHostKeyChecking=no ${USERNAME}@${HOSTNAME}:resdb/results/${count}.out ./results/
	fi
	count=`expr $count + 1`
done