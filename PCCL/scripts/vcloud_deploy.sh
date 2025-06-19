#!/bin/bash
# RES_FILE --> Name of the result file.
#
USERNAME=wangjunkai
HOSTS="$1"
NODE_CNT="$2"
RES_FILE="$3"
count=0
IDENTITY="~/aws.pem"
# for HOSTNAME in ${HOSTS}; do
# 	    SCRIPT="rm ./results/*"
# 	    echo "${HOSTNAME}: rm ./results/* ${count}"
# 	# if [ "$i" -eq 0 ];then
# 		ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
# 		cd resdb
# 		${SCRIPT}" &
# 	# fi
# 	count=`expr $count + 1`
# done

# count=0
for HOSTNAME in ${HOSTS}; do
	if [ $count -ge $NODE_CNT ]; then
		# if [ $count -eq 8 ]; then
		# 	count=`expr $count + 1`
		# 	continue;
		# fi
	    SCRIPT="./runcl -nid${count} > ${RES_FILE}${count}.out 2>&1"
	    echo "${HOSTNAME}: runcl ${count}"
	else
		# if [ $count -eq 0 ]; then
		# 	count=`expr $count + 1`
		# 	continue;
		# fi
	    SCRIPT="./rundb -nid${count} > ${RES_FILE}${count}.out 2>&1"
	    echo "${HOSTNAME}: rundb ${count}"
	fi
	# if [ "$i" -eq 0 ];then
		ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
		cd resdb
		${SCRIPT}" &
	# fi
	count=`expr $count + 1`
done

# while [ $count -gt 0 ]; do
# 	wait $pids
# 	count=`expr $count - 1`
# done
