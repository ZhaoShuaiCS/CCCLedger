#!/bin/bash
if [ $# -ne 1 ];
    then
    echo "请输入日志路径"
    exit
fi

file_name="./config.h"
#array=("1312" "2625" "3936" "5250" "6562" "7875" "9187" "10500")
#array=("2625" "5250" "7875" "10500" "13125" "15750" "18375" "21000" "23625")
array=("26250")
#array=("0.6" "0.8" "0.99")
#line=22
python3 ./scripts/StopSystem.py
mkdir ~/expr-result-0409/$1
rm ~/expr-result-0409/$1/* -r

for i in "${array[@]}"; do
#sed -i ''${line}'s/.*/#define BATCH_SIZE '$i'/g' ${file_name}
sed -i '/#define CLIENT_SEND_RATE/s/.*/#define CLIENT_SEND_RATE '$i'/g' ${file_name}  
#sed -n ''${line}','${line}'p' ${file_name} 
sed -n '/#define CLIENT_SEND_RATE/p' ${file_name}
make clean
make -j8
python3 ./scripts/scp_binaries.py
python3 ./scripts/RunSystem.py
sleep 90s
python3 ./scripts/StopSystem.py
sleep 10s
python3 ./scripts/scp_results.py
cp ./results ~/expr-result-0409/$1/$i -r

done