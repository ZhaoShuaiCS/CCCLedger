#!/bin/bash
if [ $# -ne 1 ];
    then
    echo "请输入日志路径"
    exit
fi

file_name="./config.h"
array=("0.0" "0.2" "0.4" "0.6" "0.8" "0.99")
python3 ./scripts/StopSystem.py
mkdir ~/expr-result-0409/$1
rm ~/expr-result-0409/$1/* -r

for i in "${array[@]}"; do
#sed -i ''${line}'s/.*/#define BATCH_SIZE '$i'/g' ${file_name}
sed -i '/#define ZIPF_THETA/s/.*/#define ZIPF_THETA '$i'/g' ${file_name}  
#sed -n ''${line}','${line}'p' ${file_name} 
sed -n '/#define ZIPF_THETA/p' ${file_name}
make clean
make -j8
python3 ./scripts/scp_binaries.py
python3 ./scripts/RunSystem.py
sleep 50s
python3 ./scripts/StopSystem.py
sleep 5s
python3 ./scripts/scp_results.py
cp ./results ~/expr-result-0409/$1/$i -r

done