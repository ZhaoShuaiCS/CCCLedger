# State-based Concurrency Control for Consensus in the Permissioned Blockchains
## Deployment and Testing
**Deployment** 
- We recommend deploying PCCL on Ubuntu 20.0.
  
**Testing**
- We have provided test scripts in folder "./PCCL/scripts/" for different parameters, including batchsize, send rate, Zipf, and Hotspot. Testing can be initiated by executing the command "./PCCL/scripts/batch_run_batchsize.sh ResultBatchsize", and the corresponding test results will be automatically saved to "./PCCL/results/ResultBatchsize/".
- System parameters can be configured by modifying the macro variables in file "./PCCL/config.h", such as batchsize.