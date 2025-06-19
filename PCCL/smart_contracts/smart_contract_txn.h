#ifndef _SC_TXN_H_
#define _SC_TXN_H_
#include "global.h"
#include "txn.h"
#include "wl.h"

#if BANKING_SMART_CONTRACT

class SCWorkload : public Workload
{
public:
    RC init();
    RC get_txn_man(TxnManager *&txn_manager);
    int key_to_part(uint64_t key);
};

class SmartContractTxn : public TxnManager
{
public:
    void init(uint64_t thd_id, Workload *h_wl);
    void reset();
    RC run_txn();
#if ISEOV
#if PRE_EX
    RC simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet);
#else
    RC simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet);
#endif
#if RE_EXECUTE
    RC validate_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet);
#else
    RC validate_and_commit(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet);
#endif
#endif
private:
   SCWorkload *_wl;
};

#endif
#endif
