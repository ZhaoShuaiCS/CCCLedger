#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"

#if !BANKING_SMART_CONTRACT
class YCSBQuery;

class YCSBQueryMessage;

class ycsb_request;

class YCSBWorkload : public Workload
{
public:
    RC init();
    RC get_txn_man(TxnManager *&txn_manager);
    int key_to_part(uint64_t key);

private:
    pthread_mutex_t insert_lock;
    //  For parallel initialization
    static int next_tid;
};

class YCSBTxnManager : public TxnManager
{
public:
    YCSBType type;
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
    YCSBWorkload *_wl;
};

#endif
#endif