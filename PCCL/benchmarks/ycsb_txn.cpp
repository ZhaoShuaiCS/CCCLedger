#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#if !BANKING_SMART_CONTRACT
void YCSBTxnManager::init(uint64_t thd_id, Workload *h_wl)
{
    TxnManager::init(thd_id, h_wl);
    _wl = (YCSBWorkload *)h_wl;
    reset();
}

void YCSBTxnManager::reset()
{
    TxnManager::reset();
}

RC YCSBTxnManager::run_txn()
{
    uint64_t starttime = get_sys_clock();

    YCSBQuery *ycsb_query = (YCSBQuery *)query;
    ycsb_request *yreq;
    for (uint i = 0; i < ycsb_query->requests.size(); i++)
    {
        yreq = ycsb_query->requests[i];
        db->Put(std::to_string(yreq->key), std::to_string(yreq->value));
    }

    uint64_t curr_time = get_sys_clock();
    txn_stats.process_time += curr_time - starttime;
    txn_stats.process_time_short += curr_time - starttime;
    txn_stats.wait_starttime = get_sys_clock();

    return RCOK;
}

#if ISEOV
#if PRE_EX
RC YCSBTxnManager::simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    if(((YCSBQuery *)this->query)->simulate(readSet, writeSet, speculateSet) == 1){
        return RCOK;
    }
    // switch (this->type) 
    // {
    // case YCSB_READ:
    // {
        
    // }
    // case YCSB_UPDATE:
    // {
    //     txn_man->type = YCSB_UPDATE;
    // }

    // }
    return NONE;
}
#else
RC YCSBTxnManager::simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    if(((YCSBQuery *)this->query)->simulate(readSet, writeSet) == 1){
        return RCOK;
    }
    // switch (this->type) 
    // {
    // case YCSB_READ:
    // {
        
    // }
    // case YCSB_UPDATE:
    // {
    //     txn_man->type = YCSB_UPDATE;
    // }

    // }
    return NONE;
}
#endif

#if RE_EXECUTE
RC YCSBTxnManager::validate_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
#if CHECK_CONFILICT
    if(((YCSBQuery *)this->query)->v_and_merge(readSet, writeSet, mergeSet) == 1){
        return RCOK;
    }
    else return NONE;
#else
    (YCSBQuery *)this->query->v_and_merge(readSet, writeSet, mergeSet);
    return RCOK;
#endif
}


#else
RC YCSBTxnManager::validate_and_commit(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    if(((YCSBQuery *)this->query)->v_and_c(readSet, writeSet) == 1){
        return RCOK;
    }
    
    return NONE;
}
#endif
#endif




#endif