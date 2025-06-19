#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "array.h"

class Workload;
class YCSBQuery;

class BaseQuery
{
public:
    virtual ~BaseQuery() {}
    virtual void print() = 0;
    virtual void init() = 0;
    uint64_t waiting_time;
    //void clear() = 0;
    virtual void release() = 0;
#if ISEOV
#if PRE_EX
    virtual uint64_t simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet) = 0;
#else
    virtual uint64_t simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet) = 0;
#endif
#if RE_EXECUTE
    virtual uint64_t v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet) = 0;
#else
    virtual uint64_t v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet) = 0;
#endif
#endif
};

class QueryGenerator
{
public:
    virtual ~QueryGenerator() {}
    virtual BaseQuery *create_query() = 0;
};

// All the queries for a particular thread.
class Query_thd
{
public:
    void init(Workload *h_wl, int thread_id);
    BaseQuery *get_next_query();
    int q_idx;
    YCSBQuery *queries;

    char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// class Query_queue
// {
// public:
//     void init(Workload *h_wl);
//     void init(int thread_id);
//     BaseQuery *get_next_query(uint64_t thd_id);

// private:
//     Query_thd **all_queries;
//     Workload *_wl;
// };

#endif
