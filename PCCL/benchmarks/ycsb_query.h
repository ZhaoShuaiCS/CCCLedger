#ifndef _YCSBQuery_H_
#define _YCSBQuery_H_

#include "global.h"
#include "query.h"
#include "array.h"

class Workload;
class Message;
class YCSBQueryMessage;
class YCSBClientQueryMessage;

// Each YCSBQuery contains several ycsb_requests,
// to a single table
class ycsb_request
{
public:
    ycsb_request() {}
    ycsb_request(const ycsb_request &req) : key(req.key), value(req.value), column(req.column), type(req.type) {}
    //ycsb_request(const ycsb_request &req) : key(req.key), value(req.value) {}
    void copy(ycsb_request *req)
    {
        this->key = req->key;
        this->value = req->value;
        this->column = req->column;
        this->type = req->type;
#if LARGER_TXN
        strncpy(this->payload, req->payload, 1556);
#endif
    }

    uint64_t key;
    uint64_t value;
    uint64_t column;
    YCSBType type;
#if LARGER_TXN
    char payload[1556];
    // 156, 356, 756, 1556
#endif
};

// test:add the write set of ycsb_request
#if PRE_ORDER
class ycsb_request_writeset
{
public:
    ycsb_request_writeset() {}
    ycsb_request_writeset(const ycsb_request &req) : key(req.key), value(req.value) {}
    void copy(ycsb_request_writeset *req)
    {
        this->key = req->key;
        this->value = req->value;
#if LARGER_TXN
        strncpy(this->payload, req->payload, 1556);
#endif
    }
    void copy(ycsb_request *req)
    {
        this->key = req->key;
        this->value = req->value;
#if LARGER_TXN
        strncpy(this->payload, req->payload, 1556);
#endif
    }
    uint64_t key;
    uint64_t value;
#if LARGER_TXN
    char payload[1556];
    // 156, 356, 756, 1556
#endif
};
#endif
// test_add

class YCSBQueryGenerator : public QueryGenerator
{
public:
    void init();
    BaseQuery *create_query();

private:
    BaseQuery *gen_requests_zipf();

    // for Zipfian distribution
    double zeta(uint64_t n, double theta);
    uint64_t zipf(uint64_t n, double theta);

    myrand *mrand;
    static uint64_t the_n;
    static double denom;
    double zeta_2_theta;
};

class YCSBQuery : public BaseQuery
{
public:
    YCSBQuery()
    {
    }
    ~YCSBQuery()
    {
    }

    void print();

    void init(uint64_t thd_id, Workload *h_wl){};
    void init();
    void release();
    void release_requests();
    void reset();
    static void copy_request_to_msg(YCSBQuery *ycsb_query, YCSBQueryMessage *msg, uint64_t id);

    Array<ycsb_request *> requests;
#if PRE_ORDER
    Array<ycsb_request_writeset *> requests_writeset;
#endif

#if ISEOV
    #if PRE_EX
    uint64_t simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet);
    #else
    uint64_t simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet);
    #endif
#if RE_EXECUTE
    uint64_t v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet);
#else
    uint64_t v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet);
#endif
#endif
};

#endif
