#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
//#include "wl.h"
#include "ycsb.h"
#include "message.h"
#include "global.h"

uint64_t YCSBQueryGenerator::the_n = 0;
double YCSBQueryGenerator::denom = 0;

void YCSBQueryGenerator::init()
{
    mrand = (myrand *)mem_allocator.alloc(sizeof(myrand));
    mrand->init(get_sys_clock());

    zeta_2_theta = zeta(2, g_zipf_theta);
    uint64_t table_size = g_synth_table_size / g_part_cnt;
    the_n = table_size - 1;
    denom = zeta(the_n, g_zipf_theta);
}

BaseQuery *YCSBQueryGenerator::create_query()
{
    BaseQuery *query;
    assert(the_n != 0);
    query = gen_requests_zipf();

    return query;
}

void YCSBQuery::print() {}

void YCSBQuery::init()
{
    requests.init(g_req_per_query);
}

void YCSBQuery::copy_request_to_msg(YCSBQuery *ycsb_query, YCSBQueryMessage *msg, uint64_t id)
{
    msg->requests.add(ycsb_query->requests[id]);
}

void YCSBQuery::release_requests()
{
    for (uint64_t i = 0; i < requests.size(); i++)
    {
        DEBUG_M("YCSBQuery::release() ycsb_request free\n");
        mem_allocator.free(requests[i], sizeof(ycsb_request));
    }
}

void YCSBQuery::reset()
{
    release_requests();
    requests.clear();
}

void YCSBQuery::release()
{
    DEBUG_M("YCSBQuery::release() free\n");
    release_requests();
    requests.release();
}

#if ISEOV
#if PRE_EX
uint64_t YCSBQuery::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    int result = 1;
    switch (this->requests[0]->type)
    {
    case YCSB_READ:
    {
        uint64_t elem_key = this->requests[0]->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp;
            if(speculateSet.find(elem_key * g_ycsb_column + j) == speculateSet.end())
            {
                temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
                //DEBUG_V1("test_ycsb:YCSB_READ_simulate_getfromdb:readSet[%ld] = %d\n", elem_key * g_ycsb_column + j, stoi(temp));
            }
            else
            {
                temp = std::to_string(speculateSet[elem_key * g_ycsb_column + j]);
                //DEBUG_V1("test_ycsb:YCSB_READ_simulate_getfromspeculateSet:readSet[%ld] = %d\n", elem_key * g_ycsb_column + j, stoi(temp));
            }
            
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            readSet[elem_key * g_ycsb_column + j] = attr_value;
            //DEBUG_V1("test_ycsb:YCSB_READ_simulate:readSet[%ld] = %ld\n", elem_key * g_ycsb_column + j, attr_value);
        }
        break;
    }
    case YCSB_UPDATE:
    {
        uint64_t elem_key = this->requests[0]->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp;
            if(speculateSet.find(elem_key * g_ycsb_column + j) == speculateSet.end())
            {
                temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            }
            else
            {
                temp = std::to_string(speculateSet[elem_key * g_ycsb_column + j]);
            }
            
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            readSet[elem_key * g_ycsb_column + j] = attr_value;
            //DEBUG_V1("test_ycsb:YCSB_READ_simulate:readSet[%ld] = %ld\n", elem_key * g_ycsb_column + j, attr_value);
        }
        
        writeSet[elem_key * g_ycsb_column + this->requests[0]->column] = this->requests[0]->value;
        speculateSet[elem_key * g_ycsb_column + this->requests[0]->column] = this->requests[0]->value;
        //DEBUG_V1("test_ycsb:YCSB_UPDATE_simulate:writeSet[%ld] = %ld\n", elem_key * g_ycsb_column + this->requests[0]->column, this->requests[0]->value);
        break;
    }
    default:
        //DEBUG_V1("test_ycsb:YCSB_UPDATE_simulate:err type = %d\n", this->requests[0]->type);
        assert(0);
        break;
    }

    return result;
}
#else
uint64_t YCSBQuery::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    int result = 1;
    switch (this->requests[0]->type)
    {
    case YCSB_READ:
    {
        uint64_t elem_key = this->requests[0]->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            readSet[elem_key * g_ycsb_column + j] = attr_value;
            //DEBUG_V1("test_ycsb:YCSB_READ_simulate:readSet[%ld] = %ld\n", elem_key * g_ycsb_column + j, attr_value);
        }
        break;
    }
    case YCSB_UPDATE:
    {
        uint64_t elem_key = this->requests[0]->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            readSet[elem_key * g_ycsb_column + j] = attr_value;
            //DEBUG_V1("test_ycsb:YCSB_UPDATE_simulate:readSet[%ld] = %ld\n", elem_key * g_ycsb_column + j, attr_value);
        }

        
        writeSet[elem_key * g_ycsb_column + this->requests[0]->column] = this->requests[0]->value;
        //DEBUG_V1("test_ycsb:YCSB_UPDATE_simulate:writeSet[%ld] = %ld\n", elem_key * g_ycsb_column + this->requests[0]->column, this->requests[0]->value);
        break;
    }
    default:
        //DEBUG_V1("test_ycsb:YCSB_UPDATE_simulate:err type = %d\n", this->requests[0]->type);
        assert(0);
        break;
    }

    return result;
}
#endif
#if !RE_EXECUTE
uint64_t YCSBQuery::v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    int result = 0;
    ycsb_request *req =this->requests[0];
    switch (req->type)
    {
    case YCSB_READ:
    {
        uint64_t elem_key = req->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            if((readSet[elem_key * g_ycsb_column + j]) != attr_value){
                //DEBUG_V1("test_ycsb:YCSB_READ:conflict::key = %ld, old value = %ld, now value = %ld\n", elem_key * g_ycsb_column + j, readSet[elem_key * g_ycsb_column + j], attr_value);
                return 0;
            }
        }
        //DEBUG_V1("test_ycsb:YCSB_READ:ok\n");

        result = 1;
        break;
    }
    case YCSB_UPDATE:
    {
        uint64_t elem_key = req->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            if((readSet[elem_key * g_ycsb_column + j]) != attr_value){
                //DEBUG_V1("test_ycsb:YCSB_UPDATE:conflict::key = %ld, old value = %ld, now value = %ld\n", elem_key * g_ycsb_column + j, readSet[elem_key * g_ycsb_column + j], attr_value);
                return 0;
            }
        }
        //DEBUG_V1("test_ycsb:YCSB_UPDATE:ok\n");

        db->Put(std::to_string(req->key * g_ycsb_column + req->column), std::to_string(req->value));
        result = 1;
        break;
    }
    default:
        assert(0);
        break;
    }

    return result;
}
#else
uint64_t YCSBQuery::v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
   // DEBUG_V1("test_v6:enter SmartContract::v_and_merge\n");
    int result = 0;
    ycsb_request *req =this->requests[0];
    switch (req->type)
    {
    case YCSB_READ:
    {
        uint64_t elem_key = req->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp;
            if (mergeSet.find(elem_key * g_ycsb_column + j) == mergeSet.end())
            {
                temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            }
            else {
                temp = std::to_string(mergeSet[elem_key * g_ycsb_column + j]);
            }
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            if((readSet[elem_key * g_ycsb_column + j]) != attr_value){
                //DEBUG("test_v5:TransferMoneySmartContract::get_old_source = %ld, now source = %ld\n", readSet[this->source_id], source);
                return 0;
            }
        }

        result = 1;
        break;
    }
    case YCSB_UPDATE:
    {
        uint64_t elem_key = req->key;
        for(uint64_t j = 0; j < g_ycsb_column; j++){
            string temp;
            if (mergeSet.find(elem_key * g_ycsb_column + j) == mergeSet.end())
            {
                temp = db->Get(std::to_string(elem_key * g_ycsb_column + j));
            }
            else {
                temp = std::to_string(mergeSet[elem_key * g_ycsb_column + j]);
            }
            uint64_t attr_value = temp.empty() ? 0 : stoi(temp);
            //DEBUG_V1("test_ycsb:YCSB_UPDATE:read::key = %ld, db_value = %ld, readSet_value = %ld\n", elem_key * g_ycsb_column + j, readSet[elem_key * g_ycsb_column + j], attr_value);
            if((readSet[elem_key * g_ycsb_column + j]) != attr_value){
                //DEBUG_V1("test_ycsb:YCSB_UPDATE:conflict::key = %ld, old value = %ld, now value = %ld\n", elem_key * g_ycsb_column + j, readSet[elem_key * g_ycsb_column + j], attr_value);
                return 0;
            }
        }

        //DEBUG_V1("test_ycsb:YCSB_UPDATE:write::key = %ld, writeSet_value = %ld\n", req->key * g_ycsb_column + req->column, req->value);
        mergeSet[req->key * g_ycsb_column + req->column] = req->value;

        //db->Put(std::to_string(req->key * g_ycsb_column + req->column), std::to_string(req->value));
        result = 1;
        break;
    }
    default:
        assert(0);
        break;
    }

    return result;
}
#endif
#endif


// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be
// zeta(2.0, theta).
double YCSBQueryGenerator::zeta(uint64_t n, double theta)
{
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}

uint64_t YCSBQueryGenerator::zipf(uint64_t n, double theta)
{
    assert(this->the_n == n);
    assert(theta == g_zipf_theta);
    double alpha = 1 / (1 - theta);
    double zetan = denom;
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta_2_theta / zetan);
    //	double eta = (1 - pow(2.0 / n, 1 - theta)) /
    //		(1 - zeta_2_theta / zetan);
    double u = (double)(mrand->next() % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1)
        return 1;
    if (uz < 1 + pow(0.5, theta))
        return 2;
    return 1 + (uint64_t)(n * pow(eta * u - eta + 1, alpha));
}

BaseQuery *YCSBQueryGenerator::gen_requests_zipf()
{
    YCSBQuery *query = (YCSBQuery *)mem_allocator.alloc(sizeof(YCSBQuery));
    new (query) YCSBQuery();
    query->requests.init(g_req_per_query);

    uint64_t table_size = g_synth_table_size;

    int rid = 0;
    for (UInt32 i = 0; i < g_req_per_query; i++)
    {
        //double r = (double)(mrand->next() % 10000) / 10000;
        ycsb_request *req = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request));

    #if GEN_ZIPF
        uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
        //DEBUG_V1("test_ycsb:row_id = %ld\n", row_id);
    #else 
        #if GEN_HOT
        uint64_t is_hot = false;
        if(mrand->next() % 100 < g_read_hot){
            is_hot = true;
        }
        uint64_t row_id;
        if(is_hot){
            row_id = mrand->next() % (uint64_t)(table_size * g_hotness);
        }
        else{
            row_id = table_size * g_hotness + mrand->next() % (uint64_t)(table_size * (1 - g_hotness));
        }
        #else
        uint64_t row_id = mrand->next() % table_size;
        #endif
    #endif
        ;
        //DEBUG_V1("row_id = %ld\n", row_id);
        assert(row_id < table_size);

        req->key = row_id;
        req->value = mrand->next() % 10000;
        req->column = (uint64_t)rand() % g_ycsb_column;
        //DEBUG_V1("test_ycsb:req->key = %ld, req->value = %ld \n, req->column = %ld", row_id, req->value, req->column);

        if(((uint64_t)rand() % 100) > g_ycsb_write_ratio)
        {
            req->type = YCSB_READ;
            //DEBUG_V1("test_ycsb:YCSB_READ:req->key = %ld, req->value = %ld , req->column = %ld\n", row_id, req->value, req->column);
        }
        else
        {
            req->type = YCSB_UPDATE;
            //DEBUG_V1("test_ycsb:YCSB_UPDATE:req->key = %ld, req->value = %ld , req->column = %ld\n", row_id, req->value, req->column);
        }

        rid++;

        query->requests.add(req);
    }
    assert(query->requests.size() == g_req_per_query);

    // Sort the requests in key order.
    if (g_key_order)
    {
        for (uint64_t i = 0; i < query->requests.size(); i++)
        {
            for (uint64_t j = query->requests.size() - 1; j > i; j--)
            {
                if (query->requests[j]->key < query->requests[j - 1]->key)
                {
                    query->requests.swap(j, j - 1);
                }
            }
        }
    }

    query->print();
    return query;
}
