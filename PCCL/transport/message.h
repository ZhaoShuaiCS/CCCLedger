#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "global.h"
#include "array.h"
#include <mutex>
#include <map>

class ycsb_request;
class ycsb_request_writeset;
class LogRecord;
struct Item_no;

class Message
{
public:
    virtual ~Message() {
        this->dest.clear();
    }
    static Message *create_message(char *buf);
    static Message *create_message(BaseQuery *query, RemReqType rtype);
    static Message *create_message(TxnManager *txn, RemReqType rtype);
    static Message *create_message(uint64_t txn_id, RemReqType rtype);
    static Message *create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype);
    static Message *create_message(LogRecord *record, RemReqType rtype);
    static Message *create_message(RemReqType rtype);
    static std::vector<Message *> *create_messages(char *buf);
    static void release_message(Message *msg);
    RemReqType rtype;
    uint64_t txn_id;
    uint64_t batch_id;
    uint64_t return_node_id;

    uint64_t wq_time;
    uint64_t mq_time;
    uint64_t ntwk_time;

    //signature is 768 chars, pubkey is 840
    uint64_t sigSize = 1;
    uint64_t keySize = 1;
    string signature = "0";
    string pubKey = "0";

    static uint64_t string_to_buf(char *buf, uint64_t ptr, string str);
    static uint64_t buf_to_string(char *buf, uint64_t ptr, string &str, uint64_t strSize);

    vector<uint64_t> dest;

    // Collect other stats
    double lat_work_queue_time;
    double lat_msg_queue_time;
    double lat_cc_block_time;
    double lat_cc_time;
    double lat_process_time;
    double lat_network_time;
    double lat_other_time;

    uint64_t mget_size();
    uint64_t get_txn_id() { return txn_id; }
    uint64_t get_batch_id() { return batch_id; }
    uint64_t get_return_id() { return return_node_id; }
    void mcopy_from_buf(char *buf);
    void mcopy_to_buf(char *buf);
    void mcopy_from_txn(TxnManager *txn);
    void mcopy_to_txn(TxnManager *txn);
    RemReqType get_rtype() { return rtype; }

    virtual uint64_t get_size() = 0;
    virtual void copy_from_buf(char *buf) = 0;
    virtual void copy_to_buf(char *buf) = 0;
    virtual void copy_to_txn(TxnManager *txn) = 0;
    virtual void copy_from_txn(TxnManager *txn) = 0;
    virtual void init() = 0;
    virtual void release() = 0;
#if SHARPER
    bool is_cross_shard = false;
#endif
#if MULTI_INSTANCES > 1
    #if STRONG_SERIAL
        bool force = true;
    #else
        bool force = false;
    #endif
#else
    bool force = true;
#endif
    

};

// Message types
class InitDoneMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
};

class KeyExchange : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    string pkey;
    uint64_t pkeySz;
    uint64_t return_node;
};

class ReadyServer : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
};

class QueryResponseMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    RC rc;
    uint64_t pid;
};

class DoneMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    uint64_t batch_id;
};

class ClientQueryMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();

    uint64_t pid;
    uint64_t ts;
    uint64_t client_startts;
    uint64_t first_startts;
    Array<uint64_t> partitions;
};

#if BANKING_SMART_CONTRACT
class BankingSmartContractMessage : public ClientQueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    BankingSmartContractMessage();
    ~BankingSmartContractMessage();

    BSCType type; // Type of Banking Smartcontract
    string getString();
    string getRequestString();

    Array<uint64_t> inputs;
// #if ISEOV
//     map<uint64_t,uint64_t> readSet;
//     map<uint64_t,uint64_t> writeSet;
// #endif
};
#else
class YCSBClientQueryMessage : public ClientQueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_query(BaseQuery *query);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    YCSBClientQueryMessage();
    ~YCSBClientQueryMessage();

    uint64_t return_node; // node that send this message.
    string getString();
    string getRequestString();

    Array<ycsb_request *> requests;
#if PRE_ORDER
    Array<ycsb_request_writeset *> requests_writeset;
#endif
};
#endif

class ClientResponseMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    void set_net_id(uint64_t id);
    string getString(uint64_t sender);

    RC rc;

#if CLIENT_RESPONSE_BATCH == true
    Array<uint64_t> index;
    Array<uint64_t> client_ts;
    uint64_t net_id;
#else
    uint64_t client_startts;
#endif
#if RING_BFT || SHARPER
    bool is_cross_shard = false;
#endif
    uint64_t view; // primary node id
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
};

#if CLIENT_BATCH
class ClientQueryBatch : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
#if PRE_ORDER
    void generate_input();
    void generate_output();
#endif

    uint64_t get_size();
    void init();
    void release();

    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string getString();
#if SHARPER
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
#elif RING_BFT
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    bool is_cross_shard = false;
#endif
    uint64_t return_node;
    uint64_t batch_size;
#if BANKING_SMART_CONTRACT
    Array<BankingSmartContractMessage *> cqrySet;
#else
    Array<YCSBClientQueryMessage *> cqrySet;
#if PRE_ORDER
    uint64_t inputState_size;
    uint64_t outputState_size;
    map<uint64_t,uint64_t> inputState;
    map<uint64_t,uint64_t> outputState;
#endif
    
#endif
};
#endif

class QueryMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    uint64_t pid;
};

class YCSBQueryMessage : public QueryMessage
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();

    Array<ycsb_request *> requests;
};

/***********************************/

class BatchRequests : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
#if BANKING_SMART_CONTRACT
    void copy_from_txn(TxnManager *txn, BankingSmartContractMessage *clqry);
#else
    void copy_from_txn(TxnManager *txn, YCSBClientQueryMessage *clqry);
#endif
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();
#if PRE_ORDER
    void add_state(ClientQueryBatch *bmsg);
    bool validate_pre_exec();
#endif
// #if ISEOV
//     void simulate_batch();
// #endif
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string getString(uint64_t sender);

    void add_request_msg(int idx, Message *msg);

    uint64_t view; // primary node id
#if SHARPER
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
#elif RING_BFT
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    bool is_cross_shard = false;
#endif

    Array<uint64_t> index;
#if BANKING_SMART_CONTRACT
    vector<BankingSmartContractMessage *> requestMsg;
#else
    vector<YCSBClientQueryMessage *> requestMsg;
#endif

#if ISEOV
    vector<map<uint64_t,uint64_t>> readSet;
    vector<map<uint64_t,uint64_t>> writeSet;
#endif
#if PRE_ORDER
    uint64_t inputState_size;
    uint64_t outputState_size;
    map<uint64_t,uint64_t> inputState;
    map<uint64_t,uint64_t> outputState;
#endif
// #if ISEOV
//     uint64_t readSet_size;
//     uint64_t writeSet_size;
//     map<uint64_t,uint64_t> readSet;
//     map<uint64_t,uint64_t> writeSet;
// #endif
    uint64_t hashSize; // Representative hash for the batch.
    string hash;
    uint32_t batch_size;
};

class ExecuteMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint64_t batch_size;
    uint64_t net_id;
};

class CheckpointMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string toString();
    bool addAndValidate();

    uint64_t index;       // sequence number of last request
    uint64_t return_node; //id of node that sent this message
    uint64_t end_index;
};

// Message Creation methods.
char *create_msg_buffer(Message *msg);
Message *deep_copy_msg(char *buf, Message *msg);
void delete_msg_buffer(char *buf);

class PBFTPrepMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    string toString();

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint32_t batch_size;
};

class PBFTCommitMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release() {}
    string toString();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();

    uint64_t view;        // primary node id
    uint64_t index;       // position in sequence of requests
    string hash;          //request message digest
    uint64_t hashSize;    //size of hash (for removing from buf)
    uint64_t return_node; //id of node that sent this message

    uint64_t end_index;
    uint64_t batch_size;
#if RING_BFT
    bool is_cross_shard = false;
#endif
};

class BroadcastBatchMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init();
    void release();
    //string getString(uint64_t sender);
    string getString();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate();
    void add_batch(Message *msg);
    void add_commit_msg(PBFTCommitMessage *cmsg);

    uint64_t net_id;
    uint64_t breq_return_id;
    uint64_t cmsgSet_size;
    BatchRequests *breq;
    vector<PBFTCommitMessage *> cmsgSet;

    // uint64_t view;        // primary node id
    // uint64_t index;       // position in sequence of requests
    // string hash;          //request message digest
    // uint64_t hashSize;    //size of hash (for removing from buf)
    // uint64_t return_node; //id of node that sent this message

    // uint64_t end_index;
    // uint64_t batch_size;
};

#if SHARPER

class SuperPropose : public BatchRequests
{
public:
};
#endif

#if RING_BFT
class CommitCertificateMessage : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release();

    void sign(uint64_t dest_node_id);
    bool validate();
    string toString();

    uint64_t commit_view;  // primary node id
    uint64_t commit_index; // position in sequence of requests
    uint64_t commit_hash_size;
    string commit_hash; //request message digests
    uint64_t forwarding_from = (uint64_t)-1;

    uint64_t hashSize;
    Array<uint64_t> signSize;
    Array<uint64_t> signOwner;
    string hash;
    vector<YCSBClientQueryMessage *> requestMsg;
    vector<string> signatures;
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
};

class RingBFTPrePrepare : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn){};
    void copy_to_txn(TxnManager *txn){};
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();

    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string getString(uint64_t sender);

    uint64_t view; // primary node id
    bool involved_shards[NODE_CNT / SHARD_SIZE]{false};
    uint64_t hashSize; // Representative hash for the batch.
    string hash;
    uint32_t batch_size;
};

class RingBFTCommit : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn);
    void copy_to_txn(TxnManager *txn);
    uint64_t get_size();
    void init() {}
    void release();

    void sign(uint64_t dest_node_id);
    bool validate();
    string toString();

    uint64_t commit_view;  // primary node id
    uint64_t commit_index; // position in sequence of requests
    uint64_t commit_hash_size;
    string commit_hash; //request message digests
    uint64_t forwarding_from = (uint64_t)-1;

    uint64_t hashSize;
    Array<uint64_t> signSize;
    Array<uint64_t> signOwner;
    string hash;
    vector<string> signatures;
};

#endif

/****************************************/
/*	VIEW CHANGE SPECIFIC		*/
/****************************************/

#if VIEW_CHANGES

class ViewChangeMsg : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn) {}
    void copy_to_txn(TxnManager *txn) {}
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id, TxnManager *txn);
    void release();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string toString();
    bool addAndValidate(uint64_t thd_id);

    uint64_t return_node; //id of node that sent this message
    uint64_t view;        // proposed view (v + 1)
    uint64_t index;       //index of last stable checkpoint
    uint64_t numPreMsgs;
    uint64_t numPrepMsgs;

    vector<BatchRequests *> preMsg;

    // Prepare messages <view, index, hash>.
    vector<uint64_t> prepView;
    vector<uint64_t> prepIdx;
    vector<string> prepHash;
    vector<uint64_t> prepHsize;
};

class NewViewMsg : public Message
{
public:
    void copy_from_buf(char *buf);
    void copy_to_buf(char *buf);
    void copy_from_txn(TxnManager *txn) {}
    void copy_to_txn(TxnManager *txn) {}
    uint64_t get_size();
    void init() {}
    void init(uint64_t thd_id);
    void release();
    void sign(uint64_t dest_node = UINT64_MAX);
    bool validate(uint64_t thd_id);
    string toString();

    uint64_t view; // proposed view (v + 1)
    uint64_t numViewChangeMsgs;
    uint64_t numPreMsgs;

    vector<ViewChangeMsg *> viewMsg;
    vector<BatchRequests *> preMsg;
};
/*******************************/

// Entities for handling BatchRequests message during a view change.
extern vector<BatchRequests *> breqStore;
extern std::mutex bstoreMTX;
void storeBatch(BatchRequests *breq);
void removeBatch(uint64_t range);

// Entities for handling ViewChange message.
extern vector<ViewChangeMsg *> view_change_msgs;
void storeVCMsg(ViewChangeMsg *vmsg);
void clearAllVCMsg();

#endif // VIEW_CHANGE

#endif
