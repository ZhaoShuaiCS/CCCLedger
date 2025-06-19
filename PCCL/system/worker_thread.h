#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"
#include "message.h"
#include "crypto.h"

class Workload;
class Message;

class WorkerThread : public Thread
{
public:
    ~WorkerThread();
    RC run();
    void setup();
    void send_key();
    RC process_key_exchange(Message *msg);

    void process(Message *msg);
    TxnManager *get_transaction_manager(uint64_t net_id, uint64_t txn_id, uint64_t batch_id);
    RC init_phase();
    bool is_cc_new_timestamp();
    bool exception_msg_handling(Message *msg);

#if ABORT_BATCH
    bool is_re_execute;
#endif

    uint64_t next_set;
    uint64_t get_next_txn_id();

    void release_txn_man(uint64_t net_id, uint64_t txn_id, uint64_t batch_id);
    void algorithm_specific_update(Message *msg, uint64_t idx);

    //[Dakai]
    //uint64_t last_txn_processed;

    void create_and_send_batchreq(ClientQueryBatch *msg, uint64_t tid);
    void set_txn_man_fields(BatchRequests *breq, uint64_t bid, uint64_t nid);

    bool validate_msg(Message *msg);
    bool checkMsg(Message *msg);
    RC process_client_batch(Message *msg);
    RC process_batch(Message *msg);
    RC process_broadcast_batch(Message *msg);
    void send_checkpoints(uint64_t txn_id);
    RC process_pbft_chkpt_msg(Message *msg);

#if BANKING_SMART_CONTRACT
    void init_txn_man(BankingSmartContractMessage *bscm);
#else
    void init_txn_man(YCSBClientQueryMessage *msg);
#endif
#if EXECUTION_THREAD
    void send_execute_msg();
    void send_broadcast_batch_msg();
    RC process_execute_msg(Message *msg);
#endif

#if TIMER_ON
    void add_timer(Message *msg, string qryhash);
    void remove_timer(string qryhash);
#endif

#if VIEW_CHANGES
    void client_query_check(ClientQueryBatch *clbtch);
    void check_for_timeout();
    void store_batch_msg(BatchRequests *breq);
    RC process_view_change_msg(Message *msg);
    RC process_new_view_msg(Message *msg);
    void reset();
    void fail_primary(Message *msg, uint64_t time);
#endif

#if LOCAL_FAULT
    void fail_nonprimary();
#endif

    bool prepared(PBFTPrepMessage *msg);
    RC process_pbft_prep_msg(Message *msg);

    bool committed_local(PBFTCommitMessage *msg);
    RC process_pbft_commit_msg(Message *msg);
    void unset_ready_txn(TxnManager * tman);
#if SHARPER
    void create_and_send_batchreq_cross(ClientQueryBatch *msg, uint64_t tid);
    RC process_super_propose(Message *msg);
#elif RING_BFT
    RC process_commit_certificate(Message *msg);
    RC process_ringbft_preprepare(Message *msg);
    RC process_ringbft_commit(Message *msg);
    void create_and_send_pre_prepare(CommitCertificateMessage *msg, uint64_t tid);
#endif

#if TESTING_ON
    void testcases(Message *msg);
#if TEST_CASE == ONLY_PRIMARY_NO_EXECUTE
    void test_no_execution(Message *msg);
#elif TEST_CASE == ONLY_PRIMARY_EXECUTE
    void test_only_primary_execution(Message *msg);
#elif TEST_CASE == ONLY_PRIMARY_BATCH_EXECUTE
    void test_only_primary_batch_execution(Message *msg);
#endif
#endif

private:
    uint64_t _thd_txn_id;
    ts_t _curr_ts;
    TxnManager *txn_man;
};

#endif
