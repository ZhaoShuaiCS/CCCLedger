#include "global.h"
#include "message.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "timer.h"
#include "chain.h"

#if RING_BFT
/**
 * Processes an incoming client batch and sends a Pre-prepare message to al replicas.
 *
 * This function assumes that a client sends a batch of transactions and 
 * for each transaction in the batch, a separate transaction manager is created. 
 * Next, this batch is forwarded to all the replicas as a BatchRequests Message, 
 * which corresponds to the Pre-Prepare stage in the PBFT protocol.
 *
 * @param msg Batch of Transactions of type CientQueryBatch from the client.
 * @return RC
 */
RC WorkerThread::process_client_batch(Message *msg)
{

    ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;

    // printf("CL_BATCH: %ld\t from: %ld  Thread: %ld   %s\n", msg->txn_id, msg->return_node_id, get_thd_id(), clbtch->is_cross_shard ? "Multi" : "Single");
    // fflush(stdout);

    // Authenticate the client signature.
    validate_msg(clbtch);
    if (clbtch->is_cross_shard)
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            if (i < get_shard_number(g_node_id))
                assert(!clbtch->involved_shards[i]);
            if (i == get_shard_number(g_node_id))
                assert(clbtch->involved_shards[i]);
        }

#if VIEW_CHANGES
    // If message forwarded to the non-primary.
    if (g_node_id != view_to_primary(get_current_view(get_thd_id())))
    {
        client_query_check(clbtch);
        return RCOK;
    }

    // Partial failure of Primary 0.
    fail_primary(msg, 10 * BILLION);
#endif

    // Initialize all transaction mangers and Send BatchRequests message.
    create_and_send_batchreq(clbtch, clbtch->txn_id);

    return RCOK;
}

/**
 * Process incoming BatchRequests message from the Primary.
 *
 * This function is used by the non-primary or backup replicas to process an incoming
 * BatchRequests message sent by the primary replica. This processing would require 
 * sending messages of type PBFTPrepMessage, which correspond to the Prepare phase of 
 * the PBFT protocol. Due to network delays, it is possible that a repica may have 
 * received some messages of type PBFTPrepMessage and PBFTCommitMessage, prior to 
 * receiving this BatchRequests message.
 *
 * @param msg Batch of Transactions of type BatchRequests from the primary.
 * @return RC
 */
RC WorkerThread::process_batch(Message *msg)
{
    // uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;

    // printf("BATCH_REQ: %ld  from: %ld    %s\n", breq->txn_id, breq->return_node_id, breq->is_cross_shard ? "Multi" : "Single");
    // fflush(stdout);

    // Assert that only a non-primary replica has received this message.
    assert(g_node_id != get_current_view(get_thd_id()));

    // Check if the message is valid.
    validate_msg(breq);

#if VIEW_CHANGES
    // Store the batch as it could be needed during view changes.
    store_batch_msg(breq);
#endif

    // Allocate transaction managers for all the transactions in the batch.
    set_txn_man_fields(breq, 0);

#if TIMER_ON
    // The timer for this client batch stores the hash of last request.
    add_timer(breq, txn_man->get_hash());
#endif

    // Storing the BatchRequests message.
    txn_man->set_primarybatch(breq);

    // Send Prepare messages.
    txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
    // double timepre = get_sys_clock() - cntime;
    // INC_STATS(get_thd_id(), time_pre_prepare, timepre);

    // Only when BatchRequests message comes after some Prepare message.
    for (uint64_t i = 0; i < txn_man->info_prepare.size(); i++)
    {
        // Decrement.
        uint64_t num_prep = txn_man->decr_prep_rsp_cnt();
        if (num_prep == 0)
        {
            txn_man->set_prepared();
            break;
        }
    }
    if (breq->is_cross_shard)
    {
        //txn_id x99
        digest_directory.add(breq->hash, breq->txn_id);
        txn_man->is_cross_shard = true;
        for (uint64_t i = 0; i < g_shard_cnt; i++)
            txn_man->involved_shards[i] = breq->involved_shards[i];
    }
    // If enough Prepare messages have already arrived.
    if (txn_man->is_prepared())
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        // double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
        // INC_STATS(get_thd_id(), time_prepare, timeprep);
        // double timediff = get_sys_clock() - cntime;

        // Check if any Commit messages arrived before this BatchRequests message.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            uint64_t num_comm = txn_man->decr_commit_rsp_cnt();
            if (num_comm == 0)
            {
                txn_man->set_committed();
                break;
            }
        }

        // If enough Commit messages have already arrived.
        if (txn_man->is_committed())
        {
#if TIMER_ON
            // End the timer for this client batch.
            remove_timer(txn_man->hash);
#endif
            // Proceed to executing this batch of transactions.
            send_execute_msg();

            // End the commit counter.
            // INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
        }
    }
    else
    {
        // Although batch has not prepared, still some commit messages could have arrived.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            txn_man->decr_commit_rsp_cnt();
        }
    }
    // Release this txn_man for other threads to use.
    bool ready = txn_man->set_ready();
    assert(ready);

    // UnSetting the ready for the txn id representing this batch.
    txn_man = get_transaction_manager(msg->txn_id, 0);
    unset_ready_txn(txn_man);

    return RCOK;
}

/**
 * Processes incoming Prepare message.
 *
 * This functions precessing incoming messages of type PBFTPrepMessage. If a replica 
 * received 2f identical Prepare messages from distinct replicas, then it creates 
 * and sends a PBFTCommitMessage to all the other replicas.
 *
 * @param msg Prepare message of type PBFTPrepMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_prep_msg(Message *msg)
{
    // cout << "PREP: " << msg->txn_id << "\t from: " << msg->return_node_id << "  thread:   " << get_thd_id() << endl;
    // fflush(stdout);

    // Start the counter for prepare phase.
    if (txn_man->prep_rsp_cnt == 2 * g_min_invalid_nodes)
    {
        txn_man->txn_stats.time_start_prepare = get_sys_clock();
    }

    // Check if the incoming message is valid.
    PBFTPrepMessage *pmsg = (PBFTPrepMessage *)msg;
    validate_msg(pmsg);

    // Check if sufficient number of Prepare messages have arrived.
    if (prepared(pmsg))
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        // End the prepare counter.
        INC_STATS(get_thd_id(), time_prepare, get_sys_clock() - txn_man->txn_stats.time_start_prepare);
    }

    return RCOK;
}

/**
 * Checks if the incoming PBFTCommitMessage can be accepted.
 *
 * This functions checks if the hash and view of the commit message matches that of 
 * the Pre-Prepare message. Once 2f+1 messages are received it returns a true and 
 * sets the `is_committed` flag for furtue identification.
 *
 * @param msg PBFTCommitMessage.
 * @return bool True if the transactions of this batch can be executed.
 */
bool WorkerThread::committed_local(PBFTCommitMessage *msg)
{
    //cout << "Check Commit: TID: " << txn_man->get_txn_id() << "\n";
    //fflush(stdout);

    // Once committed is set for this transaction, no further processing.
    if (txn_man->is_committed())
    {
        return false;
    }

    // If BatchRequests messages has not arrived, then hash is empty; return false.
    if (txn_man->get_hash().empty())
    {
        //cout << "hash empty: " << txn_man->get_txn_id() << "\n";
        //fflush(stdout);
        txn_man->add_commit_msg(msg);
        txn_man->info_commit.push_back(msg->return_node);
        return false;
    }
    else
    {
        if (!checkMsg(msg))
        {
            // If message did not match.
            //cout << txn_man->get_hash() << " :: " << msg->hash << "\n";
            //cout << get_current_view(get_thd_id()) << " :: " << msg->view << "\n";
            //fflush(stdout);
            return false;
        }
    }

    uint64_t comm_cnt = txn_man->decr_commit_rsp_cnt();

    txn_man->add_commit_msg(msg);
    if (comm_cnt == 0 && txn_man->is_prepared())
    {
        txn_man->set_committed();
        return true;
    }

    return false;
}

/**
 * Processes incoming Commit message.
 *
 * This functions precessing incoming messages of type PBFTCommitMessage. If a replica 
 * received 2f+1 identical Commit messages from distinct replicas, then it asks the 
 * execute-thread to execute all the transactions in this batch.
 *
 * @param msg Commit message of type PBFTCommitMessage from a replica.
 * @return RC
 */
RC WorkerThread::process_pbft_commit_msg(Message *msg)
{
    // cout << "COMM: TID " << msg->txn_id << "\t from: " << msg->return_node_id << "\n"; // txn_id 99
    // fflush(stdout);

    if (txn_man->commit_rsp_cnt == 2 * g_min_invalid_nodes + 1)
    {
        txn_man->txn_stats.time_start_commit = get_sys_clock();
    }

    // Check if message is valid.
    PBFTCommitMessage *pcmsg = (PBFTCommitMessage *)msg;
    validate_msg(pcmsg);

    txn_man->add_commit_msg(pcmsg);

    // Check if sufficient number of Commit messages have arrived.
    if (committed_local(pcmsg))
    {
        // cout << "committed_local  " << msg->txn_id << endl;
#if TIMER_ON
        // End the timer for this client batch.
        remove_timer(txn_man->hash);
#endif
        if (txn_man->is_cross_shard)
        {
            if (is_sending_ccm(g_node_id))
            {
                CommitCertificateMessage *ccm = (CommitCertificateMessage *)Message::create_message(txn_man, COMMIT_CERT_MSG);
                ccm->txn_id = txn_man->txn->txn_id;

                vector<uint64_t> dest;
                for (uint64_t i = get_shard_number(g_node_id) + 1; i < g_shard_cnt; i++)
                {
                    if (txn_man->involved_shards[i])
                    {
                        dest.push_back(sending_ccm_to(g_node_id, i));
                        break;
                    }
                }

                if (dest.size())
                {
                    msg_queue.enqueue(get_thd_id(), ccm, dest);
                    //cout << "sending CCM" << dest[0] << endl;fflush(stdout);
                    dest.clear();
                }
                else
                {

                    RingBFTCommit *rcm = (RingBFTCommit *)Message::create_message(txn_man, RING_COMMIT);
                    rcm->txn_id = txn_man->txn->txn_id;

                    vector<uint64_t> dest;
                    for (uint64_t i = 0; i < g_shard_cnt; i++)
                    {
                        if (txn_man->involved_shards[i])
                        {
                            dest.push_back(sending_ccm_to(g_node_id, i));
                            break;
                        }
                    }
                    msg_queue.enqueue(get_thd_id(), rcm, dest);
                    dest.clear();
                    // cout << "sending RCM" << dest[0] << endl;
                    // fflush(stdout);
                    Message::release_message(ccm);
                }
            }
            int exec_flag = 1;
            for (uint64_t i = get_shard_number(g_node_id) + 1; i < g_shard_cnt; i++)
            {
                if (txn_man->involved_shards[i])
                {
                    exec_flag = 0;
                }
            }
            if (exec_flag)
            {
                send_execute_msg();
            }
        }
        else
        {
            send_execute_msg();
        }

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
    }

    return RCOK;
}

RC WorkerThread::process_commit_certificate(Message *msg)
{

    CommitCertificateMessage *ccm = (CommitCertificateMessage *)msg;
    // ONLY NON PRIMARY NODES.

    //printf("PROCESS CCM   FROM: %ld THREAD: %ld %s\n", msg->return_node_id, get_thd_id(),hexStr(ccm->commit_hash.c_str(), 32).c_str());fflush(stdout);
    // cout << "process from   " << msg->return_node_id << "  "  << hexStr(ccm->commit_hash.c_str(), 32) << endl;fflush(stdout);
    if (!ccm->validate())
    {
        assert(0);
    }
    char *buf = create_msg_buffer(ccm);
    Message *ccm_to_save = deep_copy_msg(buf, ccm);
    Message *ccm_message_to_forward = deep_copy_msg(buf, ccm);
    delete_msg_buffer(buf);
    assert(ccm->commit_hash == ccm->hash);
    ccm_checklist.add(ccm->hash);
    ccm_directory.add(ccm->commit_hash, (CommitCertificateMessage *)ccm_to_save);

    //local broadcast
    CommitCertificateMessage *ccm_to_forward = (CommitCertificateMessage *)ccm_message_to_forward;
    ccm_to_forward->forwarding_from = ccm->forwarding_from == (uint64_t)-1 ? ccm->return_node_id : ccm->forwarding_from;
    vector<uint64_t> dest;
    for (uint64_t i = get_shard_number(g_node_id) * g_shard_size; i < get_shard_number(g_node_id) * g_shard_size + g_shard_size; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }
        dest.push_back(i);
    }
    msg_queue.enqueue(get_thd_id(), ccm_to_forward, dest);
    dest.clear();
    if (is_primary_node(get_thd_id(), g_node_id))
    {
        ccm_to_save->txn_id = get_and_inc_next_idx();
        create_and_send_pre_prepare((CommitCertificateMessage *)ccm_to_save, ccm_to_save->txn_id);
    }

    return RCOK;
}

void WorkerThread::create_and_send_pre_prepare(CommitCertificateMessage *msg, uint64_t tid)
{
    // Creating a new BatchRequests Message.
    Message *pmsg = Message::create_message(RING_PRE_PREPARE);
    RingBFTPrePrepare *pre_prepare = (RingBFTPrePrepare *)pmsg;
    pre_prepare->init(get_thd_id());

    // Allocate transaction manager for all the requests in batch.
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        uint64_t txn_id = get_batch_size() * tid + i;
        txn_man = get_transaction_manager(txn_id, 0);

        // Unset this txn man so that no other thread can concurrently use.
        unset_ready_txn(txn_man);

        txn_man->register_thread(this);

        init_txn_man(msg->requestMsg[i]);

        // Reset this txn manager.
        bool ready = txn_man->set_ready();
        assert(ready);
    }

    // Now we need to unset the txn_man again for the last txn of batch.
    unset_ready_txn(txn_man);

    // Generating the hash representing the whole batch in last txn man.

    txn_man->set_hash(msg->commit_hash);
    txn_man->hashSize = txn_man->hash.length();

    pre_prepare->hash = msg->commit_hash;
    pre_prepare->hashSize = pre_prepare->hash.size();
    pre_prepare->batch_size = get_batch_size();
    pre_prepare->txn_id = get_batch_size() * (tid + 1) - 3;
    digest_directory.add(msg->commit_hash, pre_prepare->txn_id);

    // txn_id x99
    txn_man->is_cross_shard = true;

    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        txn_man->involved_shards[i] = msg->involved_shards[i];
        pre_prepare->involved_shards[i] = msg->involved_shards[i];
    }

    vector<uint64_t> dest;
    for (uint64_t i = 0; i < g_node_cnt; i++)
    {
        if (i == g_node_id || !is_in_same_shard(i, g_node_id))
        {
            continue;
        }
        dest.push_back(i);
    }
    msg_queue.enqueue(get_thd_id(), pre_prepare, dest);
    dest.clear();
}

RC WorkerThread::process_ringbft_preprepare(Message *msg)
{
    uint64_t cntime = get_sys_clock();

    RingBFTPrePrepare *breq = (RingBFTPrePrepare *)msg;

    // printf("PRE_PREPARE: %ld\t from: %ld : \t\n", breq->txn_id, breq->return_node_id); // x97
    // fflush(stdout);

    // Assert that only a non-primary replica has received this message.
    assert(g_node_id != get_current_view(get_thd_id()));

    // Check if the message is valid.
    breq->validate(get_thd_id());

    CommitCertificateMessage *ccm = ccm_directory.get(breq->hash);
    // Allocate transaction managers for all the transactions in the batch.
    uint64_t first = msg->txn_id - get_batch_size() + 3;
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        txn_man = get_transaction_manager(first + i, 0);

        unset_ready_txn(txn_man);

        txn_man->register_thread(this);
        txn_man->return_id = ccm->return_node_id;

        // Fields that need to updated according to the specific algorithm.
        algorithm_specific_update(breq, i);

        init_txn_man(ccm->requestMsg[i]);
        bool ready = txn_man->set_ready();
        assert(ready);
    }
    digest_directory.add(breq->hash, msg->txn_id);
    // We need to unset txn_man again for last txn in the batch.
    unset_ready_txn(txn_man);

    txn_man->set_hash(ccm->hash);

#if TIMER_ON
    // The timer for this client batch stores the hash of last request.
    add_timer(breq, txn_man->get_hash());
#endif

    // Storing the BatchRequests message.
    // txn_man->set_primarybatch(breq);

    // Send Prepare messages.
    txn_man->send_pbft_prep_msgs();

    // End the counter for pre-prepare phase as prepare phase starts next.
    double timepre = get_sys_clock() - cntime;
    INC_STATS(get_thd_id(), time_pre_prepare, timepre);

    // Only when BatchRequests message comes after some Prepare message.
    for (uint64_t i = 0; i < txn_man->info_prepare.size(); i++)
    {
        // Decrement.
        uint64_t num_prep = txn_man->decr_prep_rsp_cnt();
        if (num_prep == 0)
        {
            txn_man->set_prepared();
            break;
        }
    }

    txn_man->is_cross_shard = true;
    for (uint64_t i = 0; i < g_shard_cnt; i++)
        txn_man->involved_shards[i] = breq->involved_shards[i];

    // If enough Prepare messages have already arrived.
    if (txn_man->is_prepared())
    {
        // Send Commit messages.
        txn_man->send_pbft_commit_msgs();

        double timeprep = get_sys_clock() - txn_man->txn_stats.time_start_prepare - timepre;
        INC_STATS(get_thd_id(), time_prepare, timeprep);
        double timediff = get_sys_clock() - cntime;

        // Check if any Commit messages arrived before this BatchRequests message.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            uint64_t num_comm = txn_man->decr_commit_rsp_cnt();
            if (num_comm == 0)
            {
                txn_man->set_committed();
                break;
            }
        }

        // If enough Commit messages have already arrived.
        if (txn_man->is_committed())
        {
#if TIMER_ON
            // End the timer for this client batch.
            remove_timer(txn_man->hash);
#endif
            // Proceed to executing this batch of transactions.
            if (is_sending_ccm(g_node_id))
            {
                CommitCertificateMessage *ccm = (CommitCertificateMessage *)Message::create_message(txn_man, COMMIT_CERT_MSG);
                ccm->txn_id = txn_man->txn->txn_id;

                vector<uint64_t> dest;
                for (uint64_t i = get_shard_number(g_node_id) + 1; i < g_shard_cnt; i++)
                {
                    if (txn_man->involved_shards[i])
                    {
                        dest.push_back(sending_ccm_to(g_node_id, i));
                        break;
                    }
                }

                if (dest.size())
                {
                    msg_queue.enqueue(get_thd_id(), ccm, dest);
                    //cout << "sending CCM" << dest[0] << endl;fflush(stdout);
                    dest.clear();
                }
                else
                {

                    RingBFTCommit *rcm = (RingBFTCommit *)Message::create_message(txn_man, RING_COMMIT);
                    rcm->txn_id = txn_man->txn->txn_id;

                    vector<uint64_t> dest;
                    for (uint64_t i = 0; i < g_shard_cnt; i++)
                    {
                        if (txn_man->involved_shards[i])
                        {
                            dest.push_back(sending_ccm_to(g_node_id, i));
                            break;
                        }
                    }
                    msg_queue.enqueue(get_thd_id(), rcm, dest);
                    dest.clear();
                    // cout << "sending RCM" << dest[0] << endl;
                    // fflush(stdout);
                    Message::release_message(ccm);
                }
            }
            int exec_flag = 1;
            for (uint64_t i = get_shard_number(g_node_id) + 1; i < g_shard_cnt; i++)
            {
                if (txn_man->involved_shards[i])
                {
                    exec_flag = 0;
                }
            }
            if (exec_flag)
            {
                send_execute_msg();
            }

            // End the commit counter.
            INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit - timediff);
        }
    }
    else
    {
        // Although batch has not prepared, still some commit messages could have arrived.
        for (uint64_t i = 0; i < txn_man->info_commit.size(); i++)
        {
            txn_man->decr_commit_rsp_cnt();
        }
    }

    // Release this txn_man for other threads to use.
    bool ready = txn_man->set_ready();
    assert(ready);

    // UnSetting the ready for the txn id representing this batch.
    txn_man = get_transaction_manager(msg->txn_id, 0);
    unset_ready_txn(txn_man);

    return RCOK;
}

RC WorkerThread::process_ringbft_commit(Message *msg)
{

    RingBFTCommit *rcm = (RingBFTCommit *)msg;
    // ONLY NON PRIMARY NODES.
    // cout << hexStr(rcm->commit_hash.c_str(), rcm->commit_hash_size) << endl;

    // printf("PROCESS RCM %ld;%ld\tFROM: %ld THREAD: %ld\n", msg->txn_id, txn_man->get_txn_id(), msg->return_node_id, get_thd_id());
    // fflush(stdout);
    rcm->validate();
    char *buf = create_msg_buffer(rcm);
    // Message *rcm_to_save = deep_copy_msg(buf, rcm);
    Message *rcm_message_to_forward = deep_copy_msg(buf, rcm);
    delete_msg_buffer(buf);

    // rcm_directory.add(rcm->commit_hash, (RingBFTCommit *)rcm_to_save);
    rcm_checklist.add(rcm->hash);

    RingBFTCommit *rcm_to_forward = (RingBFTCommit *)rcm_message_to_forward;
    rcm_to_forward->forwarding_from = rcm->forwarding_from == (uint64_t)-1 ? rcm->return_node_id : rcm->forwarding_from;
    vector<uint64_t> dest;

    // Inter Shard Transfer
    if (is_sending_ccm(g_node_id))
        for (uint64_t i = get_shard_number(g_node_id) + 1; i < g_shard_cnt; i++)
        {
            if (txn_man->involved_shards[i])
            {
                if (i != get_shard_number(rcm_to_forward->forwarding_from))
                {
                    dest.push_back(sending_ccm_to(g_node_id, i));
                    break;
                }
            }
        }
    // localbroadcast
    for (uint64_t i = get_shard_number(g_node_id) * g_shard_size; i < get_shard_number(g_node_id) * g_shard_size + g_shard_size; i++)
    {
        if (i == g_node_id)
        {
            continue;
        }
        dest.push_back(i);
    }
    msg_queue.enqueue(get_thd_id(), rcm_to_forward, dest);
    dest.clear();
    send_execute_msg();

    return RCOK;
}

#endif