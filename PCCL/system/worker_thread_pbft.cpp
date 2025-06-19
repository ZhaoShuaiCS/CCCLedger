#include "global.h"
#include "message.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "message.h"
#include "timer.h"
#include "chain.h"

#if !RING_BFT && !SHARPER
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
    #if STRONG_SERIAL
        //cout << "test_v4:primary locked\n";
        sem_wait(&consensus_lock);
        //cout << "test_v4:primary unlocked\n";
    #endif
    ClientQueryBatch *clbtch = (ClientQueryBatch *)msg;
    //DEBUG("process_client_batch! requests.size: %ld requests_writeset.size: %ld\n",clbtch->cqrySet[0]->requests.size(), clbtch->cqrySet[0]->requests_writeset.size());
    //cout << "test_v3:process_client_batch(Message *msg)::test_v3:msg->txn_id = " << msg->txn_id << "\n";
    //cout << "test_v3:msg->batch_id = " << msg->batch_id << "\n";
    #if KDK_DEBUG1
    printf("ClientQueryBatch: %ld, THD: %ld :: CL: %ld :: RQ: %ld\n", msg->txn_id, get_thd_id(), msg->return_node_id, clbtch->cqrySet[0]->requests[0]->key);
    fflush(stdout);
    #endif

    // Authenticate the client signature.
    //cout << "test_v1:begin validate_msg(clbtch)\n";
    validate_msg(clbtch);

#if VIEW_CHANGES
    // If message forwarded to the non-primary.
    #if MULTI_ON
    if (!(get_primary(clbtch->txn_id%get_totInstances()) == g_node_id))
    #else
    if (g_node_id != get_current_view(get_thd_id()))
    #endif
    {
        client_query_check(clbtch);
        cout << "returning...   " << get_current_view(get_thd_id()) << endl;
        return RCOK;
    }

    // Partial failure of Primary 0.
    //fail_primary(msg, 10 * BILLION);
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
    uint64_t cntime = get_sys_clock();

    BatchRequests *breq = (BatchRequests *)msg;
    #if KDK_DEBUG1
    printf("BatchRequests: TID:%ld : VIEW: %ld : THDB: %ld\n",breq->txn_id, breq->view, get_thd_id());
    fflush(stdout);
    #endif

    // Assert that only a non-primary replica has received this message.
    #if MULTI_ON
    assert(!isPrimary(g_node_id) || !(breq->view == g_node_id));
    #else
    assert(g_node_id != get_current_view(get_thd_id()));
    #endif

    // Check if the message is valid.
    validate_msg(breq);

#if VIEW_CHANGES
    // Store the batch as it could be needed during view changes.
    store_batch_msg(breq);
#endif

    // Allocate transaction managers for all the transactions in the batch.
    set_txn_man_fields(breq, breq->batch_id, g_net_id);
    //cout << "test_v3:process_batch:set_txn_man_fields(breq, breq->batch_id);,  batch_id = "<< breq->batch_id << "\n";

#if TIMER_ON
    // The timer for this client batch stores the hash of last request.
    add_timer(breq, txn_man->get_hash());
#endif

// #if ISEOV
//     // Storing the BatchRequests message.
//     DEBUG("test_v5:print process_batch_readSet\n");
//     uint64_t process_count = 0;
//     for(const auto &item:breq->readSet){
//         DEBUG("test_v5:print process_batch_readSet[%ld]\n", process_count++);
//         for(const auto &item2:item){
//             DEBUG("test_v5:key = %ld, value = %ld\n", item2.first, item2.second);
//         }
//     }
// #endif


    txn_man->set_primarybatch(breq);
    //DEBUG("test_v5:txn_man->set_primarybatch(breq)::txn_man->txnid == %ld\n", txn_man->get_txn_id());
// #if ISEOV
//     // Storing the BatchRequests message.
//     DEBUG("test_v5:print process_batch_readSet\n");
//     uint64_t process_count = 0;
//     for(const auto &item:txn_man->batchreq->readSet){
//         DEBUG("test_v5:print process_batch_readSet[%ld]\n", process_count++);
//         for(const auto &item2:item){
//             DEBUG("test_v5:key = %ld, value = %ld\n", item2.first, item2.second);
//         }
//     }
// #endif

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
#if P2P_BROADCAST
            Message *bmsg = Message::create_message(BROADCAST_BATCH);
            BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)bmsg;
            bbmsg->init();
            bbmsg->batch_id = txn_man->get_batch_id();
            bbmsg->txn_id = txn_man->get_txn_id();
            bbmsg->net_id = g_net_id;
            
            char *buf = create_msg_buffer(txn_man->batchreq);
            Message *deepMsg = deep_copy_msg(buf, txn_man->batchreq);

            bbmsg->add_batch((BatchRequests *)deepMsg);
            for(auto &item:txn_man->commit_msgs)
            {
                bbmsg->add_commit_msg(item);
            }
            vector<uint64_t> dest;

	        for (uint64_t i = 0; i < g_node_cnt; i++)
	        {
            	if (i % g_net_cnt == g_net_id || (i / g_net_cnt != g_net_id / g_net_cnt))
	        	{
	        		continue;
	        	}
                dest.push_back(i);
            }
            msg_queue.enqueue(get_thd_id(), bbmsg, dest);
#endif
            // Proceed to executing this batch of transactions.
            cout << "test_v4:process_batch::before send_execute_msg() = txn_id = " <<  txn_man->get_txn_id() <<"\n";
            send_execute_msg();

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
    txn_man = get_transaction_manager(g_net_id, msg->txn_id, msg->batch_id);
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
    #if KDK_DEBUG1
    // cout << "PBFTPrepMessage: TID: " << msg->txn_id << " FROM: " << msg->return_node_id << " THDP: " << get_thd_id() << endl;
    fflush(stdout);
    #endif
    // Start the counter for prepare phase.
#if MINI_NET
    if (txn_man->prep_rsp_cnt == g_min_invalid_nodes)
#else
    if (txn_man->prep_rsp_cnt == 2 * g_min_invalid_nodes)
#endif
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
    #if KDK_DEBUG1
    // cout << "PBFTCommitMessage: TID " << msg->txn_id << " FROM: " << msg->return_node_id << " THDC: " << get_thd_id() << "\n";
    fflush(stdout);
    #endif
#if MINI_NET
    if (txn_man->commit_rsp_cnt == g_min_invalid_nodes + 1)
#else
    if (txn_man->commit_rsp_cnt == 2 * g_min_invalid_nodes + 1)
#endif
    {
        txn_man->txn_stats.time_start_commit = get_sys_clock();
    }

    // Check if message is valid.
    PBFTCommitMessage *pcmsg = (PBFTCommitMessage *)msg;
    //cout << "test_v4:process_pbft_commit_msg::pcmsg->batch_id = "<<pcmsg->batch_id<<"\n";
    validate_msg(pcmsg);

    txn_man->add_commit_msg(pcmsg);

    // Check if sufficient number of Commit messages have arrived.
    if (committed_local(pcmsg))
    {
        #if STRONG_SERIAL
            if(isPrimary(g_node_id)){
                sem_post(&consensus_lock);
            }
            // else{
            //     cout <<"test_v4:not primary!!\n";
            // }
        #endif
        //cout << "test_v4:process_pbft_commit_msg::committed_local:pcmsg->batch_id = "<<pcmsg->batch_id<<"\n";
#if TIMER_ON
        // End the timer for this client batch.
        remove_timer(txn_man->hash);
#endif

        // if node is the pri of this batch, build BBmsg and sendl
#if NET_BROADCAST
#if !P2P_BROADCAST
        if (priconsensus[msg->batch_id] == true)
#else 
        if(true)
#endif
        {

            Message *bmsg = Message::create_message(BROADCAST_BATCH);
            BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)bmsg;
            bbmsg->init();
            bbmsg->batch_id = pcmsg->batch_id;
            bbmsg->txn_id = pcmsg->txn_id;
            bbmsg->net_id = g_net_id;
            
            char *buf = create_msg_buffer(txn_man->batchreq);
            Message *deepMsg = deep_copy_msg(buf, txn_man->batchreq);

            bbmsg->add_batch((BatchRequests *)deepMsg);
            //cout << "test_v4: bbmsg->add_batch(txn_man->batchreq); batchreq_id = " << bbmsg->breq->batch_id <<"\n";
            for(auto &item:txn_man->commit_msgs)
            {
                bbmsg->add_commit_msg(item);
            }
            vector<uint64_t> dest;

	        for (uint64_t i = 0; i < g_node_cnt; i++)
	        {
            #if P2P_BROADCAST
            	if (i % g_net_cnt == g_net_id || (i / g_net_cnt != g_node_id / g_net_cnt))
	        	{
	        		continue;
	        	}
                //DEBUG_V1("test_v6: send bbmsg to %ld \n", i);
                dest.push_back(i);
            #else
	        	if (i % g_net_cnt == g_net_id)
	        	{
	        		continue;
	        	}
                dest.push_back(i);
            #endif
            }
            //cout << "test_v4:send_broadcast_batch::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id<< ", dest_size = "<< dest.size() << "\n";
            //cout << "test_v4:send_broadcast_batch::, breq->get_size() = " <<  bbmsg->breq->get_size() <<"\n";
            msg_queue.enqueue(get_thd_id(), bbmsg, dest);

            //cout << "test_v4:process_pbft_commit_msg::leader::before send_execute_msg() = txn_id = " <<  pcmsg->txn_id <<"\n";
            send_execute_msg();
        }
        else
#endif 
        {
            //cout << "test_v4:process_pbft_commit_msg::before send_execute_msg() = txn_id = " <<  pcmsg->txn_id <<"\n";
            send_execute_msg();
        }

        // Add this message to execute thread's queue.
        //send_execute_msg();

        INC_STATS(get_thd_id(), time_commit, get_sys_clock() - txn_man->txn_stats.time_start_commit);
    }

    return RCOK;
}
#endif


#if NET_BROADCAST
RC WorkerThread::process_broadcast_batch(Message *msg)
{
    BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)msg;
    //cout << "test_v4:process_broadcast_batch::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id << "\n";
    //cout << "test_v4:process_broadcast_batch::, breq->get_size() = " <<  bbmsg->breq->get_size() <<"\n";
    validate_msg(bbmsg);

    set_txn_man_fields(bbmsg->breq, bbmsg->batch_id, bbmsg->net_id);
    //txn_man->set_ready();
    //cout << "test_v4:process_broadcast_batch::before set_primarybatch::txn_man->net_id = " << txn_man->net_id << ", txn_id = "<< txn_man->get_txn_id() << "\n";

    txn_man->set_primarybatch(bbmsg->breq);
    //cout << "test_v4:process_broadcast_batch::before send_execute_msg()::bbmsg->txn_id = " << bbmsg->txn_id<< ", batch_id = " << bbmsg->batch_id << ", net_id = "<< bbmsg->net_id << ", return_node_id = "<< bbmsg->return_node_id << "\n";
    send_execute_msg();

    return RCOK;
}
#endif