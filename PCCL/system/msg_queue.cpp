#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "pool.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void MessageQueue::init()
{
    //m_queue = new boost::lockfree::queue<msg_entry* > (0);
    m_queue = new boost::lockfree::queue<msg_entry *> *[g_this_send_thread_cnt];
#if NETWORK_DELAY_TEST
    cl_m_queue = new boost::lockfree::queue<msg_entry *> *[g_this_send_thread_cnt];
#endif
    for (uint64_t i = 0; i < g_this_send_thread_cnt; i++)
    {
        m_queue[i] = new boost::lockfree::queue<msg_entry *>(0);
#if NETWORK_DELAY_TEST
        cl_m_queue[i] = new boost::lockfree::queue<msg_entry *>(0);
#endif
    }
    ctr = new uint64_t *[g_this_send_thread_cnt];
    for (uint64_t i = 0; i < g_this_send_thread_cnt; i++)
    {
        ctr[i] = (uint64_t *)mem_allocator.align_alloc(sizeof(uint64_t));
        *ctr[i] = i % g_thread_cnt;
    }
    for (uint64_t i = 0; i < g_this_send_thread_cnt; i++)
        sthd_m_cache.push_back(NULL);
}

void MessageQueue::release()
 {
     if(m_queue){
         for (uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
             msg_entry *entry = NULL;
             while(m_queue[i]->pop(entry)){
                 if(entry&&entry->msg){
                     Message::release_message(entry->msg);
                 }
             }
             delete m_queue[i];
         }
         delete m_queue;
         m_queue = nullptr;
     }

 #if NETWORK_DELAY_TEST
     if(cl_m_queue){
         for (uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
             delete cl_m_queue[i];
         }
         delete cl_m_queue;
         cl_m_queue = nullptr;
     }
 #endif

     if(ctr){
         for (uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
             delete ctr[i];
         }
         delete ctr;
         ctr = nullptr;
     }
 }

void MessageQueue::enqueue(uint64_t thd_id, Message *msg, const vector<uint64_t> &dest)
{

    msg_entry *entry = (msg_entry *)mem_allocator.alloc(sizeof(struct msg_entry));
    new (entry) msg_entry();

    entry->msg = msg;
    if (msg == NULL)
    {
        assert(0);
        return;
    }

    /* 
        We sign the messages here before sending it to some replica.
        This idea works till every replica needs to generate a different signature
        for every other replica.
    */
    switch (msg->get_rtype())
    {
    case KEYEX:
        break;
    case CL_RSP:
        ((ClientResponseMessage *)msg)->sign(dest[0]);
        entry->allsign.push_back(((ClientResponseMessage *)msg)->signature);
        break;

    case CL_BATCH:
        ((ClientQueryBatch *)msg)->sign(dest[0]);
        entry->allsign.push_back(msg->signature);
        break;
#if SHARPER
    case SUPER_PROPOSE:
#endif
    case BATCH_REQ:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            //cout << "test_v1:enter for (uint64_t i = 0; i < dest.size(); i++)\n";
            ((BatchRequests *)msg)->sign(dest[i]);
            //cout << "test_v1:after ((BatchRequests *)msg)->sign(dest[i])\n";
            entry->allsign.push_back(msg->signature);
            //cout << "test_v1:after entry->allsign.push_back(msg->signature);\n";
        }
        //cout << "test_v1:out of (uint64_t i = 0; i < dest.size(); i++)\n";
        break;
    case PBFT_CHKPT_MSG:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((CheckpointMessage *)msg)->sign(dest[i]);
            entry->allsign.push_back(((CheckpointMessage *)msg)->signature);
        }
        break;
    case PBFT_PREP_MSG:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((PBFTPrepMessage *)msg)->sign(dest[i]);
            entry->allsign.push_back(((PBFTPrepMessage *)msg)->signature);
        }
        break;
    case BROADCAST_BATCH:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((BroadcastBatchMessage *)msg)->sign(dest[i]);
            entry->allsign.push_back(((BroadcastBatchMessage *)msg)->signature);
        }
        break;
#if CONSENSUS == PBFT && !RING_BFT
    case PBFT_COMMIT_MSG:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((PBFTCommitMessage *)msg)->sign(dest[i]);
            entry->allsign.push_back(((PBFTCommitMessage *)msg)->signature);
        }
        break;
#elif RING_BFT
    case PBFT_COMMIT_MSG:
        if (((PBFTCommitMessage *)msg)->is_cross_shard)
        {
            // cout << "ED2  "<< msg->txn_id / get_batch_size() << endl;fflush(stdout);
            ((PBFTCommitMessage *)msg)->sign(dest[0]);
            for (uint64_t i = 0; i < dest.size(); i++)
            {
                entry->allsign.push_back(((PBFTCommitMessage *)msg)->signature);
            }
        }
        else
        {
            // cout << "MAC  "<< msg->txn_id / get_batch_size() << endl;fflush(stdout);
            for (uint64_t i = 0; i < dest.size(); i++)
            {
                ((PBFTCommitMessage *)msg)->sign(dest[i]);
                entry->allsign.push_back(((PBFTCommitMessage *)msg)->signature);
            }
        }
        break;
    case COMMIT_CERT_MSG:
        if (((CommitCertificateMessage *)msg)->forwarding_from == (uint64_t)-1)
            ((CommitCertificateMessage *)msg)->sign(dest[0]);
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            entry->allsign.push_back(((CommitCertificateMessage *)msg)->signature);
        }
        break;
    case RING_PRE_PREPARE:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((RingBFTPrePrepare *)msg)->sign(dest[i]);
            entry->allsign.push_back(msg->signature);
        }
        break;
    case RING_COMMIT:
        if (((RingBFTCommit *)msg)->forwarding_from == (uint64_t)-1)
            ((RingBFTCommit *)msg)->sign(dest[0]);
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            entry->allsign.push_back(((RingBFTCommit *)msg)->signature);
        }
        break;
#endif

#if VIEW_CHANGES
    case VIEW_CHANGE:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((ViewChangeMsg *)msg)->sign(dest[i]);
            entry->allsign.push_back(((ViewChangeMsg *)msg)->signature);
        }
        break;
    case NEW_VIEW:
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            ((NewViewMsg *)msg)->sign(dest[i]);
            entry->allsign.push_back(((NewViewMsg *)msg)->signature);
        }
        break;
#endif
    default:
        break;
    }

    // Depending on the type of message either we place in queues of all the
    // output thread or only a sepecific output thread.
    switch (msg->get_rtype())
    {
    case INIT_DONE:
    case READY:
    case KEYEX:
    case CL_RSP:
    case CL_BATCH:
    {
        // Based on the destination (only 1), messages are placed in the queue.
        entry->starttime = get_sys_clock();
        entry->msg->dest.push_back(dest[0]);

        uint64_t rand = dest[0] % g_this_send_thread_cnt;
        while (!m_queue[rand]->push(entry) && !simulation->is_done())
        {
        }

        #if SEMA_TEST
        if(ISSERVER){
            // After a msg is enqueued, increase the value of output_semaphore by 1
            sem_post(&output_semaphore[rand]);
        }
        #endif

        INC_STATS(thd_id, msg_queue_enq_cnt, 1);
        break;
    }
#if SHARPER
    case SUPER_PROPOSE:
#endif
    case BATCH_REQ:
    case PBFT_CHKPT_MSG:
    case PBFT_PREP_MSG:
    case PBFT_COMMIT_MSG:
    case BROADCAST_BATCH:

#if RING_BFT
    case COMMIT_CERT_MSG:
    case RING_PRE_PREPARE:
    case RING_COMMIT:
#endif

#if VIEW_CHANGES
    case VIEW_CHANGE:
    case NEW_VIEW:
#endif
    {
        // Putting in queue of all the output threads as destinations differ.
        char *buf = create_msg_buffer(entry->msg);
        uint64_t j = 0;
        for (; j < g_this_send_thread_cnt - 1; j++)
        {
            // if (msg->get_rtype() == BATCH_REQ){
            //     //cout << "test_v3:MessageQueue::enqueue1:breq->batch_id = "<< msg->batch_id << "\n";
            // }
            // if (msg->get_rtype() == BROADCAST_BATCH){
            //     BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)msg;
            //     cout << "test_v4:MessageQueue::enqueue1:send bbmsg->batch_id = "<< msg->batch_id << "net_id = " << bbmsg->net_id<<"\n";
            // }
#if TRANSPORT_OPTIMIZATION
            if(dest.size() == 1 && dest[0] % g_this_send_thread_cnt != j){
                continue;
            }
#endif
            msg_entry *entry2 = (msg_entry *)mem_allocator.alloc(sizeof(struct msg_entry));
            //msg_pool.get(entry2);
            new (entry2) msg_entry();

            Message *deepCMsg = deep_copy_msg(buf, entry->msg);
            entry2->msg = deepCMsg;
            for (uint64_t i = 0; i < dest.size(); i++)
            {
                entry2->msg->dest.push_back(dest[i]);
            }
            for (uint64_t i = 0; i < entry->allsign.size(); i++)
            {
                entry2->allsign.push_back(entry->allsign[i]);
            }
            entry2->starttime = get_sys_clock();
            // if (msg->get_rtype() == BATCH_REQ){
            //     cout << "test_v3:MessageQueue::enqueue2:entry2->batch_id = "<< entry2->msg->batch_id << ", entry->batch_id ="<< entry->msg->batch_id << ", deepCMsg->batch_id ="<< deepCMsg->batch_id <<"\n";
            // }

            while (!m_queue[j]->push(entry2) && !simulation->is_done())
            {
            }

            #if SEMA_TEST
            if(ISSERVER)
                // After a msg is enqueued, increase the value of output_semaphore by 1
                sem_post(&output_semaphore[j]);
            #endif

            INC_STATS(thd_id, msg_queue_enq_cnt, 1);
        }
#if TRANSPORT_OPTIMIZATION
        if(dest.size() == 1 && dest[0] % g_this_send_thread_cnt != j){
            delete_msg_buffer(buf);
            break;
        }
#endif
        // Putting in queue of the last output thread.
        for (uint64_t i = 0; i < dest.size(); i++)
        {
            entry->msg->dest.push_back(dest[i]);
        }
        entry->starttime = get_sys_clock();
        while (!m_queue[j]->push(entry) && !simulation->is_done())
        {
        }

        #if SEMA_TEST
        if(ISSERVER)
            // After a msg is enqueued, increase the value of output_semaphore by 1
            sem_post(&output_semaphore[j]);
        #endif

        INC_STATS(thd_id, msg_queue_enq_cnt, 1);

        delete_msg_buffer(buf);
        break;
    }
    default:
        break;
    }
}

void MessageQueue::dequeue(uint64_t thd_id, vector<string> &allsign, Message *&msg)
{
    msg_entry *entry = NULL;
    vector<uint64_t> dest;
    bool valid = false;

#if TRANSPORT_OPTIMIZATION
    uint64_t td_id = 0;
    if(ISSERVER)
        td_id = thd_id - g_this_rem_thread_cnt - g_thread_cnt;
    else
        td_id = thd_id - g_this_rem_thread_cnt - g_client_thread_cnt;
#endif

#if NETWORK_DELAY_TEST
    valid = cl_m_queue[td_id % g_this_send_thread_cnt]->pop(entry);
    if (!valid)
    {
        entry = sthd_m_cache[td_id % g_this_send_thread_cnt];
        if (entry)
            valid = true;
        else
            valid = m_queue[td_id % g_this_send_thread_cnt]->pop(entry);
    }
#else
    valid = m_queue[td_id % g_this_send_thread_cnt]->pop(entry);
#endif

    uint64_t curr_time = get_sys_clock();
    if (valid)
    {
        assert(entry);
#if NETWORK_DELAY_TEST
        if (!ISCLIENTN(entry->dest))
        {
            if (ISSERVER && (get_sys_clock() - entry->starttime) < g_network_delay)
            {
                sthd_m_cache[td_id % g_this_send_thread_cnt] = entry;
                INC_STATS(thd_id, mtx[5], get_sys_clock() - curr_time);
                return UINT64_MAX;
            }
            else
            {
                sthd_m_cache[td_id % g_this_send_thread_cnt] = NULL;
            }
            if (ISSERVER)
            {
                INC_STATS(thd_id, mtx[38], 1);
                INC_STATS(thd_id, mtx[39], curr_time - entry->starttime);
            }
        }

#endif

        msg = entry->msg;
        // if (msg->get_rtype() == BROADCAST_BATCH){
        //     BroadcastBatchMessage *bbmsg = (BroadcastBatchMessage *)msg;
        //     cout << "test_v4:MessageQueue::dequeue1:receive bbmsg->batch_id = "<< msg->batch_id << "net_id = " << bbmsg->net_id << "\n";
        // }
        // if(msg->get_rtype() == BATCH_REQ)
        // {
        //     cout << "test_v3:MessageQueue::dequeue1:msg->batch_id = "<< msg->batch_id << "\n";
        // }
        for (uint64_t i = 0; i < msg->dest.size(); i++)
        {
            dest.push_back(msg->dest[i]);
        }
        for (uint64_t i = 0; i < entry->allsign.size(); i++)
        {
            allsign.push_back(entry->allsign[i]);
        }

        //printf("MQ Dequeue: %d :: Thd: %ld \n",msg->rtype,thd_id);
        //fflush(stdout);

        INC_STATS(thd_id, msg_queue_delay_time, curr_time - entry->starttime);
        INC_STATS(thd_id, msg_queue_cnt, 1);
        msg->mq_time = curr_time - entry->starttime;
        DEBUG_M("MessageQueue::enqueue msg_entry free\n");
        delete entry;
        return;
    }
    msg = NULL;
    return;
}
