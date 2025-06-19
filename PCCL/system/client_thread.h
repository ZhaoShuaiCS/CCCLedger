#ifndef _CLIENT_THREAD_H_
#define _CLIENT_THREAD_H_

#include "global.h"

#if VIEW_CHANGES == true || LOCAL_FAULT
#include "message.h"
#endif

class Workload;

class ClientThread : public Thread
{
public:
    RC run();

#if VIEW_CHANGES == true
    void resend_msg(ClientQueryBatch *symsg);
#endif

    void setup();
    void send_key();

#if GEN_ZIPF
    static double denom;
    myrand *sbmrand;
    double zeta_2_theta;
    uint64_t zipf(uint64_t n, double theta);
    double zeta(uint64_t n, double theta);
#endif

private:
    uint64_t last_send_time;
    uint64_t send_interval;
#if RING_BFT || SHARPER
    uint64_t txn_batch_sent_cnt;
    uint64_t cross_txn_batch_sent_cnt;
#endif
};

#endif
