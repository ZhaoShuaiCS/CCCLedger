#include "global.h"
#include "smart_contract.h"
#include "smart_contract_txn.h"

#if BANKING_SMART_CONTRACT
/*
returns:
     1 for commit 
     0 for abort
*/
uint64_t TransferMoneySmartContract::execute()
{
    string temp = db->Get(std::to_string(this->source_id));
    uint64_t source = temp.empty() ? 0 : stoi(temp);
    temp = db->Get(std::to_string(this->dest_id));
    uint64_t dest = temp.empty() ? 0 : stoi(temp);
    if (amount <= source)
    {
        db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
        return 1;
    }
    return 0;
}

/*
returns:
     1 for commit 
*/
uint64_t DepositMoneySmartContract::execute()
{
    string temp = db->Get(std::to_string(this->dest_id));
    uint64_t dest = temp.empty() ? 0 : stoi(temp);
    db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
    return 1;
}

/*
returns:
     1 for commit 
     0 for abort
*/
uint64_t WithdrawMoneySmartContract::execute()
{
    string temp = db->Get(std::to_string(this->source_id));
#if SB_READ_TX
    return 1;
#else
    uint64_t source = temp.empty() ? 0 : stoi(temp);
    if (amount <= source)
    {
        db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        return 1;
    }
#endif
    return 0;
}

#if ISEOV
#if PRE_EX
uint64_t TransferMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    string temp;
    if (speculateSet.find(this->source_id) == speculateSet.end())
    {
        temp = db->Get(std::to_string(this->source_id));
    }
    else {
        temp = std::to_string(speculateSet[this->source_id]);
    }
    
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    readSet[this->source_id] = source;

    if (speculateSet.find(this->dest_id) == speculateSet.end())
    {
        temp = db->Get(std::to_string(this->dest_id));
    }
    else {
        temp = std::to_string(speculateSet[this->dest_id]);
    }
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    readSet[this->dest_id] = dest;
    writeSet[this->source_id] = source - amount;
    writeSet[this->dest_id] = dest + amount;
    speculateSet[this->source_id] = source - amount;
    speculateSet[this->dest_id] = dest + amount;   
    if (amount <= source)
    {
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
        //writeSet[this->source_id] = source - amount;
        //writeSet[this->dest_id] = dest + amount;
        return 1;
    }
    return 0;
}

/*
returns:
     1 for commit 
*/
uint64_t DepositMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    string temp;
    if (speculateSet.find(this->dest_id) == speculateSet.end())
    {
        temp = db->Get(std::to_string(this->dest_id));
    }
    else {
        temp = std::to_string(speculateSet[this->dest_id]);
    }
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    readSet[this->dest_id] = dest;
    //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
    writeSet[this->dest_id] = dest + amount;
    speculateSet[this->dest_id] = dest + amount;
    return 1;
}

uint64_t WithdrawMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    string temp;
    if (speculateSet.find(this->source_id) == speculateSet.end())
    {
        temp = db->Get(std::to_string(this->source_id));
    }
    else {
        temp = std::to_string(speculateSet[this->source_id]);
    }
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    readSet[this->source_id] = source;
#if SB_READ_TX
    writeSet[this->source_id] = source;
    speculateSet[this->source_id] = source;
#else
    writeSet[this->source_id] = source - amount;
    speculateSet[this->source_id] = source - amount;
#endif
    if (amount <= source)
    {
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        //writeSet[this->source_id] = source - amount;
        return 1;
    }
    return 0;
}
#else
uint64_t TransferMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->source_id));
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    readSet[this->source_id] = source;
    temp = db->Get(std::to_string(this->dest_id));
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    readSet[this->dest_id] = dest;
    writeSet[this->source_id] = source - amount;
    writeSet[this->dest_id] = dest + amount;
    if (amount <= source)
    {
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
        //writeSet[this->source_id] = source - amount;
        //writeSet[this->dest_id] = dest + amount;
        return 1;
    }
    return 0;
}

/*
returns:
     1 for commit 
*/
uint64_t DepositMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->dest_id));
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    readSet[this->dest_id] = dest;
    //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
    writeSet[this->dest_id] = dest + amount;
    return 1;
}

uint64_t WithdrawMoneySmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->source_id));
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    readSet[this->source_id] = source;
#if SB_READ_TX
    writeSet[this->source_id] = source;
#else
    writeSet[this->source_id] = source - amount;
#endif
    if (amount <= source)
    {
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        //writeSet[this->source_id] = source - amount;
        return 1;
    }
    return 0;
}
#endif

#if !RE_EXECUTE
uint64_t TransferMoneySmartContract::v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->source_id));
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->source_id] != source){
        DEBUG("test_v5:TransferMoneySmartContract::get_old_source = %ld, now source = %ld\n", readSet[this->source_id], source);
        return 0;
    }
    temp = db->Get(std::to_string(this->dest_id));
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->dest_id] != dest){
        DEBUG("test_v5:TransferMoneySmartContract::get_old_dest = %ld, now dest = %ld\n", readSet[this->dest_id], dest);
        return 0;
    }

    if (amount <= source)
    {
        db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
        return 1;
    }
    DEBUG("test_v5:TransferMoneySmartContract::v_and_c err\n");
    return 0;
}


uint64_t DepositMoneySmartContract::v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->dest_id));
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->dest_id] != dest){
        DEBUG("test_v5:TransferMoneySmartContract::get_old_dest = %ld, now dest = %ld\n", readSet[this->dest_id], dest);
        return 0;
    }
    db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
    return 1;
}

/*
returns:
     1 for commit 
     0 for abort
*/
uint64_t WithdrawMoneySmartContract::v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    string temp = db->Get(std::to_string(this->source_id));
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->source_id] != source){
        DEBUG("test_v5:WithdrawMoneySmartContract::get_old_source = %ld, now source = %ld\n", readSet[this->source_id], source);
        return 0;
    }
    if (amount <= source)
    {
        db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        return 1;
    }
    DEBUG("test_v5:WithdrawMoneySmartContract::v_and_c err\n");
    return 0;
}
#else
uint64_t TransferMoneySmartContract::v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
    //DEBUG_V1("test_v6:enter TransferMoneySmartContract::v_and_merge\n");
    //string temp = db->Get(std::to_string(this->source_id));
    string temp;
    if (mergeSet.find(this->source_id) == mergeSet.end())
    {
        temp = db->Get(std::to_string(this->source_id));
    }
    else {
        temp = std::to_string(mergeSet[this->source_id]);
    }
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->source_id] != source){
        //DEBUG_V1("test_v5:Merge:TransferMoneySmartContract::get_old_source = %ld, now source = %ld\n", readSet[this->source_id], source);
        return 0;
    }
    //temp = db->Get(std::to_string(this->dest_id));
    if (mergeSet.find(this->dest_id) == mergeSet.end())
    {
        temp = db->Get(std::to_string(this->dest_id));
    }
    else {
        temp = std::to_string(mergeSet[this->dest_id]);
    }
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->dest_id] != dest){
        //DEBUG_V1("test_v5:Merge:TransferMoneySmartContract::get_old_dest = %ld, now dest = %ld\n", readSet[this->dest_id], dest);
        return 0;
    }

    if (amount <= source)
    {
        mergeSet[this->source_id] = source - amount;
        mergeSet[this->dest_id] = dest + amount;
        //DEBUG_V1("test_v6:enter TransferMoneySmartContract::return 1, source_key = %ld, amount = %ld, dest_key = %ld\n", this->source_id, amount, this->dest_id);
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
        return 1;
    }
    //DEBUG_V1("test_v5:Merge:TransferMoneySmartContract::v_and_c err\n");
    return 0;
}


uint64_t DepositMoneySmartContract::v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
    //DEBUG_V1("test_v6:enter DepositMoneySmartContract::v_and_merge\n");
    //string temp = db->Get(std::to_string(this->dest_id));
    string temp;
    if (mergeSet.find(this->dest_id) == mergeSet.end())
    {
        temp = db->Get(std::to_string(this->dest_id));
    }
    else {
        temp = std::to_string(mergeSet[this->dest_id]);
    }
    //uint64_t dest = temp.empty() ? 0 : stoi(temp);
    uint64_t dest = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->dest_id] != dest){
        //DEBUG_V1("test_v5:Merge:TransferMoneySmartContract::get_old_dest = %ld, now dest = %ld\n", readSet[this->dest_id], dest);
        return 0;
    }
    mergeSet[this->dest_id] = dest + amount;
    //db->Put(std::to_string(this->dest_id), std::to_string(dest + amount));
    return 1;
}

/*
returns:
     1 for commit 
     0 for abort
*/
uint64_t WithdrawMoneySmartContract::v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
    //DEBUG_V1("test_v6:enter WithdrawMoneySmartContract::v_and_merge\n");
    //string temp = db->Get(std::to_string(this->source_id));
    string temp;
    if (mergeSet.find(this->source_id) == mergeSet.end())
    {
        temp = db->Get(std::to_string(this->source_id));
    }
    else {
        temp = std::to_string(mergeSet[this->source_id]);
    }
    //uint64_t source = temp.empty() ? 0 : stoi(temp);
    uint64_t source = temp.empty() ? 10000 : stoi(temp);
    if(readSet[this->source_id] != source){
       // DEBUG_V1("test_v5:Merge:WithdrawMoneySmartContract::get_old_source = %ld, now source = %ld\n", readSet[this->source_id], source);
        return 0;
    }
#if SB_READ_TX
    return 1;
#else
    if (amount <= source)
    {
        mergeSet[this->source_id] = source - amount;
        //db->Put(std::to_string(this->source_id), std::to_string(source - amount));
        return 1;
    }
#endif
    //DEBUG_V1("test_v5:Merge:WithdrawMoneySmartContract::v_and_c err\n");
    return 0;
}
#endif
#endif

/*
Smartt Contract Transaction Manager and Workload
*/

void SmartContractTxn::init(uint64_t thd_id, Workload *h_wl)
{
    TxnManager::init(thd_id, h_wl);
    _wl = (SCWorkload *)h_wl;
    reset();
}

void SmartContractTxn::reset()
{
    TxnManager::reset();
}

RC SmartContractTxn::run_txn()
{
    this->smart_contract->execute();
    return RCOK;
};

#if ISEOV
#if PRE_EX
RC SmartContractTxn::simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    this->smart_contract->simulate(readSet, writeSet, speculateSet);
    return RCOK;
};
#else
RC SmartContractTxn::simulate_txn(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    this->smart_contract->simulate(readSet, writeSet);
    return RCOK;
};
#endif
#if !RE_EXECUTE
RC SmartContractTxn::validate_and_commit(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
#if CHECK_CONFILICT
    if(this->smart_contract->v_and_c(readSet, writeSet) == RCOK){
        return RCOK;
    }
    else return NONE;
#else
    this->smart_contract->v_and_c(readSet, writeSet);
    return RCOK;
#endif
};
#else
RC SmartContractTxn::validate_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
#if CHECK_CONFILICT
    if(this->smart_contract->v_and_merge(readSet, writeSet, mergeSet) == RCOK){
        return RCOK;
    }
    else return NONE;
#else
    this->smart_contract->v_and_c(readSet, writeSet);
    return RCOK;
#endif
};
#endif

#endif

RC SCWorkload::init()
{
    Workload::init();
    return RCOK;
}

RC SCWorkload::get_txn_man(TxnManager *&txn_manager)
{
    DEBUG_M("YCSBWorkload::get_txn_man YCSBTxnManager alloc\n");
    txn_manager = (SmartContractTxn *)
                      mem_allocator.align_alloc(sizeof(SmartContractTxn));
    new (txn_manager) SmartContractTxn();
    return RCOK;
}

uint64_t SmartContract::execute()
{
    int result = 0;
    switch (this->type)
    {
    case BSC_TRANSFER:
    {
        TransferMoneySmartContract *tm = (TransferMoneySmartContract *)this;
        result = tm->execute();
        break;
    }
    case BSC_DEPOSIT:
    {
        DepositMoneySmartContract *dm = (DepositMoneySmartContract *)this;
        result = dm->execute();
        break;
    }
    case BSC_WITHDRAW:
    {
        WithdrawMoneySmartContract *wm = (WithdrawMoneySmartContract *)this;
        result = wm->execute();
        break;
    }
    default:
        assert(0);
        break;
    }

    if (result)
        return RCOK;
    else
        return NONE;
}

#if ISEOV
#if PRE_EX
uint64_t SmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &speculateSet)
{
    int result = 0;
    switch (this->type)
    {
    case BSC_TRANSFER:
    {
        TransferMoneySmartContract *tm = (TransferMoneySmartContract *)this;
        result = tm->simulate(readSet, writeSet, speculateSet);
        break;
    }
    case BSC_DEPOSIT:
    {
        DepositMoneySmartContract *dm = (DepositMoneySmartContract *)this;
        result = dm->simulate(readSet, writeSet, speculateSet);
        break;
    }
    case BSC_WITHDRAW:
    {
        WithdrawMoneySmartContract *wm = (WithdrawMoneySmartContract *)this;
        result = wm->simulate(readSet, writeSet, speculateSet);
        break;
    }
    default:
        assert(0);
        break;
    }

    if (result)
        return RCOK;
    else
        return NONE;
}
#else
uint64_t SmartContract::simulate(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    int result = 0;
    switch (this->type)
    {
    case BSC_TRANSFER:
    {
        TransferMoneySmartContract *tm = (TransferMoneySmartContract *)this;
        result = tm->simulate(readSet, writeSet);
        break;
    }
    case BSC_DEPOSIT:
    {
        DepositMoneySmartContract *dm = (DepositMoneySmartContract *)this;
        result = dm->simulate(readSet, writeSet);
        break;
    }
    case BSC_WITHDRAW:
    {
        WithdrawMoneySmartContract *wm = (WithdrawMoneySmartContract *)this;
        result = wm->simulate(readSet, writeSet);
        break;
    }
    default:
        assert(0);
        break;
    }

    if (result)
        return RCOK;
    else
        return NONE;
}
#endif
#if !RE_EXECUTE
uint64_t SmartContract::v_and_c(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet)
{
    int result = 0;
    switch (this->type)
    {
    case BSC_TRANSFER:
    {
        TransferMoneySmartContract *tm = (TransferMoneySmartContract *)this;
        result = tm->v_and_c(readSet, writeSet);
        break;
    }
    case BSC_DEPOSIT:
    {
        DepositMoneySmartContract *dm = (DepositMoneySmartContract *)this;
        result = dm->v_and_c(readSet, writeSet);
        break;
    }
    case BSC_WITHDRAW:
    {
        WithdrawMoneySmartContract *wm = (WithdrawMoneySmartContract *)this;
        result = wm->v_and_c(readSet, writeSet);
        break;
    }
    default:
        assert(0);
        break;
    }

    if (result)
        return RCOK;
    else
        return NONE;
}
#else
uint64_t SmartContract::v_and_merge(map<uint64_t,uint64_t> &readSet, map<uint64_t,uint64_t> &writeSet, unordered_map<uint64_t,uint64_t> &mergeSet)
{
   // DEBUG_V1("test_v6:enter SmartContract::v_and_merge\n");
    int result = 0;
    switch (this->type)
    {
    case BSC_TRANSFER:
    {
        TransferMoneySmartContract *tm = (TransferMoneySmartContract *)this;
        result = tm->v_and_merge(readSet, writeSet, mergeSet);
        break;
    }
    case BSC_DEPOSIT:
    {
        DepositMoneySmartContract *dm = (DepositMoneySmartContract *)this;
        result = dm->v_and_merge(readSet, writeSet, mergeSet);
        break;
    }
    case BSC_WITHDRAW:
    {
        WithdrawMoneySmartContract *wm = (WithdrawMoneySmartContract *)this;
        result = wm->v_and_merge(readSet, writeSet, mergeSet);
        break;
    }
    default:
        assert(0);
        break;
    }

    if (result)
    {
        //DEBUG_V1("test_v6:enter SmartContract::v_and_merge::return RCOK\n");
        return RCOK;
    }
    else
        return NONE;
}
#endif
#endif

#endif