#ifndef _SC_H_
#define _SC_H_
#include "global.h"
#include "wl.h"

#if BANKING_SMART_CONTRACT

class SmartContract
{
public:
    uint64_t execute();
    BSCType type;
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
/*
Transfer money form source account to the destination account
Transaction will abort in case that the source account doesn't have enough money
*/
class TransferMoneySmartContract : public SmartContract
{
public:
    uint64_t source_id;
    uint64_t dest_id;
    uint64_t amount;
    uint64_t execute();
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

/*
Deposit $amount$ money to the destination account
Transaction will always commit 
*/
class DepositMoneySmartContract : public SmartContract
{
public:
    uint64_t dest_id;
    uint64_t amount;
    uint64_t execute();
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

/*
Withdraw $amount$ money form source account 
Transaction will abort in case that the source account doesn't have enough money
*/
class WithdrawMoneySmartContract : public SmartContract
{
public:
    uint64_t source_id;
    uint64_t amount;
    uint64_t execute();
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
#endif
