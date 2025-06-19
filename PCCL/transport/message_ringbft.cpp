/* Copyright (C) Exploratory Systems Laboratory - All Rights Reserved

Unauthorized copying, distribute, display, remix, derivative or deletion of any files or directories in this repository, via any medium is strictly prohibited. Proprietary and confidential
Written by Sajjad Rahnama, May 2019.
*/

#include "mem_alloc.h"
#include "query.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "global.h"
#include "message.h"
#include "crypto.h"
#include <fstream>
#include <ctime>
#include <string>
#include "helper.h"

#if RING_BFT

uint64_t CommitCertificateMessage::get_size()
{
    uint64_t size = Message::mget_size();
    size += sizeof(commit_view);
    size += sizeof(commit_index);
    size += sizeof(commit_hash_size);
    size += commit_hash.length();
    size += sizeof(hashSize);
    size += hash.length();
    size += sizeof(uint64_t) * signOwner.size();
    size += sizeof(uint64_t) * signSize.size();
    for (uint i = 0; i < g_batch_size; i++)
    {
        size += requestMsg[i]->get_size();
    }
    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {
        size += signatures[i].length();
    }
    size += sizeof(forwarding_from);
    size += sizeof(bool) * (g_shard_cnt);
    return size;
}

void CommitCertificateMessage::copy_from_txn(TxnManager *txn)
{

    // assert(is_primary_node(g_node_id)); // only primary creates this message

    // Setting txn_id 2 less than the actual value.

    this->txn_id = txn->get_txn_id() - 2;

    // Initialization
    this->requestMsg.resize(get_batch_size());
    this->signatures.resize(g_min_invalid_nodes * 2);
    this->signOwner.init(g_min_invalid_nodes * 2);
    this->signSize.init(g_min_invalid_nodes * 2);
    this->hash = txn->hash;
    this->hashSize = txn->hashSize;
    for (uint64_t txnid = txn->txn->txn_id - g_batch_size + 1; txnid < txn->txn->txn_id + 1; txnid++)
    {
        //sajjad
        TxnManager *t_man = txn_table.get_transaction_manager(0, txnid, 0);
        uint64_t idx = txnid % get_batch_size();

        YCSBQuery *yquery = ((YCSBQuery *)t_man->query);

        YCSBClientQueryMessage *yqry = (YCSBClientQueryMessage *)Message::create_message(CL_QRY);
        yqry->client_startts = t_man->client_startts;
        yqry->requests.init(g_req_per_query);
        yqry->return_node = t_man->client_id;
        for (uint64_t i = 0; i < g_req_per_query; i++)
        {
            ycsb_request *req = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request));
            req->key = yquery->requests[i]->key;
            req->value = yquery->requests[i]->value;
            yqry->requests.add(req);
        }
        this->requestMsg[idx] = yqry;
    }
    PBFTCommitMessage *cmsg;
    uint64_t i = 0;
    for (i = 0; i < txn->commit_msgs.size(); i++)
    {
        if (i >= (uint64_t)(g_min_invalid_nodes * 2))
            break;
        cmsg = txn->commit_msgs[i];
        // One time to fill ccm itself
        if (i == 0)
        {
            this->commit_hash = cmsg->hash;
            this->commit_view = cmsg->view;
            this->commit_index = cmsg->index;
            this->commit_hash_size = cmsg->hashSize;
        }
        for (uint64_t i = 0; i < g_shard_cnt; i++)
        {
            this->involved_shards[i] = txn->involved_shards[i];
        }
        this->signOwner.add(cmsg->return_node);
        this->signatures[i] = cmsg->signature;
        this->signSize.add(cmsg->signature.length());
    }
}

void CommitCertificateMessage::release()
{
    signOwner.release();
    signSize.release();
    for (size_t i = 0; i < requestMsg.size(); i++)
    {
        Message::release_message(requestMsg[i]);
    }
    vector<YCSBClientQueryMessage *>().swap(requestMsg);
    vector<string>().swap(signatures);
}

void CommitCertificateMessage::copy_to_txn(TxnManager *txn)
{
    Message::mcopy_to_txn(txn);
}

void CommitCertificateMessage::copy_from_buf(char *buf)
{
    Message::mcopy_from_buf(buf);

    uint64_t ptr = Message::mget_size();
    COPY_VAL(commit_view, buf, ptr);
    COPY_VAL(commit_index, buf, ptr);
    COPY_VAL(commit_hash_size, buf, ptr);
    ptr = buf_to_string(buf, ptr, commit_hash, commit_hash_size);

    uint64_t elem;
    // Initialization
    requestMsg.resize(g_batch_size);
    signatures.resize(g_min_invalid_nodes * 2);
    signSize.init(g_min_invalid_nodes * 2);
    signOwner.init(g_min_invalid_nodes * 2);
    COPY_VAL(elem, buf, ptr);
    this->hashSize = elem;
    string thash;
    ptr = buf_to_string(buf, ptr, thash, this->hashSize);
    this->hash = thash;
    for (uint i = 0; i < g_batch_size; i++)
    {
        Message *msg = create_message(&buf[ptr]);
        ptr += msg->get_size();
        requestMsg[i] = ((YCSBClientQueryMessage *)msg);
    }
    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {
        COPY_VAL(elem, buf, ptr);
        signOwner.add(elem);

        COPY_VAL(elem, buf, ptr);
        signSize.add(elem);

        string tsign;
        ptr = buf_to_string(buf, ptr, tsign, signSize[i]);
        signatures[i] = tsign;
    }
    COPY_VAL(forwarding_from, buf, ptr);
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        COPY_VAL(involved_shards[i], buf, ptr);
    }
    assert(ptr == get_size());
}

void CommitCertificateMessage::copy_to_buf(char *buf)
{
    Message::mcopy_to_buf(buf);
    uint64_t ptr = Message::mget_size();
    COPY_BUF(buf, commit_view, ptr);
    COPY_BUF(buf, commit_index, ptr);
    COPY_BUF(buf, commit_hash_size, ptr);
    char v;
    for (uint64_t i = 0; i < commit_hash_size; i++)
    {
        v = commit_hash[i];
        COPY_BUF(buf, v, ptr);
    }
    // return;
    uint64_t elem;
    elem = this->hashSize;
    COPY_BUF(buf, elem, ptr);

    string hstr = this->hash;
    for (uint j = 0; j < hstr.size(); j++)
    {
        v = hstr[j];
        COPY_BUF(buf, v, ptr);
    }

    for (uint i = 0; i < g_batch_size; i++)
    {
        //copy client request stored in message to buf
        requestMsg[i]->copy_to_buf(&buf[ptr]);
        ptr += requestMsg[i]->get_size();
    }
    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {

        elem = signOwner[i];
        COPY_BUF(buf, elem, ptr);
        elem = signSize[i];
        COPY_BUF(buf, elem, ptr);
        char v;
        string sstr = signatures[i];
        for (uint j = 0; j < sstr.size(); j++)
        {
            v = sstr[j];
            COPY_BUF(buf, v, ptr);
        }
    }
    COPY_BUF(buf, forwarding_from, ptr);
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        COPY_BUF(buf, involved_shards[i], ptr);
    }
    assert(ptr == get_size());
}

void CommitCertificateMessage::sign(uint64_t dest_node_id)
{
#if USE_CRYPTO
    string message = toString();
    signingClientNode(message, this->signature, this->pubKey, dest_node_id);
#else
    this->signature = "0";
#endif
    this->sigSize = this->signature.size();
    this->keySize = this->pubKey.size();
}

string CommitCertificateMessage::toString()
{
    string message = std::to_string(this->commit_view);
    message += this->commit_hash;
    message += this->commit_index;
    message += this->commit_view;

    return message;
}

//makes sure message is valid, returns true for false
bool CommitCertificateMessage::validate()
{
#if USE_CRYPTO
    string message = this->toString();
    uint64_t return_node = this->forwarding_from != (uint64_t)-1 ? this->forwarding_from : this->return_node_id;
    // cout << return_node << "    " << hexStr(message.c_str(), message.length()) << endl;
    if (!validateClientNode(message, g_pub_keys[return_node], this->signature, return_node))
    {
        assert(0);
        return false;
    }
    for (uint64_t i = 0; i < this->signatures.size(); i++)
    {

        string signString = std::to_string(this->commit_view);
        signString += '_' + std::to_string(this->commit_index);
        signString += '_' + this->commit_hash;
        signString += '_' + std::to_string(this->signOwner[i]);
        signString += '_' + to_string(this->commit_index + get_batch_size() - 1);
        // cout << this->signOwner[i] << endl;
        assert(validateClientNode(signString, g_pub_keys[this->signOwner[i]], this->signatures[i], this->signOwner[i]));
    }

#endif
    string batchStr = "";
    for (uint64_t i = 0; i < get_batch_size(); i++)
    {
        batchStr += this->requestMsg[i]->getString();
    }
    if (this->hash != calculateHash(batchStr))
    {
        assert(0);
        return false;
    }
    return true;
}

//----------------

uint64_t RingBFTPrePrepare::get_size()
{
    uint64_t size = Message::mget_size();
    size += sizeof(view);
    size += sizeof(uint64_t);
    size += hash.length();
    size += sizeof(batch_size);
    size += sizeof(bool) * (g_shard_cnt);

    return size;
}

// Initialization
void RingBFTPrePrepare::init(uint64_t thd_id)
{
    assert(view_to_primary(get_current_view(thd_id)) == g_node_id);
    this->view = get_current_view(thd_id);
}

void RingBFTPrePrepare::release()
{
}

void RingBFTPrePrepare::copy_from_buf(char *buf)
{
    Message::mcopy_from_buf(buf);

    uint64_t ptr = Message::mget_size();
    COPY_VAL(view, buf, ptr);
    COPY_VAL(hashSize, buf, ptr);
    ptr = buf_to_string(buf, ptr, hash, hashSize);
    COPY_VAL(batch_size, buf, ptr);
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        COPY_VAL(involved_shards[i], buf, ptr);
    }
    assert(ptr == get_size());
}

void RingBFTPrePrepare::copy_to_buf(char *buf)
{
    Message::mcopy_to_buf(buf);

    uint64_t ptr = Message::mget_size();
    COPY_BUF(buf, view, ptr);

    COPY_BUF(buf, hashSize, ptr);
    char v;
    for (uint j = 0; j < hash.size(); j++)
    {
        v = hash[j];
        COPY_BUF(buf, v, ptr);
    }

    COPY_BUF(buf, batch_size, ptr);
    for (uint64_t i = 0; i < g_shard_cnt; i++)
    {
        COPY_BUF(buf, involved_shards[i], ptr);
    }
    assert(ptr == get_size());
}

string RingBFTPrePrepare::getString(uint64_t sender)
{
    string message = std::to_string(sender);
    message += hash;
    message += view;

    return message;
}

void RingBFTPrePrepare::sign(uint64_t dest_node)
{
#if USE_CRYPTO
    string message = getString(g_node_id);

    signingNodeNode(message, this->signature, this->pubKey, dest_node);
#else
    this->signature = "0";
#endif
    this->sigSize = this->signature.size();
    this->keySize = this->pubKey.size();
}

//makes sure message is valid, returns true for false
bool RingBFTPrePrepare::validate(uint64_t thd_id)
{

#if USE_CRYPTO
    string message = getString(this->return_node_id);

    if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
    {
        assert(0);
        return false;
    }

#endif

    if (this->view != get_current_view(thd_id))
    {
        assert(0);
        return false;
    }

    return true;
}

//----------------

uint64_t RingBFTCommit::get_size()
{
    uint64_t size = Message::mget_size();
    size += sizeof(commit_view);
    size += sizeof(commit_index);
    size += sizeof(commit_hash_size);
    size += commit_hash.length();
    size += sizeof(hashSize);
    size += hash.length();
    size += sizeof(uint64_t) * signOwner.size();
    size += sizeof(uint64_t) * signSize.size();
    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {
        size += signatures[i].length();
    }
    size += sizeof(forwarding_from);
    return size;
}

void RingBFTCommit::copy_from_txn(TxnManager *txn)
{

    // assert(is_primary_node(g_node_id)); // only primary creates this message

    // Setting txn_id 2 less than the actual value.

    this->txn_id = txn->get_txn_id() - 2;

    // Initialization
    this->signatures.resize(g_min_invalid_nodes * 2);
    this->signOwner.init(g_min_invalid_nodes * 2);
    this->signSize.init(g_min_invalid_nodes * 2);
    this->hash = txn->hash;
    this->hashSize = txn->hashSize;
    PBFTCommitMessage *cmsg;
    uint64_t i = 0;
    for (i = 0; i < txn->commit_msgs.size(); i++)
    {
        if (i >= (uint64_t)(g_min_invalid_nodes * 2))
            break;
        cmsg = txn->commit_msgs[i];
        // One time to fill ccm itself
        if (i == 0)
        {
            this->commit_hash = cmsg->hash;
            this->commit_view = cmsg->view;
            this->commit_index = cmsg->index;
            this->commit_hash_size = cmsg->hashSize;
        }
        this->signOwner.add(cmsg->return_node);
        this->signatures[i] = cmsg->signature;
        this->signSize.add(cmsg->signature.length());
    }
}

void RingBFTCommit::release()
{
    signOwner.release();
    signSize.release();
    vector<string>().swap(signatures);
}

void RingBFTCommit::copy_to_txn(TxnManager *txn)
{
    Message::mcopy_to_txn(txn);
}

void RingBFTCommit::copy_from_buf(char *buf)
{
    Message::mcopy_from_buf(buf);

    uint64_t ptr = Message::mget_size();
    COPY_VAL(commit_view, buf, ptr);
    COPY_VAL(commit_index, buf, ptr);
    COPY_VAL(commit_hash_size, buf, ptr);
    ptr = buf_to_string(buf, ptr, commit_hash, commit_hash_size);

    uint64_t elem;
    // Initialization
    signatures.resize(g_min_invalid_nodes * 2);
    signSize.init(g_min_invalid_nodes * 2);
    signOwner.init(g_min_invalid_nodes * 2);
    COPY_VAL(elem, buf, ptr);
    this->hashSize = elem;
    string thash;
    ptr = buf_to_string(buf, ptr, thash, this->hashSize);
    this->hash = thash;
    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {
        COPY_VAL(elem, buf, ptr);
        signOwner.add(elem);

        COPY_VAL(elem, buf, ptr);
        signSize.add(elem);

        string tsign;
        ptr = buf_to_string(buf, ptr, tsign, signSize[i]);
        signatures[i] = tsign;
    }
    COPY_VAL(forwarding_from, buf, ptr);
    assert(ptr == get_size());
}

void RingBFTCommit::copy_to_buf(char *buf)
{
    Message::mcopy_to_buf(buf);
    uint64_t ptr = Message::mget_size();
    COPY_BUF(buf, commit_view, ptr);
    COPY_BUF(buf, commit_index, ptr);
    COPY_BUF(buf, commit_hash_size, ptr);
    char v;
    for (uint64_t i = 0; i < commit_hash_size; i++)
    {
        v = commit_hash[i];
        COPY_BUF(buf, v, ptr);
    }
    // return;
    uint64_t elem;
    elem = this->hashSize;
    COPY_BUF(buf, elem, ptr);

    string hstr = this->hash;
    for (uint j = 0; j < hstr.size(); j++)
    {
        v = hstr[j];
        COPY_BUF(buf, v, ptr);
    }

    for (uint i = 0; i < (g_min_invalid_nodes * 2); i++)
    {

        elem = signOwner[i];
        COPY_BUF(buf, elem, ptr);
        elem = signSize[i];
        COPY_BUF(buf, elem, ptr);
        char v;
        string sstr = signatures[i];
        for (uint j = 0; j < sstr.size(); j++)
        {
            v = sstr[j];
            COPY_BUF(buf, v, ptr);
        }
    }
    COPY_BUF(buf, forwarding_from, ptr);
    assert(ptr == get_size());
}

void RingBFTCommit::sign(uint64_t dest_node_id)
{
#if USE_CRYPTO
    string message = toString();
    signingClientNode(message, this->signature, this->pubKey, dest_node_id);
#else
    this->signature = "0";
#endif
    this->sigSize = this->signature.size();
    this->keySize = this->pubKey.size();
}

string RingBFTCommit::toString()
{
    string message = std::to_string(this->commit_view);
    message += this->commit_hash;
    message += this->commit_index;
    message += this->commit_view;

    return message;
}

//makes sure message is valid, returns true for false
bool RingBFTCommit::validate()
{
#if USE_CRYPTO
    string message = this->toString();
    uint64_t return_node = this->forwarding_from != (uint64_t)-1 ? this->forwarding_from : this->return_node_id;
    // cout << return_node << "    " << hexStr(message.c_str(), message.length()) << endl;
    if (!validateClientNode(message, g_pub_keys[return_node], this->signature, return_node))
    {
        assert(0);
        return false;
    }
    for (uint64_t i = 0; i < this->signatures.size(); i++)
    {

        string signString = std::to_string(this->commit_view);
        signString += '_' + std::to_string(this->commit_index);
        signString += '_' + this->commit_hash;
        signString += '_' + std::to_string(this->signOwner[i]);
        signString += '_' + to_string(this->commit_index + get_batch_size() - 1);
        // cout << this->signOwner[i] << endl;
        assert(validateClientNode(signString, g_pub_keys[this->signOwner[i]], this->signatures[i], this->signOwner[i]));
    }

#endif
    return true;
}

#endif