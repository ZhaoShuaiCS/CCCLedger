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
#include <map>


std::vector<Message *> *Message::create_messages(char *buf)
{
	std::vector<Message *> *all_msgs = new std::vector<Message *>;
	char *data = buf;
	uint64_t ptr = 0;
	uint32_t dest_id;
	uint32_t return_id;
	uint32_t txn_cnt;
	COPY_VAL(dest_id, data, ptr);
	COPY_VAL(return_id, data, ptr);
	COPY_VAL(txn_cnt, data, ptr);
	assert(dest_id == g_node_id);
	assert(return_id != g_node_id);
	assert(ISCLIENTN(return_id) || ISSERVERN(return_id) || ISREPLICAN(return_id));
	while (txn_cnt > 0)
	{
		Message *msg = create_message(&data[ptr]);
		msg->return_node_id = return_id;
		ptr += msg->get_size();
		all_msgs->push_back(msg);
		--txn_cnt;
	}
	return all_msgs;
}

Message *Message::create_message(char *buf)
{
	RemReqType rtype = NO_MSG;
	uint64_t ptr = 0;
	COPY_VAL(rtype, buf, ptr);
	Message *msg = create_message(rtype);
	msg->copy_from_buf(buf);
	return msg;
}

Message *Message::create_message(TxnManager *txn, RemReqType rtype)
{
	Message *msg = create_message(rtype);
	msg->mcopy_from_txn(txn);
	msg->copy_from_txn(txn);
	//cout << "test_v3:create_message(TxnManager *txn, RemReqType rtype) rtype= " << rtype << " batch_id = "<< msg->batch_id << "\n";

	// copy latency here
	msg->lat_work_queue_time = txn->txn_stats.work_queue_time_short;
	msg->lat_msg_queue_time = txn->txn_stats.msg_queue_time_short;
	msg->lat_cc_block_time = txn->txn_stats.cc_block_time_short;
	msg->lat_cc_time = txn->txn_stats.cc_time_short;
	msg->lat_process_time = txn->txn_stats.process_time_short;
	msg->lat_network_time = txn->txn_stats.lat_network_time_start;
	msg->lat_other_time = txn->txn_stats.lat_other_time_start;

	return msg;
}

#if !BANKING_SMART_CONTRACT
Message *Message::create_message(BaseQuery *query, RemReqType rtype)
{
	assert(rtype == CL_QRY);
	Message *msg = create_message(rtype);
	((YCSBClientQueryMessage *)msg)->copy_from_query(query);
	return msg;
}
#endif

Message *Message::create_message(uint64_t txn_id, RemReqType rtype)
{
	Message *msg = create_message(rtype);
	msg->txn_id = txn_id;
	return msg;
}

Message *Message::create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype)
{
	Message *msg = create_message(rtype);
	msg->txn_id = txn_id;
	msg->batch_id = batch_id;
	return msg;
}

Message *Message::create_message(RemReqType rtype)
{
	Message *msg;
	switch (rtype)
	{
	case INIT_DONE:
		msg = new InitDoneMessage;
		break;
	case KEYEX:
		msg = new KeyExchange;
		break;
	case READY:
		msg = new ReadyServer;
		break;
#if SHARPER
	case SUPER_PROPOSE:
		msg = new SuperPropose;
		break;
#endif
#if BANKING_SMART_CONTRACT
	case BSC_MSG:
		msg = new BankingSmartContractMessage;
		break;
#else
	case CL_QRY:
	case RTXN:
	case RTXN_CONT:
		msg = new YCSBClientQueryMessage;
		msg->init();
		break;
#endif
	case CL_BATCH:
		msg = new ClientQueryBatch;
		break;
	case RDONE:
		msg = new DoneMessage;
		break;
	case CL_RSP:
		msg = new ClientResponseMessage;
		break;
	case EXECUTE_MSG:
		msg = new ExecuteMessage;
		break;
	case BATCH_REQ:
		msg = new BatchRequests;
		break;
	case BROADCAST_BATCH:
		msg = new BroadcastBatchMessage;
		break;

#if VIEW_CHANGES == true
	case VIEW_CHANGE:
		msg = new ViewChangeMsg;
		break;
	case NEW_VIEW:
		msg = new NewViewMsg;
		break;
#endif

	case PBFT_CHKPT_MSG:
		msg = new CheckpointMessage;
		break;
	case PBFT_PREP_MSG:
		msg = new PBFTPrepMessage;
		break;
	case PBFT_COMMIT_MSG:
		msg = new PBFTCommitMessage;
		break;
	case BROADCAST_BATCH_MSG:
		msg = new BroadcastBatchMessage;
		break;

#if RING_BFT
	case COMMIT_CERT_MSG:
		msg = new CommitCertificateMessage;
		break;
	case RING_PRE_PREPARE:
		msg = new RingBFTPrePrepare;
		break;
	case RING_COMMIT:
		msg = new RingBFTCommit;
		break;
#endif

	default:
		cout << "FALSE TYPE: " << rtype << "\n";
		fflush(stdout);
		assert(false);
	}
	assert(msg);
	msg->rtype = rtype;
	msg->txn_id = UINT64_MAX;
	msg->batch_id = UINT64_MAX;
	msg->return_node_id = g_node_id;
	msg->wq_time = 0;
	msg->mq_time = 0;
	msg->ntwk_time = 0;

	msg->lat_work_queue_time = 0;
	msg->lat_msg_queue_time = 0;
	msg->lat_cc_block_time = 0;
	msg->lat_cc_time = 0;
	msg->lat_process_time = 0;
	msg->lat_network_time = 0;
	msg->lat_other_time = 0;

	return msg;
}

uint64_t Message::mget_size()
{
	uint64_t size = 0;
	size += sizeof(RemReqType);
	size += sizeof(uint64_t);
	// for batch_id
	size += sizeof(uint64_t);

	// for stats, send message queue time
	size += sizeof(uint64_t);
	size += signature.size();
	size += pubKey.size();
	size += sizeof(sigSize);
	size += sizeof(keySize);

	// for stats, latency
	size += sizeof(uint64_t) * 7;

#if SHARPER
	size += sizeof(is_cross_shard);
#endif
	return size;
}

void Message::mcopy_from_txn(TxnManager *txn)
{
	txn_id = txn->get_txn_id();
	batch_id = txn->get_batch_id();
}

void Message::mcopy_to_txn(TxnManager *txn)
{
	txn->return_id = return_node_id;
}

void Message::mcopy_from_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_VAL(rtype, buf, ptr);
	COPY_VAL(txn_id, buf, ptr);
	COPY_VAL(mq_time, buf, ptr);
	COPY_VAL(batch_id, buf, ptr);

	COPY_VAL(lat_work_queue_time, buf, ptr);
	COPY_VAL(lat_msg_queue_time, buf, ptr);
	COPY_VAL(lat_cc_block_time, buf, ptr);
	COPY_VAL(lat_cc_time, buf, ptr);
	COPY_VAL(lat_process_time, buf, ptr);
	COPY_VAL(lat_network_time, buf, ptr);
	COPY_VAL(lat_other_time, buf, ptr);

	if (IS_LOCAL(txn_id))
	{
		lat_network_time = (get_sys_clock() - lat_network_time) - lat_other_time;
	}
	else
	{
		lat_other_time = get_sys_clock();
	}
	//printf("buftot %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);

	COPY_VAL(sigSize, buf, ptr);
	COPY_VAL(keySize, buf, ptr);
	signature.pop_back();
	pubKey.pop_back();

	char v;
	for (uint64_t i = 0; i < sigSize; i++)
	{
		COPY_VAL(v, buf, ptr);
		signature += v;
	}
	for (uint64_t j = 0; j < keySize; j++)
	{
		COPY_VAL(v, buf, ptr);
		pubKey += v;
	}
#if SHARPER
	COPY_VAL(is_cross_shard, buf, ptr);
#endif
}

void Message::mcopy_to_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_BUF(buf, rtype, ptr);
	COPY_BUF(buf, txn_id, ptr);
	COPY_BUF(buf, mq_time, ptr);
	COPY_BUF(buf, batch_id, ptr);

	COPY_BUF(buf, lat_work_queue_time, ptr);
	COPY_BUF(buf, lat_msg_queue_time, ptr);
	COPY_BUF(buf, lat_cc_block_time, ptr);
	COPY_BUF(buf, lat_cc_time, ptr);
	COPY_BUF(buf, lat_process_time, ptr);
	lat_network_time = get_sys_clock();

	//printf("mtobuf %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
	COPY_BUF(buf, lat_network_time, ptr);
	COPY_BUF(buf, lat_other_time, ptr);

	COPY_BUF(buf, sigSize, ptr);
	COPY_BUF(buf, keySize, ptr);

	char v;
	for (uint64_t i = 0; i < sigSize; i++)
	{
		v = signature[i];
		COPY_BUF(buf, v, ptr);
	}
	for (uint64_t j = 0; j < keySize; j++)
	{
		v = pubKey[j];
		COPY_BUF(buf, v, ptr);
	}
#if SHARPER
	COPY_BUF(buf, is_cross_shard, ptr);
#endif
}

void Message::release_message(Message *msg)
{
	switch (msg->rtype)
	{
	case INIT_DONE:
	{
		InitDoneMessage *m_msg = (InitDoneMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case KEYEX:
	{
		KeyExchange *m_msg = (KeyExchange *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case READY:
	{
		ReadyServer *m_msg = (ReadyServer *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
#if SHARPER
	case SUPER_PROPOSE:
	{
		SuperPropose *m_msg = (SuperPropose *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
#endif
#if BANKING_SMART_CONTRACT
	case BSC_MSG:
	{
		BankingSmartContractMessage *m_msg = (BankingSmartContractMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
#else
	case CL_QRY:
	{
		YCSBClientQueryMessage *m_msg = (YCSBClientQueryMessage *)msg;
		//DEBUG("Release CLQRY! requests.size: %ld requests_writeset.size: %ld\n",m_msg->requests.size(), m_msg->requests_writeset.size());
		m_msg->release();
		delete m_msg;
		break;
	}
#endif
	case CL_BATCH:
	{
		ClientQueryBatch *m_msg = (ClientQueryBatch *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case RDONE:
	{
		DoneMessage *m_msg = (DoneMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case CL_RSP:
	{
		ClientResponseMessage *m_msg = (ClientResponseMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case EXECUTE_MSG:
	{
		ExecuteMessage *m_msg = (ExecuteMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case BATCH_REQ:
	{
		BatchRequests *m_msg = (BatchRequests *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}

#if VIEW_CHANGES == true
	case VIEW_CHANGE:
	{
		ViewChangeMsg *m_msg = (ViewChangeMsg *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case NEW_VIEW:
	{
		NewViewMsg *m_msg = (NewViewMsg *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
#endif

	case PBFT_CHKPT_MSG:
	{
		CheckpointMessage *m_msg = (CheckpointMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case PBFT_PREP_MSG:
	{
		PBFTPrepMessage *m_msg = (PBFTPrepMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case PBFT_COMMIT_MSG:
	{
		PBFTCommitMessage *m_msg = (PBFTCommitMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case BROADCAST_BATCH:
	{
		BroadcastBatchMessage *m_msg = (BroadcastBatchMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}

#if RING_BFT
	case COMMIT_CERT_MSG:
	{
		CommitCertificateMessage *m_msg = (CommitCertificateMessage *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case RING_PRE_PREPARE:
	{
		RingBFTPrePrepare *m_msg = (RingBFTPrePrepare *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
	case RING_COMMIT:
	{
		RingBFTCommit *m_msg = (RingBFTCommit *)msg;
		m_msg->release();
		delete m_msg;
		break;
	}
#endif

	default:
	{
		assert(false);
	}
	}
	msg->dest.clear();
}
/************************/

uint64_t QueryMessage::get_size()
{
	uint64_t size = Message::mget_size();
	return size;
}

void QueryMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
}

void QueryMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void QueryMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr __attribute__((unused));
	ptr = Message::mget_size();
}

void QueryMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr __attribute__((unused));
	ptr = Message::mget_size();
}

/************************/
#if BANKING_SMART_CONTRACT
void BankingSmartContractMessage::init()
{
}

BankingSmartContractMessage::BankingSmartContractMessage() {}

BankingSmartContractMessage::~BankingSmartContractMessage()
{
	release();
}

void BankingSmartContractMessage::release()
{
	ClientQueryMessage::release();
	inputs.release();
}

uint64_t BankingSmartContractMessage::get_size()
{
	uint64_t size = 0;
	size += sizeof(RemReqType);
	size += sizeof(return_node_id);
	size += sizeof(client_startts);
	size += sizeof(size_t);
	size += sizeof(uint64_t) * inputs.size();
	size += sizeof(BSCType);
	// #if ISEOV
	// size += 2 * sizeof(uint64_t) * readSet.size();
	// size += 2 * sizeof(uint64_t) * writeSet.size();
	// #endif

	return size;
}

void BankingSmartContractMessage::copy_from_query(BaseQuery *query) {}

void BankingSmartContractMessage::copy_from_txn(TxnManager *txn) {}

void BankingSmartContractMessage::copy_to_txn(TxnManager *txn) {}

void BankingSmartContractMessage::copy_from_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_VAL(rtype, buf, ptr);
	COPY_VAL(return_node_id, buf, ptr);
	COPY_VAL(client_startts, buf, ptr);
	size_t size;
	COPY_VAL(size, buf, ptr);
	inputs.init(size);
	for (uint64_t i = 0; i < size; i++)
	{
		uint64_t input;
		COPY_VAL(input, buf, ptr);
		inputs.add(input);
	}

	COPY_VAL(type, buf, ptr);

	// #if ISEOV
	// uint64_t readSet_size = 1;
	// uint64_t key = 0;
	// uint64_t value = 0;

	// if (type == BSC_TRANSFER) readSet_size = 2;
	// else readSet_size = 1;

	// for(uint64_t i = 0; i < readSet_size; i++){
	// 	COPY_VAL(key, buf, ptr);
	// 	COPY_VAL(value, buf, ptr);
	// 	readSet[key] = value;
	// }

	// for(uint64_t i = 0; i < readSet_size; i++){
	// 	COPY_VAL(key, buf, ptr);
	// 	COPY_VAL(value, buf, ptr);
	// 	writeSet[key] = value;
	// }

	// #endif

	assert(ptr == get_size());
}

void BankingSmartContractMessage::copy_to_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_BUF(buf, rtype, ptr);
	COPY_BUF(buf, return_node_id, ptr);
	COPY_BUF(buf, client_startts, ptr);
	size_t size = inputs.size();
	COPY_BUF(buf, size, ptr);
	for (uint64_t i = 0; i < inputs.size(); i++)
	{
		uint64_t input = inputs[i];
		COPY_BUF(buf, input, ptr);
	}

	COPY_BUF(buf, type, ptr);

	// #if ISEOV
	// // uint64_t readSet_size = 0;
	// // if (type == BSC_TRANSFER) readSet_size = 2;
	// // else readSet_size = 1;

	// for(auto item: readSet){
	// 	COPY_BUF(buf, item.first, ptr);
	// 	COPY_BUF(buf, item.second, ptr);
	// }

	// for(auto item: writeSet){
	// 	COPY_BUF(buf, item.first, ptr);
	// 	COPY_BUF(buf, item.second, ptr);
	// }

	// #endif

	assert(ptr == get_size());
}

//returns a string representation of the requests in this message
string BankingSmartContractMessage::getRequestString()
{
	string message;
	for (uint64_t i = 0; i < inputs.size(); i++)
	{
		message += std::to_string(inputs[i]);
		message += " ";
	}

	return message;
}

//returns the string that needs to be signed/verified for this message
string BankingSmartContractMessage::getString()
{
	string message = this->getRequestString();
	message += " ";
	message += to_string(this->client_startts);

	return message;
}
#else
void YCSBClientQueryMessage::init()
{
}

YCSBClientQueryMessage::YCSBClientQueryMessage() {}

YCSBClientQueryMessage::~YCSBClientQueryMessage()
{
	release();
}

void YCSBClientQueryMessage::release()
{
	ClientQueryMessage::release();
	// Freeing requests is the responsibility of txn at commit time
	if (!ISCLIENT)
	{
		//cout << "RELEASE! requests.size:"<< requests.size() << "requests_writeset.size:" << requests_writeset.size()<<"\n";
		for (uint64_t i = 0; i < requests.size(); i++)
		{
			DEBUG_M("YCSBClientQueryMessage::release ycsb_request free\n");
			mem_allocator.free(requests[i], sizeof(ycsb_request));
#if PRE_ORDER
			mem_allocator.free(requests_writeset[i], sizeof(ycsb_request_writeset));
#endif
		}
		
	}
	requests.release();
}

uint64_t YCSBClientQueryMessage::get_size()
{
	uint64_t size = 0;
	size += sizeof(RemReqType);
	size += sizeof(client_startts);
	size += sizeof(size_t);
	size += sizeof(ycsb_request) * requests.size();
#if PRE_ORDER
	size += sizeof(ycsb_request_writeset) * requests_writeset.size();
#endif
	size += sizeof(return_node);

	return size;
}

void YCSBClientQueryMessage::copy_from_query(BaseQuery *query)
{
	ClientQueryMessage::copy_from_query(query);
	requests.copy(((YCSBQuery *)(query))->requests);
#if PRE_ORDER
	requests_writeset.init(requests.size());
	for (uint64_t i = 0; i < requests.size(); i++){
		ycsb_request_writeset *one_request_writeset = (ycsb_request_writeset *)mem_allocator.alloc(sizeof(ycsb_request_writeset));
		new (one_request_writeset) ycsb_request_writeset();
		one_request_writeset->key = requests[i]->key;
		one_request_writeset->value = requests[i]->value;

		requests_writeset.add(one_request_writeset);
		//requests_writeset[i]->copy(requests[i]);
		//DEBUG("copy_from_query! requests.size: %ld requests_writeset.size: %ld\n",requests.size(), requests_writeset.size());
	}
#endif
	
}

void YCSBClientQueryMessage::copy_from_txn(TxnManager *txn)
{
	ClientQueryMessage::mcopy_from_txn(txn);
	requests.copy(((YCSBQuery *)(txn->query))->requests);
#if PRE_ORDER
	requests_writeset.init(requests.size());
	for (uint64_t i = 0; i < requests.size(); i++){
		ycsb_request_writeset *one_request_writeset = (ycsb_request_writeset *)mem_allocator.alloc(sizeof(ycsb_request_writeset));
		new (one_request_writeset) ycsb_request_writeset();
		one_request_writeset->key = requests[i]->key;
		one_request_writeset->value = requests[i]->value;

		requests_writeset.add(one_request_writeset);
		//requests_writeset[i]->copy(requests[i]);
		//DEBUG("copy_from_query! requests.size: %ld requests_writeset.size: %ld\n",requests.size(), requests_writeset.size());
	}
#endif
}

void YCSBClientQueryMessage::copy_to_txn(TxnManager *txn)
{
	// this only copies over the pointers, so if requests are freed, we'll lose the request data
	ClientQueryMessage::copy_to_txn(txn);

	txn->client_id = return_node;
	// Copies pointers to txn
	((YCSBQuery *)(txn->query))->requests.append(requests);
#if PRE_ORDER
	((YCSBQuery *)(txn->query))->requests_writeset.append(requests_writeset);
#endif
}

void YCSBClientQueryMessage::copy_from_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_VAL(rtype, buf, ptr);
	COPY_VAL(client_startts, buf, ptr);
	size_t size;
	COPY_VAL(size, buf, ptr);
	requests.init(size);
	for (uint64_t i = 0; i < size; i++)
	{
		DEBUG_M("YCSBClientQueryMessage::copy ycsb_request alloc\n");
		ycsb_request *req = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request));
		COPY_VAL(*req, buf, ptr);
		assert(req->key < g_synth_table_size);
		requests.add(req);
	}
#if PRE_ORDER
	requests_writeset.init(size);
	for (uint64_t i = 0; i < size; i++)
	{
		DEBUG_M("YCSBClientQueryMessage::copy ycsb_request_write alloc\n");
		ycsb_request_writeset *req_wset = (ycsb_request_writeset *)mem_allocator.alloc(sizeof(ycsb_request_writeset));
		COPY_VAL(*req_wset, buf, ptr);
		assert(req_wset->key < g_synth_table_size);
		requests_writeset.add(req_wset);
	}
#endif

	COPY_VAL(return_node, buf, ptr);

	assert(ptr == get_size());
}

void YCSBClientQueryMessage::copy_to_buf(char *buf)
{
	uint64_t ptr = 0;
	COPY_BUF(buf, rtype, ptr);
	COPY_BUF(buf, client_startts, ptr);
	size_t size = requests.size();
	COPY_BUF(buf, size, ptr);
	for (uint64_t i = 0; i < requests.size(); i++)
	{
		ycsb_request *req = requests[i];
		assert(req->key < g_synth_table_size);
		COPY_BUF(buf, *req, ptr);
	}
#if PRE_ORDER
	for (uint64_t i = 0; i < requests_writeset.size(); i++)
	{
		ycsb_request_writeset *req_wset = requests_writeset[i];
		assert(req_wset->key < g_synth_table_size);
		COPY_BUF(buf, *req_wset, ptr);
	}
#endif

	COPY_BUF(buf, return_node, ptr);

	assert(ptr == get_size());
}

//returns a string representation of the requests in this message
string YCSBClientQueryMessage::getRequestString()
{
	string message;
	for (uint64_t i = 0; i < requests.size(); i++)
	{
		message += std::to_string(requests[i]->key);
		message += " ";
		message += requests[i]->value;
		message += " ";
	}
#if PRE_ORDER
	for (uint64_t i = 0; i < requests_writeset.size(); i++)
	{
		message += std::to_string(requests_writeset[i]->key);
		message += " ";
		message += requests_writeset[i]->value;
		message += " ";
	}
#endif
	return message;
}

//returns the string that needs to be signed/verified for this message
string YCSBClientQueryMessage::getString()
{
#if PRE_ORDER
	string message = this->getRequestString();
	message += " ";
#else
	string message = " ";
#endif

	message += to_string(this->client_startts);

	return message;
}

/************************/

void YCSBQueryMessage::init()
{
}

void YCSBQueryMessage::release()
{
	QueryMessage::release();
	// Freeing requests is the responsibility of txn
	/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBQueryMessage::release ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
*/
	requests.release();
}

uint64_t YCSBQueryMessage::get_size()
{
	uint64_t size = QueryMessage::get_size();
	size += sizeof(size_t);
	size += sizeof(ycsb_request) * requests.size();
	return size;
}

void YCSBQueryMessage::copy_from_txn(TxnManager *txn)
{
	QueryMessage::copy_from_txn(txn);
	requests.init(g_req_per_query);
	//requests.copy(((YCSBQuery*)(txn->query))->requests);
}

void YCSBQueryMessage::copy_to_txn(TxnManager *txn)
{
	QueryMessage::copy_to_txn(txn);
	//((YCSBQuery*)(txn->query))->requests.copy(requests);
	((YCSBQuery *)(txn->query))->requests.append(requests);
}

void YCSBQueryMessage::copy_from_buf(char *buf)
{
	QueryMessage::copy_from_buf(buf);
	uint64_t ptr = QueryMessage::get_size();
	size_t size;
	COPY_VAL(size, buf, ptr);
	assert(size <= g_req_per_query);
	requests.init(size);
	for (uint64_t i = 0; i < size; i++)
	{
		DEBUG_M("YCSBQueryMessage::copy ycsb_request alloc\n");
		ycsb_request *req = (ycsb_request *)mem_allocator.alloc(sizeof(ycsb_request));
		COPY_VAL(*req, buf, ptr);
		ASSERT(req->key < g_synth_table_size);
		requests.add(req);
	}
	assert(ptr == get_size());
}

void YCSBQueryMessage::copy_to_buf(char *buf)
{
	QueryMessage::copy_to_buf(buf);
	uint64_t ptr = QueryMessage::get_size();
	size_t size = requests.size();
	COPY_BUF(buf, size, ptr);
	for (uint64_t i = 0; i < requests.size(); i++)
	{
		ycsb_request *req = requests[i];
		COPY_BUF(buf, *req, ptr);
	}
	assert(ptr == get_size());
}

/****************************************/

#endif

/************************/

void ClientQueryMessage::init()
{
	first_startts = 0;
}

void ClientQueryMessage::release()
{
	partitions.release();
	first_startts = 0;
}

uint64_t ClientQueryMessage::get_size()
{
	uint64_t size = Message::mget_size();
	size += sizeof(client_startts);
	size += sizeof(size_t);
	size += sizeof(uint64_t) * partitions.size();
	return size;
}

void ClientQueryMessage::copy_from_query(BaseQuery *query)
{
	partitions.clear();
}

void ClientQueryMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
	partitions.clear();
	client_startts = txn->client_startts;
}

void ClientQueryMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
	txn->client_startts = client_startts;
	txn->client_id = return_node_id;
}

void ClientQueryMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_VAL(client_startts, buf, ptr);
	size_t size;
	COPY_VAL(size, buf, ptr);
	partitions.init(size);
	for (uint64_t i = 0; i < size; i++)
	{
		uint64_t part;
		COPY_VAL(part, buf, ptr);
		partitions.add(part);
	}
}

void ClientQueryMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, client_startts, ptr);
	size_t size = partitions.size();
	COPY_BUF(buf, size, ptr);
	for (uint64_t i = 0; i < size; i++)
	{
		uint64_t part = partitions[i];
		COPY_BUF(buf, part, ptr);
	}
}

/************************/

uint64_t ClientResponseMessage::get_size()
{
	uint64_t size = Message::mget_size();
	size += sizeof(uint64_t);
	//for net_id
	size += sizeof(uint64_t);

#if CLIENT_RESPONSE_BATCH == true
	size += sizeof(uint64_t) * index.size();
	size += sizeof(uint64_t) * client_ts.size();
#else
	size += sizeof(uint64_t);
#endif
#if RING_BFT || SHARPER
	size += sizeof(is_cross_shard);
#endif
	return size;
}

void ClientResponseMessage::init()
{
	this->index.init(get_batch_size());
	this->client_ts.init(get_batch_size());
}

void ClientResponseMessage::set_net_id(uint64_t id)
{
	this->net_id = id;
}

void ClientResponseMessage::release()
{
#if CLIENT_RESPONSE_BATCH == true
	index.release();
	client_ts.release();
#endif
}

void ClientResponseMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);

	view = get_current_view(txn->get_thd_id());

#if CLIENT_RESPONSE_BATCH
	this->index.add(txn->get_txn_id());
	this->client_ts.add(txn->client_startts);
#else
	client_startts = txn->client_startts;
#endif
}

void ClientResponseMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);

#if !CLIENT_RESPONSE_BATCH
	txn->client_startts = client_startts;
#endif
}

void ClientResponseMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr = Message::mget_size();

#if CLIENT_RESPONSE_BATCH == true
	index.init(get_batch_size());
	uint64_t tval;
	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		COPY_VAL(tval, buf, ptr);
		index.add(tval);
	}

	client_ts.init(get_batch_size());
	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		COPY_VAL(tval, buf, ptr);
		client_ts.add(tval);
	}
#else
	COPY_VAL(client_startts, buf, ptr);
#endif

	COPY_VAL(view, buf, ptr);
	COPY_VAL(net_id, buf, ptr);
#if RING_BFT || SHARPER
	COPY_VAL(is_cross_shard, buf, ptr);
#endif
	assert(ptr == get_size());
}

void ClientResponseMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr = Message::mget_size();

#if CLIENT_RESPONSE_BATCH == true
	uint64_t tval;
	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		tval = index[i];
		COPY_BUF(buf, tval, ptr);
	}

	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		tval = client_ts[i];
		COPY_BUF(buf, tval, ptr);
	}
#else
	COPY_BUF(buf, client_startts, ptr);
#endif

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, net_id, ptr);
#if RING_BFT || SHARPER
	COPY_BUF(buf, is_cross_shard, ptr);
#endif
	assert(ptr == get_size());
}

string ClientResponseMessage::getString(uint64_t sender)
{
	string message = std::to_string(sender);
	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		message += std::to_string(index[i]) + ":" +
				   std::to_string(client_ts[i]);
	}
	return message;
}

void ClientResponseMessage::sign(uint64_t dest_node)
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

//validate message
bool ClientResponseMessage::validate()
{
#if USE_CRYPTO
	//is signature valid
	string message = getString(this->return_node_id);
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}
#endif

	//count number of accepted response messages for this transaction
	//cout << "IN: " << this->txn_id << " :: " << this->return_node_id << "\n";
	//fflush(stdout);

	return true;
}

/************************/

uint64_t DoneMessage::get_size()
{
	uint64_t size = Message::mget_size();
	return size;
}

void DoneMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
}

void DoneMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void DoneMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr = Message::mget_size();
	assert(ptr == get_size());
}

void DoneMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr = Message::mget_size();
	assert(ptr == get_size());
}

/************************/

uint64_t QueryResponseMessage::get_size()
{
	uint64_t size = Message::mget_size();
	size += sizeof(RC);
	return size;
}

void QueryResponseMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
	rc = txn->get_rc();
}

void QueryResponseMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void QueryResponseMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_VAL(rc, buf, ptr);

	assert(ptr == get_size());
}

void QueryResponseMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, rc, ptr);
	assert(ptr == get_size());
}

/************************/

uint64_t InitDoneMessage::get_size()
{
	uint64_t size = Message::mget_size();
	return size;
}

void InitDoneMessage::copy_from_txn(TxnManager *txn)
{
}

void InitDoneMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void InitDoneMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
}

void InitDoneMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
}

/************************/

uint64_t ReadyServer::get_size()
{
	uint64_t size = Message::mget_size();
	return size;
}

void ReadyServer::copy_from_txn(TxnManager *txn)
{
}

void ReadyServer::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void ReadyServer::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
}

void ReadyServer::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
}

/************************/

uint64_t KeyExchange::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(uint64_t);
	size += pkey.size();
	size += sizeof(uint64_t);
	return size;
}

void KeyExchange::copy_from_txn(TxnManager *txn)
{
}

void KeyExchange::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void KeyExchange::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_VAL(pkeySz, buf, ptr);
	char v;
	for (uint64_t j = 0; j < pkeySz; j++)
	{
		COPY_VAL(v, buf, ptr);
		pkey += v;
	}
	COPY_VAL(return_node, buf, ptr);
}

void KeyExchange::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);
	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, pkeySz, ptr);
	char v;
	for (uint64_t j = 0; j < pkeySz; j++)
	{
		v = pkey[j];
		COPY_BUF(buf, v, ptr);
	}
	COPY_BUF(buf, return_node, ptr);
}

#if CLIENT_BATCH

uint64_t ClientQueryBatch::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(return_node);
	size += sizeof(batch_size);

#if SHARPER
	size += sizeof(bool) * (g_shard_cnt);
#endif

	for (uint i = 0; i < get_batch_size(); i++)
	{
		size += cqrySet[i]->get_size();
	}

#if PRE_ORDER
	size += sizeof(inputState_size);
	size += sizeof(outputState_size);
	size += 2 * sizeof(uint64_t) * inputState.size();
	size += 2 * sizeof(uint64_t) * outputState.size();
#endif

	//DEBUG("test_v2:ClientQueryBatch::get_size():: inputState.size() = %ld, outputState.size() = %ld, sizeof(uint64_t) = %ld\n", inputState.size(), outputState.size(), sizeof(uint64_t));

#if RING_BFT
	size += sizeof(is_cross_shard);
	size += sizeof(bool) * (g_shard_cnt);
#endif
	return size;
}

void ClientQueryBatch::init()
{
	this->return_node = g_node_id;
	this->batch_size = get_batch_size();
#if PRE_ORDER
	this->inputState_size = 0;
	this->outputState_size = 0;
#endif
	this->cqrySet.init(get_batch_size());
}

void ClientQueryBatch::release()
{
	for (uint64_t i = 0; i < get_batch_size(); i++)
	{
		Message::release_message(cqrySet[i]);
	}
	cqrySet.release();
#if PRE_ORDER
	map<uint64_t,uint64_t>().swap(outputState);
	map<uint64_t,uint64_t>().swap(inputState);
#endif
}

void ClientQueryBatch::copy_from_txn(TxnManager *txn)
{
	assert(0);
}

void ClientQueryBatch::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
	assert(0);
}

void ClientQueryBatch::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_VAL(return_node, buf, ptr);
	COPY_VAL(batch_size, buf, ptr);
#if SHARPER
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_VAL(involved_shards[i], buf, ptr);
	}
#endif

	for (uint64_t i = 0; i < cqrySet.size(); i++)
    {
        Message::release_message(cqrySet[i]);
    }
    cqrySet.release();
	
	cqrySet.init(get_batch_size());
	for (uint i = 0; i < get_batch_size(); i++)
	{
		Message *msg = create_message(&buf[ptr]);
		ptr += msg->get_size();
#if BANKING_SMART_CONTRACT
		cqrySet.add((BankingSmartContractMessage *)msg);
#else
		cqrySet.add((YCSBClientQueryMessage *)msg);
#endif
	}

#if PRE_ORDER
	uint64_t key = 0;
	uint64_t value = 0;

	COPY_VAL(inputState_size, buf, ptr);

	for(uint64_t i = 0; i < inputState_size; i++)
	{
		COPY_VAL(key, buf, ptr);
		COPY_VAL(value, buf, ptr);
		inputState[key] = value;
	}

	COPY_VAL(outputState_size, buf, ptr);

	for(uint64_t i = 0; i < outputState_size; i++)
	{
		COPY_VAL(key, buf, ptr);
		COPY_VAL(value, buf, ptr);
		outputState[key] = value;
	}
#endif
	// DEBUG("test_v2:getmessage = %s\n", buf);

	// DEBUG("test_v2:get inputState\n");
	// for(auto item:inputState)
	// {
	// 	DEBUG("key = %ld, value = %ld\n", item.first, item.second);
	// }
	// DEBUG("test_v2:get outputState\n");
	// for(auto item:outputState)
	// {
	// 	DEBUG("key = %ld, value = %ld\n", item.first, item.second);
	// }

#if RING_BFT
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_VAL(involved_shards[i], buf, ptr);
	}
	COPY_VAL(is_cross_shard, buf, ptr);
#endif

	assert(ptr == get_size());
}

void ClientQueryBatch::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, return_node, ptr);
	COPY_BUF(buf, batch_size, ptr);
#if SHARPER
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_BUF(buf, involved_shards[i], ptr);
	}
#endif
	for (uint i = 0; i < get_batch_size(); i++)
	{
		cqrySet[i]->copy_to_buf(&buf[ptr]);
		ptr += cqrySet[i]->get_size();
	}

#if PRE_ORDER
	COPY_BUF(buf, inputState_size, ptr);

	for(auto item:inputState){
		COPY_BUF(buf, item.first, ptr);
		COPY_BUF(buf, item.second, ptr);
    }

	COPY_BUF(buf, outputState_size, ptr);

	for(auto item:outputState){
		COPY_BUF(buf, item.first, ptr);
	 	COPY_BUF(buf, item.second, ptr);
    }
#endif
	// DEBUG("test_v2:sendmessage = %s\n", buf);

	// DEBUG("test_v2:send inputState\n");
	// for(auto item:inputState)
	// {
	// 	DEBUG("key = %ld, value = %ld\n", item.first, item.second);
	// }
	// DEBUG("test_v2:send outputState\n");
	// for(auto item:outputState)
	// {
	// 	DEBUG("key = %ld, value = %ld\n", item.first, item.second);
	// }

#if RING_BFT
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_BUF(buf, involved_shards[i], ptr);
	}
	COPY_BUF(buf, is_cross_shard, ptr);
#endif
	//DEBUG("test_v2: ptr = %ld, size = %ld\n", ptr, get_size())

	assert(ptr == get_size());
}

#if PRE_ORDER
void ClientQueryBatch::generate_input()
{
	for (uint i = 0; i < get_batch_size(); i++)
	{
		for (uint j = 0; j < cqrySet[i]->requests.size(); j++)
		{
			//outputState.insert(make_pair(cqrySet[i]->requests[j]->key, cqrySet[i]->requests[j]->value));不会覆盖
			inputState[cqrySet[i]->requests[j]->key] = 0;
		}
	}
	inputState_size = inputState.size();
}

void ClientQueryBatch::generate_output()
{
	for (uint i = 0; i < get_batch_size(); i++)
	{
		for (uint j = 0; j < cqrySet[i]->requests.size(); j++)
		{
			//outputState.insert(make_pair(cqrySet[i]->requests[j]->key, cqrySet[i]->requests[j]->value));不会覆盖
			outputState[cqrySet[i]->requests[j]->key] = cqrySet[i]->requests[j]->value;
		}
	}
	outputState_size = outputState.size();
}
#endif

string ClientQueryBatch::getString()
{
	string message = std::to_string(this->return_node);
	for (int i = 0; i < BATCH_SIZE; i++)
	{
		message += cqrySet[i]->getRequestString();
	}

#if PRE_ORDER
	for(auto item:inputState){
        message += to_string(item.first);
		message += " ";
        message += to_string(item.second);
		message += " ";
    }

	for(auto item:outputState){
        message += to_string(item.first);
		message += " ";
        message += to_string(item.second);
		message += " ";
    }
#endif

	return message;
}

void ClientQueryBatch::sign(uint64_t dest_node)
{
#if USE_CRYPTO
	string message = this->getString();
	signingClientNode(message, this->signature, this->pubKey, dest_node);

	//cout << "Message: " << message << endl;
	//cout << "Signature: " << this->signature << " :: " << signature.size() << endl;
	//fflush(stdout);

#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

bool ClientQueryBatch::validate()
{
#if USE_CRYPTO
	string message = this->getString();

#if VIEW_CHANGES
	// =====================
	// Sign Bug for forwarded messages
	uint64_t source_node_id = this->return_node;
	if (this->return_node_id < g_node_cnt)
		source_node_id = this->return_node_id;
	// =====================
	if (!validateClientNode(message, this->pubKey, this->signature, source_node_id))
	{
		assert(0);
		return false;
	}
#else
	// make sure signature is valid
	if (!validateClientNode(message, this->pubKey, this->signature, this->return_node))
	{
		assert(0);
		return false;
	}
#endif
#endif
	return true;
}

#endif // Client_Batch

/**************************************************/

uint64_t BatchRequests::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
#if PRE_ORDER
	size += sizeof(inputState_size);
	size += sizeof(outputState_size);
#endif

	size += sizeof(uint64_t) * index.size();
	size += sizeof(uint64_t);
#if SHARPER
	size += sizeof(bool) * (g_shard_cnt);
#endif
	size += hash.length();

	for (uint i = 0; i < get_batch_size(); i++)
	{
		size += requestMsg[i]->get_size();
		// #if ISEOV
		// 	size += 2 * sizeof(uint64_t) * readSet[i].size();
		// 	size += 2 * sizeof(uint64_t) * writeSet[i].size();
		// #endif
	}
#if ISEOV
	for (uint i = 0; i < get_batch_size(); i++)
	{
		//DEBUG_V1("test_v5:readSet[%d].size() = %ld\n", i, readSet[i].size());
		size += 2 * sizeof(uint64_t) * readSet[i].size();
		size += 2 * sizeof(uint64_t) * writeSet[i].size();
	}
#endif
#if PRE_ORDER
	size += 2 * sizeof(uint64_t) * inputState.size();
	size += 2 * sizeof(uint64_t) * outputState.size();
#endif

	size += sizeof(batch_size);

#if RING_BFT
	size += sizeof(is_cross_shard);
	size += sizeof(bool) * (g_shard_cnt);
#endif
	return size;
}

void BatchRequests::add_request_msg(int idx, Message * msg){
     if(requestMsg[idx]){
 		Message::release_message(requestMsg[idx]);
     }
 #if BANKING_SMART_CONTRACT
 	requestMsg[idx] = static_cast<BankingSmartContractMessage *>(msg);
 #else
 	requestMsg[idx] = static_cast<YCSBClientQueryMessage*>(msg);
 #endif
 }

// Initialization
void BatchRequests::init(uint64_t thd_id)
{
// Only primary should create this message
#if MULTI_ON
	this->view = g_node_id;
#elif SHARPER
	assert(view_to_primary(get_current_view(thd_id)) == g_node_id);
	this->view = get_current_view(thd_id);
#elif RING_BFT
	assert(view_to_primary(get_current_view(thd_id)) == g_node_id);
	this->view = get_current_view(thd_id);
#else
	// Only primary should create this message
	assert(get_current_view(thd_id) == g_node_id);
	this->view = get_current_view(thd_id);
#endif
	this->index.init(get_batch_size());
	this->requestMsg.resize(get_batch_size());
#if ISEOV
	this->readSet.resize(get_batch_size());
	this->writeSet.resize(get_batch_size());
#endif

#if PRE_ORDER
	this->inputState_size = 0;
	this->outputState_size = 0; 
#endif
}

#if PRE_ORDER
void BatchRequests::add_state(ClientQueryBatch *bmsg)
{
	this->inputState_size = bmsg->inputState_size;
	this->outputState_size = bmsg->outputState_size;

	this->inputState.insert(bmsg->inputState.begin(), bmsg->inputState.end());
	this->outputState.insert(bmsg->outputState.begin(), bmsg->outputState.end());
	//this->inputState = bmsg->inputState;
	//this->outputState = bmsg->inputState;
}
#endif

// #if ISEOV
//     void BatchRequests::simulate_batch(){
// 		for (uint i = 0; i < get_batch_size(); i++)
// 		{
// 			for (uint j = 0; j < requestMsg[i]->requests.size(); j++)
// 			{
				
// 			}
// 		}
// 	}
// #endif

#if BANKING_SMART_CONTRACT
void BatchRequests::copy_from_txn(TxnManager *txn, BankingSmartContractMessage *clqry)
{
	// Index of the transaction in this bacth.
	uint64_t txnid = txn->get_txn_id();
	uint64_t idx = txnid % get_batch_size();

	// TODO: Some memory is getting consumed while storing client query.
	char *bfr = (char *)malloc(clqry->get_size());
	clqry->copy_to_buf(bfr);
	Message *tmsg = Message::create_message(bfr);
	BankingSmartContractMessage *yqry = (BankingSmartContractMessage *)tmsg;
	free(bfr);

	// this->requestMsg[idx] = yqry;
	add_request_msg(idx, yqry);
	this->index.add(txnid);
}
#else
void BatchRequests::copy_from_txn(TxnManager *txn, YCSBClientQueryMessage *clqry)
{
	// Index of the transaction in this bacth.
	uint64_t txnid = txn->get_txn_id();
	uint64_t idx = txnid % get_batch_size();

	// TODO: Some memory is getting consumed while storing client query.
	char *bfr = (char *)malloc(clqry->get_size());
	clqry->copy_to_buf(bfr);
	Message *tmsg = Message::create_message(bfr);
	YCSBClientQueryMessage *yqry = (YCSBClientQueryMessage *)tmsg;
	free(bfr);

	// this->requestMsg[idx] = yqry;
	add_request_msg(idx, yqry);
	this->index.add(txnid);
}
#endif
void BatchRequests::copy_from_txn(TxnManager *txn)
{
	// Setting txn_id 2 less than the actual value.
	this->txn_id = txn->get_txn_id() - 2;
	this->batch_size = get_batch_size();

	// Storing the representative hash of the batch.
	this->hash = txn->hash;
	this->hashSize = txn->hashSize;
	// Use these lines for testing plain hash function.
	//string message = "anc_def";
	//this->hash.add(calculateHash(message));
}

void BatchRequests::release()
{
	//DEBUG("test_v5:BatchRequests::release():txn_id == %ld\n", txn_id);
	index.release();

	for (uint64_t i = 0; i < requestMsg.size(); i++)
	{
		Message::release_message(requestMsg[i]);
	}
	requestMsg.clear();
#if ISEOV
	for(auto &item:readSet)
	{
		map<uint64_t,uint64_t>().swap(item);
	}
	for(auto &item:writeSet)
	{
		map<uint64_t,uint64_t>().swap(item);
	}
	vector <map<uint64_t,uint64_t>>().swap(readSet);
	vector <map<uint64_t,uint64_t>>().swap(writeSet);
#endif
#if PRE_ORDER
	map<uint64_t,uint64_t>().swap(outputState);
	map<uint64_t,uint64_t>().swap(inputState);
#endif
}

void BatchRequests::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void BatchRequests::copy_from_buf(char *buf)
{
	//DEBUG("test_v5:BatchRequests::copy_from_buf::start\n");
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_VAL(view, buf, ptr);
#if SHARPER
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_VAL(involved_shards[i], buf, ptr);
	}
#endif

	uint64_t elem;
	// Initialization
	release();
	index.init(get_batch_size());
	requestMsg.resize(get_batch_size());

	for (uint i = 0; i < get_batch_size(); i++)
	{
		COPY_VAL(elem, buf, ptr);
		index.add(elem);

		Message *msg = create_message(&buf[ptr]);
		ptr += msg->get_size();
// #if BANKING_SMART_CONTRACT
// 		requestMsg[i] = (BankingSmartContractMessage *)msg;
// #else
// 		requestMsg[i] = (YCSBClientQueryMessage *)msg;
// #endif
		add_request_msg(i, msg);
	}

#if ISEOV
	this->readSet.resize(get_batch_size());
	this->writeSet.resize(get_batch_size());
	for (uint i = 0; i < get_batch_size(); i++)
	{
		uint64_t key = 0;
		uint64_t value = 0;
		uint64_t readSet_size = 1;
	#if BANKING_SMART_CONTRACT
		if (requestMsg[i]->type == 0)
		{	
			readSet_size = 2;
		}
	#else
		uint64_t writeSet_size = 0;
		if (requestMsg[i]->requests[0]->type == YCSB_READ)
		{	
			readSet_size = g_ycsb_column;
			writeSet_size = 0;
		}
		else {
			readSet_size = g_ycsb_column;
			writeSet_size = 1;
		}

	#endif
	
	#if BANKING_SMART_CONTRACT
		for(uint64_t j = 0; j < readSet_size; j++)
		{
			COPY_VAL(key, buf, ptr);
			COPY_VAL(value, buf, ptr);
			//DEBUG_V1("test_v5:copy_from_buf::read[%d]key == %ld, value == %ld\n",i , key, value);
			readSet[i][key] = value;
			//DEBUG("test_v5:BatchRequests::copy_from_buf::readset[%d] = %ld, %ld\n", i, key, value);
		}
		for(uint64_t j = 0; j < readSet_size; j++)
		{
			COPY_VAL(key, buf, ptr);
			COPY_VAL(value, buf, ptr);
			writeSet[i][key] = value;
			//DEBUG_V1("test_v5:copy_from_buf::write[%d]key == %ld, value == %ld\n",i , key, value);
			//DEBUG("test_v5:BatchRequests::copy_from_buf::writeset[%d] = %ld, %ld\n", i, key, value);
		}
	#else
		for(uint64_t j = 0; j < readSet_size; j++)
		{
			COPY_VAL(key, buf, ptr);
			COPY_VAL(value, buf, ptr);
			//DEBUG_V1("test_v5:copy_from_buf::read[%d]key == %ld, value == %ld\n",i , key, value);
			readSet[i][key] = value;
			//DEBUG("test_v5:BatchRequests::copy_from_buf::readset[%d] = %ld, %ld\n", i, key, value);
		}
		for(uint64_t j = 0; j < writeSet_size; j++)
		{	
			COPY_VAL(key, buf, ptr);
			COPY_VAL(value, buf, ptr);
			writeSet[i][key] = value;
			//DEBUG_V1("test_v5:copy_from_buf::write[%d]key == %ld, value == %ld\n",i , key, value);
			//DEBUG("test_v5:BatchRequests::copy_from_buf::writeset[%d] = %ld, %ld\n", i, key, value);
		}

	#endif
    	
		// DEBUG("test_v5:BatchRequests::copy_from_buf::add_an_rw[%d],type = %d\n", i, requestMsg[i]->type);
    	//     for(auto item:this->readSet[i]){
    	//         DEBUG("test_v5:BatchRequests::copy_from_buf::readset[%d] = %ld, %ld\n", i, item.first, item.second);
    	//     }
		//     for(auto item:this->writeSet[i]){
    	//         DEBUG("test_v5:BatchRequests::copy_from_buf::write[%d] = %ld, %ld\n", i, item.first, item.second);
    	//     }
    	
	}
#endif

#if PRE_ORDER
	uint64_t key = 0;
	uint64_t value = 0;

	COPY_VAL(inputState_size, buf, ptr);

	for(uint64_t i = 0; i < inputState_size; i++)
	{
		COPY_VAL(key, buf, ptr);
		COPY_VAL(value, buf, ptr);
		inputState[key] = value;
	}

	COPY_VAL(outputState_size, buf, ptr);

	for(uint64_t i = 0; i < outputState_size; i++)
	{
		COPY_VAL(key, buf, ptr);
		COPY_VAL(value, buf, ptr);
		outputState[key] = value;
	}
#endif

	COPY_VAL(hashSize, buf, ptr);
	ptr = buf_to_string(buf, ptr, hash, hashSize);

	COPY_VAL(batch_size, buf, ptr);

#if RING_BFT
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_VAL(involved_shards[i], buf, ptr);
	}
	COPY_VAL(is_cross_shard, buf, ptr);
#endif
	//DEBUG_V1("test_v5:BatchRequests::copy_from_buf::ptr == %ld, get_size == %ld\n", ptr, get_size());
	assert(ptr == get_size());
	//DEBUG("test_v5:BatchRequests::copy_from_buf::end\n");
	//cout << "test_v4:BatchRequests::copy_from_buf(), get_size() = " <<  get_size() <<"\n";
}

void BatchRequests::copy_to_buf(char *buf)
{
	//DEBUG("test_v5:BatchRequests::copy_to_buf::start\n");
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, view, ptr);
#if SHARPER
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_BUF(buf, involved_shards[i], ptr);
	}
#endif

	uint64_t elem;
	for (uint i = 0; i < get_batch_size(); i++)
	{
		elem = index[i];
		COPY_BUF(buf, elem, ptr);

		//copy client request stored in message to buf
		requestMsg[i]->copy_to_buf(&buf[ptr]);
		ptr += requestMsg[i]->get_size();
	}

#if ISEOV
	for (uint i = 0; i < get_batch_size(); i++)
	{
		// uint64_t readSet_size = 1;
		// if (requestMsg[i]->type == BSC_TRANSFER) readSet_size = 2;
		// else readSet_size = 1;
		// uint64_t count = 0;
		for(auto item:readSet[i])
		{
			//count ++;
			COPY_BUF(buf, item.first, ptr);
			COPY_BUF(buf, item.second, ptr);
			//DEBUG_V1("test_v5:copy_to_buf::read[%d]key == %ld, value == %ld\n",i , item.first, item.second);
			//DEBUG("test_v5:BatchRequests::copy_to_buf::readset[%d] = %ld, %ld\n", i, item.first, item.second);
		}
		// if (count != readSet_size){
		// 	DEBUG("count != readSet_size");
		// 	assert(0);
		// }
		// for(const auto &item:writeSet[i])
		for(auto item:writeSet[i])
		{
			COPY_BUF(buf, item.first, ptr);
			COPY_BUF(buf, item.second, ptr);
			//DEBUG_V1("test_v5:copy_to_buf::write[%d]key == %ld, value == %ld\n",i , item.first, item.second);
			//DEBUG("test_v5:BatchRequests::copy_to_buf::write[%d] = %ld, %ld\n", i, item.first, item.second);
		}
		// for(auto item:readSet[i])
		// {
		// 	COPY_BUF(buf, item.first, ptr);
		// 	COPY_BUF(buf, item.second, ptr);
		// 	DEBUG("test_v5:BatchRequests::copy_to_buf::readset[%d] = %ld, %ld\n", i, item.first, item.second);
		// }
		// for(auto item:writeSet[i])
		// {
		// 	COPY_BUF(buf, item.first, ptr);
		// 	COPY_BUF(buf, item.second, ptr);
		// 	DEBUG("test_v5:BatchRequests::copy_to_buf::write[%d] = %ld, %ld\n", i, item.first, item.second);
		// }
		// DEBUG("test_v5:BatchRequests::copy_to_buf::add_an_rw[%d],type = %d\n", i, requestMsg[i]->type);
    	//     for(auto item:this->readSet[i]){
    	//         DEBUG("test_v5:BatchRequests::copy_to_buf::readset[%d] = %ld, %ld\n", i, item.first, item.second);
    	//     }
		//     for(auto item:this->writeSet[i]){
    	//         DEBUG("test_v5:BatchRequests::copy_to_buf::write[%d] = %ld, %ld\n", i, item.first, item.second);
    	//     }
	}
#endif

#if PRE_ORDER
	COPY_BUF(buf, inputState_size, ptr);

	for(auto item:inputState){
		COPY_BUF(buf, item.first, ptr);
		COPY_BUF(buf, item.second, ptr);
    }

	COPY_BUF(buf, outputState_size, ptr);

	for(auto item:outputState){
		COPY_BUF(buf, item.first, ptr);
	 	COPY_BUF(buf, item.second, ptr);
    }
#endif

	COPY_BUF(buf, hashSize, ptr);

	char v;
	for (uint j = 0; j < hash.size(); j++)
	{
		v = hash[j];
		COPY_BUF(buf, v, ptr);
	}

	COPY_BUF(buf, batch_size, ptr);

#if RING_BFT
	for (uint64_t i = 0; i < g_shard_cnt; i++)
	{
		COPY_BUF(buf, involved_shards[i], ptr);
	}
	COPY_BUF(buf, is_cross_shard, ptr);
#endif
	//DEBUG_V1("test_v5:BatchRequests::copy_to_buf::ptr == %ld, get_size == %ld\n", ptr, get_size());
	assert(ptr == get_size());
	//DEBUG("test_v5:BatchRequests::copy_to_buf::end\n");
	//cout << "test_v5:BatchRequests::copy_to_buf::end\n";
	//cout << "test_v4:BatchRequests::copy_to_buf(), get_size() = " <<  get_size() <<"\n";
}

string BatchRequests::getString(uint64_t sender)
{
	string message = std::to_string(sender);
	for (uint i = 0; i < get_batch_size(); i++)
	{
		message += std::to_string(index[i]);
		message += requestMsg[i]->getRequestString();
	}
	message += hash;

#if PRE_ORDER
	for(auto item:inputState){
        message += to_string(item.first);
		message += " ";
        message += to_string(item.second);
		message += " ";
    }

	for(auto item:outputState){
        message += to_string(item.first);
		message += " ";
        message += to_string(item.second);
		message += " ";
    }
#endif

	return message;
}

void BatchRequests::sign(uint64_t dest_node)
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
bool BatchRequests::validate(uint64_t thd_id)
{

#if USE_CRYPTO
	string message = getString(this->return_node_id);

	//cout << "Sign: " << this->signature << "\n";
	//fflush(stdout);

	//cout << "Pkey: " << this->pubKey << "\n";
	//fflush(stdout);

	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}

#endif

	// String of transactions in a batch to generate hash.
	string batchStr;
	for (uint i = 0; i < get_batch_size(); i++)
	{
		// Append string representation of this txn.
		batchStr += this->requestMsg[i]->getString();
	}

	// Is hash of request message valid
	if (this->hash != calculateHash(batchStr))
	{
		assert(0);
		return false;
	}

	//cout << "Done Hash\n";
	//fflush(stdout);

	//is the view the same as the view observed by this message
#if !RBFT_ON && !MULTI_ON
	if (this->view != get_current_view(thd_id))
	{
		cout << "this->view: " << this->view << endl;
		cout << "get_current_view: " << get_current_view(thd_id) << endl;
		cout << "this->txn_id: " << this-> txn_id << endl;
		fflush(stdout);
		assert(0);
		return false;
	}
#endif

	return true;
}

#if PRE_ORDER
bool BatchRequests::validate_pre_exec()
{
	map<uint64_t,uint64_t> temp;
	for (uint i = 0; i < get_batch_size(); i++)
	{
		for (uint j = 0; j < requestMsg[i]->requests.size(); j++)
		{
			temp[requestMsg[i]->requests[j]->key] = requestMsg[i]->requests[j]->value;
		}
	}
	if (temp.size() != outputState_size) 
	{
		DEBUG("test_v2:temp.size() != outputState_size, %ld, %ld\n",temp.size(), outputState_size )
		return false;
	}
	else if(temp != outputState)
	{
		DEBUG("test_v2:temp != outputState\n")
		DEBUG("temp: \n")
		for(auto item:temp)
		{
			DEBUG("test_v2:key = %ld, value = %ld\n", item.first, item.second);
		}
		DEBUG("outputState: \n")
		for(auto item:outputState)
		{
			DEBUG("test_v2:key = %ld, value = %ld\n", item.first, item.second);
		}
		return false;
	}

	return true;
}
#endif


/************************************/

uint64_t ExecuteMessage::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
	size += sizeof(index);
	size += hash.length();
	size += sizeof(hashSize);
	size += sizeof(return_node);
	size += sizeof(end_index);
	size += sizeof(batch_size);
	size += sizeof(net_id);

	return size;
}

void ExecuteMessage::copy_from_txn(TxnManager *txn)
{
	// Constructing txn manager for one transaction less than end index.
	this->txn_id = txn->get_txn_id() - 1;

	this->view = get_current_view(txn->get_thd_id());
	this->index = txn->get_txn_id() + 1 - get_batch_size();
	this->end_index = txn->get_txn_id();
	this->batch_size = get_batch_size();
	this->hash = txn->get_hash();
	this->hashSize = txn->get_hashSize();
	this->return_node = g_node_id;
	this->net_id = txn->net_id;
}

void ExecuteMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void ExecuteMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(view, buf, ptr);
	COPY_VAL(index, buf, ptr);
	COPY_VAL(hashSize, buf, ptr);

	ptr = buf_to_string(buf, ptr, hash, hashSize);

	COPY_VAL(return_node, buf, ptr);
	COPY_VAL(end_index, buf, ptr);
	COPY_VAL(batch_size, buf, ptr);
	COPY_VAL(net_id, buf, ptr);

	assert(ptr == get_size());
}

void ExecuteMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, index, ptr);
	COPY_BUF(buf, hashSize, ptr);

	char v;
	for (uint64_t i = 0; i < hash.size(); i++)
	{
		v = hash[i];
		COPY_BUF(buf, v, ptr);
	}

	COPY_BUF(buf, return_node, ptr);
	COPY_BUF(buf, end_index, ptr);
	COPY_BUF(buf, batch_size, ptr);
	COPY_BUF(buf, net_id, ptr);

	assert(ptr == get_size());
}

/************************************/

uint64_t CheckpointMessage::get_size()
{
	uint64_t size = Message::mget_size();
	size += sizeof(index);
	size += sizeof(return_node);
	size += sizeof(end_index);

	return size;
}

void CheckpointMessage::copy_from_txn(TxnManager *txn)
{
	this->txn_id = txn->get_txn_id() - 5;

	//index of first request executed since last chkpt.
	this->index = curr_next_index() - txn_per_chkpt();
	this->return_node = g_node_id;
	this->end_index = curr_next_index();

	// Now implemted in msg_queue::enqueue
	//this->sign();
}

//unused
void CheckpointMessage::copy_to_txn(TxnManager *txn)
{
	assert(0);
	Message::mcopy_to_txn(txn);
}

void CheckpointMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(index, buf, ptr);
	COPY_VAL(return_node, buf, ptr);
	COPY_VAL(end_index, buf, ptr);

	assert(ptr == get_size());
}

void CheckpointMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, index, ptr);
	COPY_BUF(buf, return_node, ptr);
	COPY_BUF(buf, end_index, ptr);

	assert(ptr == get_size());
}

void CheckpointMessage::sign(uint64_t dest_node)
{
#if USE_CRYPTO
	string message = this->toString();
	signingNodeNode(message, this->signature, this->pubKey, dest_node);
#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

//validate message, add to message log, and check if node has enough messages
bool CheckpointMessage::addAndValidate()
{
	if (!this->validate())
	{
		return false;
	}

	if (this->index < get_curr_chkpt())
	{
		return false;
	}

	return true;
}

string CheckpointMessage::toString()
{
	return std::to_string(this->index) + '_' +
		   std::to_string(this->end_index) + '_' +
		   std::to_string(this->return_node); //still needs digest of state
}

//is message valid
bool CheckpointMessage::validate()
{
#if USE_CRYPTO
	string message = this->toString();

	//verify signature of message
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}
#endif

	return true;
}

/************************************/

//function for copying a string into the char buffer
uint64_t Message::string_to_buf(char *buf, uint64_t ptr, string str)
{
	char v;
	for (uint64_t i = 0; i < str.size(); i++)
	{
		v = str[i];
		COPY_BUF(buf, v, ptr);
	}
	return ptr;
}

//function for copying data from the buffer into a string
uint64_t Message::buf_to_string(char *buf, uint64_t ptr, string &str, uint64_t strSize)
{
	char v;
	for (uint64_t i = 0; i < strSize; i++)
	{
		COPY_VAL(v, buf, ptr);
		str += v;
	}
	return ptr;
}

// Message Creation methods.
char *create_msg_buffer(Message *msg)
{
	char *buf = (char *)malloc(msg->get_size());
	return buf;
}

Message *deep_copy_msg(char *buf, Message *msg)
{
	msg->copy_to_buf(buf);
	Message *copy_msg = Message::create_message(buf);
	return copy_msg;
}

void delete_msg_buffer(char *buf)
{
	free(buf);
}

/**************************/

uint64_t PBFTPrepMessage::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
	size += sizeof(index);
	size += hash.length();
	size += sizeof(hashSize);
	size += sizeof(return_node);
	size += sizeof(end_index);
	size += sizeof(batch_size);

	return size;
}

void PBFTPrepMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
#if MULTI_ON
  	this->view = txn->instance_id;
#else
	this->view = get_current_view(txn->get_thd_id());
  	//this->view = get_current_view(local_view[txn->get_thd_id()]);
#endif  
	this->end_index = txn->get_txn_id();
	this->index = this->end_index + 1 - get_batch_size();
	this->hash = txn->get_hash();
	this->hashSize = txn->get_hashSize();
	this->return_node = g_node_id;
	this->batch_size = get_batch_size();
}

void PBFTPrepMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void PBFTPrepMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(view, buf, ptr);
	COPY_VAL(index, buf, ptr);
	COPY_VAL(hashSize, buf, ptr);

	ptr = buf_to_string(buf, ptr, hash, hashSize);

	COPY_VAL(return_node, buf, ptr);
	COPY_VAL(end_index, buf, ptr);
	COPY_VAL(batch_size, buf, ptr);

	assert(ptr == get_size());
}

void PBFTPrepMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, index, ptr);
	COPY_BUF(buf, hashSize, ptr);

	char v;
	for (uint64_t i = 0; i < hash.size(); i++)
	{
		v = hash[i];
		COPY_BUF(buf, v, ptr);
	}

	COPY_BUF(buf, return_node, ptr);

	COPY_BUF(buf, end_index, ptr);
	COPY_BUF(buf, batch_size, ptr);

	assert(ptr == get_size());
}

string PBFTPrepMessage::toString()
{
	string signString = std::to_string(this->view);
	signString += '_' + std::to_string(this->index) + '_' +
				  this->hash + '_' + std::to_string(this->return_node) +
				  '_' + to_string(this->end_index);

	return signString;
}

void PBFTPrepMessage::sign(uint64_t dest_node)
{
#if USE_CRYPTO
	string message = this->toString();
	signingNodeNode(message, this->signature, this->pubKey, dest_node);
#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

//makes sure message is valid, returns true or false;
bool PBFTPrepMessage::validate()
{
#if USE_CRYPTO
	//verifies message signature
	string message = this->toString();
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}
#endif

	return true;
}

/****************************************/

uint64_t PBFTCommitMessage::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
	size += sizeof(index);
	size += hash.length();
	size += sizeof(hashSize);
	size += sizeof(return_node);
	size += sizeof(end_index);
	size += sizeof(batch_size);

#if RING_BFT
	size += sizeof(is_cross_shard);
#endif
	return size;
}

void PBFTCommitMessage::copy_from_txn(TxnManager *txn)
{
	Message::mcopy_from_txn(txn);
	#if MULTI_ON
  	this->view = txn->instance_id;
    #else
	this->view = get_current_view(txn->get_thd_id());
	#endif
	this->end_index = txn->get_txn_id();
	this->index = this->end_index + 1 - get_batch_size();
	this->hash = txn->get_hash();
	this->hashSize = txn->get_hashSize();
	this->return_node = g_node_id;
	this->batch_size = get_batch_size();
}

void PBFTCommitMessage::copy_to_txn(TxnManager *txn)
{
	Message::mcopy_to_txn(txn);
}

void PBFTCommitMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(view, buf, ptr);
	COPY_VAL(index, buf, ptr);
	COPY_VAL(hashSize, buf, ptr);

	ptr = buf_to_string(buf, ptr, hash, hashSize);

	COPY_VAL(return_node, buf, ptr);
	COPY_VAL(end_index, buf, ptr);
	COPY_VAL(batch_size, buf, ptr);
#if RING_BFT
	COPY_VAL(is_cross_shard, buf, ptr);
#endif

	assert(ptr == get_size());
}

void PBFTCommitMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, index, ptr);
	COPY_BUF(buf, hashSize, ptr);

	char v;
	for (uint64_t i = 0; i < hash.size(); i++)
	{
		v = hash[i];
		COPY_BUF(buf, v, ptr);
	}

	COPY_BUF(buf, return_node, ptr);
	COPY_BUF(buf, end_index, ptr);
	COPY_BUF(buf, batch_size, ptr);

#if RING_BFT
	COPY_BUF(buf, is_cross_shard, ptr);
#endif
	assert(ptr == get_size());
}

string PBFTCommitMessage::toString()
{
	string signString = std::to_string(this->view);
	signString += '_' + std::to_string(this->index) + '_' +
				  this->hash + '_' + std::to_string(this->return_node) +
				  '_' + to_string(this->end_index);

	return signString;
}

//signs current message
void PBFTCommitMessage::sign(uint64_t dest_node)
{
#if USE_CRYPTO
	string message = this->toString();

	//cout << "Signing Commit msg: " << message << endl;
#if RING_BFT
	if (this->is_cross_shard)
		signingClientNode(message, this->signature, this->pubKey, dest_node);
	else
		signingNodeNode(message, this->signature, this->pubKey, dest_node);
#else
	signingNodeNode(message, this->signature, this->pubKey, dest_node);
#endif

#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

//makes sure message is valid, returns true or false;
bool PBFTCommitMessage::validate()
{
	string message = this->toString();

#if USE_CRYPTO

//verify signature of message
#if RING_BFT
	if (this->is_cross_shard)
	{
		if (!validateClientNode(message, this->pubKey, this->signature, this->return_node_id))
		{
			assert(0);
			return false;
		}
	}
	else
	{
		if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
		{
			assert(0);
			return false;
		}
	}

#else
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}
#endif
#endif
	return true;
}

void BroadcastBatchMessage::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_VAL(net_id, buf, ptr);
	COPY_VAL(breq_return_id, buf, ptr);
	Message *msg = create_message(&buf[ptr]);
	ptr += msg->get_size();
	breq = static_cast<BatchRequests*>(msg);
	breq->return_node_id = breq_return_id;
	COPY_VAL(cmsgSet_size, buf, ptr);

	cmsgSet.resize(cmsgSet_size);
	for (uint i = 0; i < cmsgSet_size; i++)
	{
		Message *msg = create_message(&buf[ptr]);
		cmsgSet[i] = (PBFTCommitMessage *)msg;
		ptr += msg->get_size();
	}

	#if DEBUG_DISTR
	if(ptr != get_size())
	{
		DEBUG("test_v3: BBroadcastBatchMessage::copy_from_buf::ptr != get_size(), ptr = %ld,  size = %ld", ptr, get_size());
	}
	#endif
	

	assert(ptr == get_size());
}

void BroadcastBatchMessage::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();
	COPY_BUF(buf, net_id, ptr);
	COPY_BUF(buf, breq_return_id, ptr);
	breq->copy_to_buf(&buf[ptr]);
	ptr += breq->get_size();
	COPY_BUF(buf, cmsgSet_size, ptr);

	for (uint i = 0; i < cmsgSet_size; i++)
	{
		//copy client request stored in message to buf
		cmsgSet[i]->copy_to_buf(&buf[ptr]);
		ptr += cmsgSet[i]->get_size();
	}
	#if DEBUG_DISTR
	if(ptr != get_size())
	{
		DEBUG("test_v3: BroadcastBatchMessage::copy_to_buf::ptr != get_size(), ptr = %ld,  size = %ld", ptr, get_size());
	}
	#endif
	

	assert(ptr == get_size());
}

void BroadcastBatchMessage::copy_from_txn(TxnManager *txn)
{
	DEBUG("test_v3: err:BroadcastBatchMessage::copy_from_txn");
	assert(0);
}

void BroadcastBatchMessage::copy_to_txn(TxnManager *txn)
{
	DEBUG("test_v3: err:BroadcastBatchMessage::copy_to_txn");
	assert(0);
}

uint64_t BroadcastBatchMessage::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(net_id);
	size += breq->get_size();
	size += sizeof(cmsgSet_size);
	size += sizeof(breq_return_id);
	for (uint i = 0; i < cmsgSet_size; i++)
	{
		size += cmsgSet[i]->get_size();
	}

	return size;
}

void BroadcastBatchMessage::init()
{
	//global call get_net_id;
	this->net_id = g_net_id;
	cmsgSet_size = 0;
}
void BroadcastBatchMessage::release()
{
	Message::release_message(this->breq);
	for (uint i = 0; i < cmsgSet_size; i++)
	{
		Message::release_message(cmsgSet[i]);
	}
}
//string BroadcastBatchMessage::getString(uint64_t sender)
string BroadcastBatchMessage::getString()
{
	//string message = std::to_string(sender);
	string message = std::to_string(this->net_id);
	message += this->breq->getString(this->breq->return_node_id);
	message += std::to_string(this->cmsgSet_size);
	for (uint i = 0; i < cmsgSet_size; i++)
	{
		message += cmsgSet[i]->toString();
	}
	//cout << "test_v4: BroadcastBatchMessage::getString-breq->key length = : " << breq->keySize <<"\n"; 
	//cout << "test_v4: BroadcastBatchMessage::getString: " << message <<"\n"; 
	return message;

}
void BroadcastBatchMessage::sign(uint64_t dest_node)
{
	#if USE_CRYPTO
	//string message = getString(g_node_id);
	string message = getString();

	signingNodeNode(message, this->signature, this->pubKey, dest_node);
#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}
bool BroadcastBatchMessage::validate()
{
	#if USE_CRYPTO
	//string message = getString(this->return_node_id);
	string message = getString();

	//cout << "Sign: " << this->signature << "\n";
	//fflush(stdout);

	//cout << "Pkey: " << this->pubKey << "\n";
	//fflush(stdout);

	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}

	#endif

	string batchStr;
	for (uint i = 0; i < get_batch_size(); i++)
	{
		// Append string representation of this txn.
		batchStr += this->breq->requestMsg[i]->getString();
	}

	// Is hash of request message valid
	if (this->breq->hash != calculateHash(batchStr))
	{
		assert(0);
		return false;
	}
	return true;

	//uint64_t thd_id = 0;
	//return this->breq->validate(thd_id);
}

void BroadcastBatchMessage::add_batch(Message *msg)
{
	//cout << "test_v4:BroadcastBatchMessage::add_batch(Message *msg), batch_id : " << msg->batch_id;
 	breq = static_cast<BatchRequests *>(msg);
	breq_return_id = breq->return_node_id;
}

void BroadcastBatchMessage::add_commit_msg(PBFTCommitMessage *cmsg)
{
	char *buf = create_msg_buffer(cmsg);
    Message *deepMsg = deep_copy_msg(buf, cmsg);
    cmsgSet.push_back((PBFTCommitMessage *)deepMsg);
    delete_msg_buffer(buf);
	//cmsgSet.push_back(cmsg);
	cmsgSet_size++;
}

/****************************************/
/*	VIEW CHANGE SPECIFIC		*/
/****************************************/

#if VIEW_CHANGES

uint64_t ViewChangeMsg::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
	size += sizeof(index);
	size += sizeof(numPreMsgs);
	size += sizeof(numPrepMsgs);

	for (uint64_t i = 0; i < preMsg.size(); i++)
	{
		size += preMsg[i]->get_size();
	}

	size += sizeof(uint64_t) * numPrepMsgs * 3;
	for (uint64_t i = 0; i < numPrepMsgs; i++)
	{
		size += prepHash[i].length();
	}

	size += sizeof(return_node);

	return size;
}

void ViewChangeMsg::init(uint64_t thd_id, TxnManager *txn)
{
	// Set the succeeding replica as the new primary.
	this->view = ((get_current_view(thd_id) + 1) % g_node_cnt);
	this->index = txn->get_txn_id(); // Last checkpoint idx.
	this->return_node = g_node_id;

	// cout << "Checkpoint: " << this->index << "\n";
	fflush(stdout);

	// Start collecting from the next batch, but as curr_next_index would
	// only point to the last request in the batch, so we add required amt
	uint64_t st = get_commit_message_txn_id(this->index + get_batch_size());

	bool found;
	uint64_t j = 0;
	BatchRequests *breq;
	for (uint64_t i = st; i <= curr_next_index(); i += get_batch_size())
	{
		found = false;

		// Linearly accessing the breqStore. As all requests are stored
		// sequentially so no need for repetitive access.
		bstoreMTX.lock();
		for (; j < breqStore.size(); j++)
		{
			breq = breqStore[j];
			if (breq->index[get_batch_size() - 1] == i)
			{
				found = true;
				break;
			}
		}
		bstoreMTX.unlock();

		// Storing the matched batch to the vector.
		if (found)
		{
			char *buf = create_msg_buffer(breq);
			Message *deepCMsg = deep_copy_msg(buf, breq);
			this->preMsg.push_back((BatchRequests *)deepCMsg);
			delete_msg_buffer(buf);
		}
	}

	// Store the corresponding PBFTPrepMessage.
	for (uint64_t i = st; i <= curr_next_index(); i += get_batch_size())
	{
		TxnManager *tman = txn_table.get_transaction_manager(thd_id, i, 0);
		while (true)
		{
			bool ready = tman->unset_ready();
			if (!ready)
			{
				printf("trying to get txn_man %ld\n", tman->get_txn_id());
				continue;
			}
			else
			{
				break;
			}
		}
		prepView.push_back(get_current_view(thd_id));
		prepIdx.push_back(i);
		prepHash.push_back(tman->get_hash());
		prepHsize.push_back(tman->get_hashSize());
		bool ready = tman->set_ready();
        assert(ready);
	}

	this->numPreMsgs = this->preMsg.size();
	this->numPrepMsgs = this->prepIdx.size();
}

void ViewChangeMsg::copy_from_buf(char *buf)
{
	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(view, buf, ptr);
	COPY_VAL(index, buf, ptr);
	COPY_VAL(numPreMsgs, buf, ptr);
	COPY_VAL(numPrepMsgs, buf, ptr);

	for (uint i = 0; i < numPreMsgs; i++)
	{
		Message *msg = create_message(&buf[ptr]);
		ptr += msg->get_size();
		preMsg.push_back((BatchRequests *)msg);
	}

	for (uint64_t i = 0; i < numPrepMsgs; i++)
	{
		uint64_t tval;
		string hsh;
		COPY_VAL(tval, buf, ptr);
		prepView.push_back(tval);

		COPY_VAL(tval, buf, ptr);
		prepIdx.push_back(tval);

		COPY_VAL(tval, buf, ptr);
		prepHsize.push_back(tval);

		ptr = buf_to_string(buf, ptr, hsh, tval);
		prepHash.push_back(hsh);
	}

	COPY_VAL(return_node, buf, ptr);

	assert(ptr == get_size());
}

void ViewChangeMsg::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, index, ptr);
	COPY_BUF(buf, numPreMsgs, ptr);
	COPY_BUF(buf, numPrepMsgs, ptr);

	assert(numPreMsgs == preMsg.size());
	for (uint64_t i = 0; i < numPreMsgs; i++)
	{
		preMsg[i]->copy_to_buf(&buf[ptr]);
		ptr += preMsg[i]->get_size();
	}

	assert(numPrepMsgs == prepIdx.size());
	uint64_t tval;
	for (uint64_t i = 0; i < numPrepMsgs; i++)
	{
		tval = prepView[i];
		COPY_BUF(buf, tval, ptr);

		tval = prepIdx[i];
		COPY_BUF(buf, tval, ptr);

		tval = prepHsize[i];
		COPY_BUF(buf, tval, ptr);

		char v;
		string hsh = prepHash[i];
		for (uint64_t j = 0; j < tval; j++)
		{
			v = hsh[j];
			COPY_BUF(buf, v, ptr);
		}
	}

	COPY_BUF(buf, return_node, ptr);
	assert(ptr == get_size());
}

void ViewChangeMsg::release()
{
	BatchRequests *pmsg;
	uint64_t i = 0;
	for (; i < preMsg.size();)
	{
		pmsg = preMsg[i];
		preMsg.erase(preMsg.begin() + i);
		Message::release_message(pmsg);
	}
	preMsg.clear();

	prepView.clear();
	prepIdx.clear();
	prepHash.clear();
	prepHsize.clear();
}

void ViewChangeMsg::sign(uint64_t dest)
{
#if USE_CRYPTO
	string message = this->toString();

	signingNodeNode(message, this->signature, this->pubKey, dest);
#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

// validate message and check to see if it should be added to message log
// and if we have recieved enough messages
bool ViewChangeMsg::addAndValidate(uint64_t thd_id)
{
	if (this->return_node != g_node_id)
	{
		if (!this->validate(thd_id))
		{
			return false;
		}
	}

	//count number of matching view change messages recieved previously
	uint64_t i = 0;
	ViewChangeMsg *vmsg;
	for (; i < view_change_msgs.size(); i++)
	{
		vmsg = view_change_msgs[i];
		if (this->return_node == vmsg->return_node ||
			this->index != vmsg->index ||
			this->view != vmsg->view)
		{
			assert(0);
		}
	}

	storeVCMsg(this);

	// If equal to the number of msgs recieved.
	i++;
	if (i < (2 * g_min_invalid_nodes + 1))
	{
		return false;
	}

	return true;
}

//validate message and all messages this message contains
bool ViewChangeMsg::validate(uint64_t thd_id)
{
	string message = this->toString();

#if USE_CRYPTO
	//verify signature of message
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}
#endif

	// Validate the prepare messages
	BatchRequests *bmsg;
	uint64_t count;
	for (uint64_t i = 0; i < preMsg.size(); i++)
	{
		bmsg = preMsg[i];
		/*
		if (!bmsg->validate(thd_id))
		{
			assert(0);
			return false;
		}
		*/

		count = 0;
		for (uint64_t j = 0; j < prepIdx.size(); j++)
		{
			if (prepIdx[j] == bmsg->txn_id + 2)
			{
				if (prepHash[j] != bmsg->hash)
				{
					assert(0);
				}
				count++;
			}
		}

		if (count == 0)
		{
			assert(0);
		}
	}

	return true;
}

string ViewChangeMsg::toString()
{
	string message;
	message += to_string(view) + '_' + to_string(index) + '_';

	for (uint i = 0; i < preMsg.size(); i++)
	{
		message += preMsg[i]->getString(this->return_node) + '_';
	}

	for (uint64_t i = 0; i < prepIdx.size(); i++)
	{
		message += to_string(prepView[i]) + '_' +
				   to_string(prepIdx[i]) + '_' +
				   prepHash[i] + '_' + to_string(prepHsize[i]);
	}

	message += to_string(return_node);

	return message;
}

/************************************/

void NewViewMsg::init(uint64_t thd_id)
{
	this->view = g_node_id;

	//add view change messages
	ViewChangeMsg *vmsg;
	for (uint64_t i = 0; i < view_change_msgs.size(); i++)
	{
		vmsg = view_change_msgs[i];

		// cout << "View MSG: " << vmsg->index << "\n";
		// fflush(stdout);

		char *buf = create_msg_buffer(vmsg);
		Message *deepCMsg = deep_copy_msg(buf, vmsg);
		deepCMsg->return_node_id = vmsg->return_node_id;
		this->viewMsg.push_back((ViewChangeMsg *)deepCMsg);
		delete_msg_buffer(buf);
	}

	this->numViewChangeMsgs = this->viewMsg.size();

	// Ideally, we should find the range of completed pre-prepare requests.
	// However, in our case as all the view change messages are same, so
	// we just copy all pre-prepare messages from one of the view change.
	vmsg = view_change_msgs[0];
	BatchRequests *pmsg;
	for (uint64_t i = 0; i < vmsg->preMsg.size(); i++)
	{
		pmsg = vmsg->preMsg[i];
		char *buf = create_msg_buffer(pmsg);
		Message *deepCMsg = deep_copy_msg(buf, pmsg);
		deepCMsg->return_node_id = pmsg->return_node_id;
		this->preMsg.push_back((BatchRequests *)deepCMsg);
		delete_msg_buffer(buf);
	}

	this->numPreMsgs = vmsg->preMsg.size();
}

uint64_t NewViewMsg::get_size()
{
	uint64_t size = Message::mget_size();

	size += sizeof(view);
	size += sizeof(numViewChangeMsgs);
	size += sizeof(numPreMsgs);

	for (uint64_t i = 0; i < viewMsg.size(); i++)
		size += viewMsg[i]->get_size();

	for (uint64_t i = 0; i < preMsg.size(); i++)
		size += preMsg[i]->get_size();

	return size;
}

void NewViewMsg::copy_from_buf(char *buf)
{

	Message::mcopy_from_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_VAL(view, buf, ptr);
	COPY_VAL(numViewChangeMsgs, buf, ptr);
	COPY_VAL(numPreMsgs, buf, ptr);

	for (uint i = 0; i < numViewChangeMsgs; i++)
	{
		Message *msg = create_message(&buf[ptr]);
		ptr += msg->get_size();
		viewMsg.push_back((ViewChangeMsg *)msg);
	}

	for (uint i = 0; i < numPreMsgs; i++)
	{
		Message *msg = create_message(&buf[ptr]);
		ptr += msg->get_size();
		preMsg.push_back((BatchRequests *)msg);
	}

	assert(ptr == get_size());
}

void NewViewMsg::copy_to_buf(char *buf)
{
	Message::mcopy_to_buf(buf);

	uint64_t ptr = Message::mget_size();

	COPY_BUF(buf, view, ptr);
	COPY_BUF(buf, numViewChangeMsgs, ptr);
	COPY_BUF(buf, numPreMsgs, ptr);

	assert(viewMsg.size() == numViewChangeMsgs);
	for (uint64_t i = 0; i < viewMsg.size(); i++)
	{
		viewMsg[i]->copy_to_buf(&buf[ptr]);
		ptr += viewMsg[i]->get_size();
	}

	assert(numPreMsgs == preMsg.size());
	for (uint64_t i = 0; i < numPreMsgs; i++)
	{
		preMsg[i]->copy_to_buf(&buf[ptr]);
		ptr += preMsg[i]->get_size();
	}

	assert(ptr == get_size());
}

void NewViewMsg::release()
{
	BatchRequests *pmsg;
	uint64_t i = 0;
	for (; i < preMsg.size();)
	{
		pmsg = preMsg[i];
		preMsg.erase(preMsg.begin() + i);
		Message::release_message(pmsg);
	}

	ViewChangeMsg *vmsg;
	for (i = 0; i < viewMsg.size();)
	{
		vmsg = viewMsg[i];
		viewMsg.erase(viewMsg.begin() + i);
		Message::release_message(vmsg);
	}
	viewMsg.clear();
}

string NewViewMsg::toString()
{
	string message;
	message += this->view;
	message += '_';

	for (uint64_t i = 0; i < viewMsg.size(); i++)
	{
		message += viewMsg[i]->toString();
		message += '_';
	}

	for (uint64_t i = 0; i < preMsg.size(); i++)
	{
		message += preMsg[i]->getString(this->return_node_id);
		message += '_';
	}

	return message;
}

void NewViewMsg::sign(uint64_t dest)
{
#if USE_CRYPTO
	string message = this->toString();

	signingNodeNode(message, this->signature, this->pubKey, dest);
#else
	this->signature = "0";
#endif
	this->sigSize = this->signature.size();
	this->keySize = this->pubKey.size();
}

bool NewViewMsg::validate(uint64_t thd_id)
{
	string message = this->toString();

#if USE_CRYPTO
	//verify signature of message
	if (!validateNodeNode(message, this->pubKey, this->signature, this->return_node_id))
	{
		assert(0);
		return false;
	}

#endif
	return true;
}

/*******************************/

// Entities for handling BatchRequests message during a view change.
vector<BatchRequests *> breqStore;
std::mutex bstoreMTX;

// Stores a BatchRequests message.
void storeBatch(BatchRequests *breq)
{
	bstoreMTX.lock();
	breqStore.push_back(breq);
	bstoreMTX.unlock();
}

// Removes all the BatchRequests message till the specified range.
void removeBatch(uint64_t range)
{
	uint64_t i = 0;
	BatchRequests *bmsg;
	bstoreMTX.lock();
	for (; i < breqStore.size();)
	{
		bmsg = breqStore[i];
		if (bmsg->index[get_batch_size() - 1] < range)
		{
			breqStore.erase(breqStore.begin() + i);
			Message::release_message(bmsg);
		}
		else
		{
			// The first message with greater index, break!
			break;
		}
	}
	bstoreMTX.unlock();
}

// Entities for handling ViewChange message.
vector<ViewChangeMsg *> view_change_msgs;

void storeVCMsg(ViewChangeMsg *vmsg)
{
	char *buf = create_msg_buffer(vmsg);
	Message *deepCMsg = deep_copy_msg(buf, vmsg);
	view_change_msgs.push_back((ViewChangeMsg *)deepCMsg);
	delete_msg_buffer(buf);
}

void clearAllVCMsg()
{
	ViewChangeMsg *vmsg;
	uint64_t i = 0;
	for (; i < view_change_msgs.size();)
	{
		vmsg = view_change_msgs[i];
		view_change_msgs.erase(view_change_msgs.begin() + i);
		Message::release_message(vmsg);
	}
	view_change_msgs.clear();
}

#endif // VIEW_CHANGES

/************************************/


/************************************/
