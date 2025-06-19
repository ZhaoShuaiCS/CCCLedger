#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "sim_manager.h"
#include "query.h"
#include "client_query.h"
#include "transport.h"
#include "work_queue.h"

#include "msg_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "client_txn.h"
#include "txn.h"
#include "../config.h"
#include <array>

mem_alloc mem_allocator;
Stats stats;
SimManager *simulation;
// Query_queue query_queue;
Client_query_queue client_query_queue;
Transport tport_man;
// TxnManPool txn_man_pool;
TxnPool txn_pool;
// TxnTablePool txn_table_pool;
QryPool qry_pool;
#if NET_BROADCAST
vector<TxnTable *> txn_tables;
#else
TxnTable txn_tables;
#endif
QWorkQueue work_queue;

MessageQueue msg_queue;
Client_txn client_man;
//map<uint64_t, bool> priconsensus;
#if NET_BROADCAST
std::array<bool,PRICONSENSUS_SIZE> priconsensus;
#endif

bool volatile warmup_done = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;

UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
#if RING_BFT
int32_t g_inflight_max = !g_node_id ? MAX_TXN_IN_FLIGHT : MAX_TXN_IN_FLIGHT * (1 + CROSS_SHARD_PRECENTAGE / 100);
#else
int32_t g_inflight_max = MAX_TXN_IN_FLIGHT;
#endif
uint64_t g_msg_size = MSG_SIZE_MAX;
int32_t g_load_per_server = LOAD_PER_SERVER;

bool g_hw_migrate = HW_MIGRATE;

volatile UInt64 g_row_id = 0;
bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_txn_read_perc = 1.0 - TXN_WRITE_PERC;
double g_txn_write_perc = TXN_WRITE_PERC;
double g_tup_read_perc = 1.0 - TUP_WRITE_PERC;
double g_tup_write_perc = TUP_WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
double g_data_perc = DATA_PERC;
double g_access_perc = ACCESS_PERC;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_node_id = 0;
UInt32 g_node_cnt = NODE_CNT;
uint64_t g_net_id = 0;
UInt32 g_net_cnt = NET_CNT;
UInt32 g_net_size = NODE_CNT / NET_CNT;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_core_cnt = CORE_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt32 g_priconsensus_size = PRICONSENSUS_SIZE;
UInt32 g_merge_percent = MERGE_PERCENT;
uint64_t g_account_num = ACCOUNT_NUM;

#if EXECUTION_THREAD
UInt32 g_execute_thd = EXECUTE_THD_CNT;
#else
UInt32 g_execute_thd = 0;
#endif

#if SIGN_THREADS
UInt32 g_sign_thd = SIGN_THD_CNT;
#else
UInt32 g_sign_thd = 0;
#endif

UInt32 g_is_sharding = RING_BFT || SHARPER;

UInt32 g_rem_thread_cnt = REM_THREAD_CNT;
UInt32 g_send_thread_cnt = SEND_THREAD_CNT;
UInt32 g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt;
UInt32 g_total_client_thread_cnt = g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
UInt32 g_total_node_cnt = g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
UInt32 g_ycsb_column = YCSB_COLUMN;
UInt32 g_ycsb_write_ratio = YCSB_WRITE_RATIO;
bool g_strict_ppt = STRICT_PPT == 1;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;

UInt32 g_table_num = TABLE_NUM;

// Client Related Data.
UInt32 g_client_node_cnt = CLIENT_NODE_CNT;
UInt32 g_client_thread_cnt = CLIENT_THREAD_CNT;
UInt32 g_client_rem_thread_cnt = CLIENT_REM_THREAD_CNT;
UInt32 g_client_send_thread_cnt = CLIENT_SEND_THREAD_CNT;
UInt32 g_servers_per_client = 0;
UInt32 g_clients_per_server = 0;
uint64_t last_valid_txn = 0;
uint64_t g_client_send_rate = CLIENT_SEND_RATE;
uint64_t get_last_valid_txn()
{
	return last_valid_txn;
}

void set_last_valid_txn(uint64_t txn_id)
{
	last_valid_txn = txn_id;
}

UInt32 g_server_start_node = 0;
UInt32 g_this_thread_cnt = ISCLIENT ? g_client_thread_cnt : g_thread_cnt;
UInt32 g_this_rem_thread_cnt = ISCLIENT ? g_client_rem_thread_cnt : g_rem_thread_cnt;
UInt32 g_this_send_thread_cnt = ISCLIENT ? g_client_send_thread_cnt : g_send_thread_cnt;
UInt32 g_this_total_thread_cnt = ISCLIENT ? g_total_client_thread_cnt : g_total_thread_cnt;

UInt32 g_max_txn_per_part = MAX_TXN_PER_PART;
UInt32 g_network_delay = NETWORK_DELAY;
UInt64 g_done_timer = DONE_TIMER;
UInt64 g_seq_batch_time_limit = SEQ_BATCH_TIMER;
UInt64 g_prog_timer = PROG_TIMER;
UInt64 g_warmup_timer = WARMUP_TIMER;
UInt64 g_msg_time_limit = MSG_TIME_LIMIT;
UInt64 g_read_hot = READ_HOT;
UInt64 g_write_hot = WRITE_HOT;
double g_hotness = HOTNESS;

double g_mpr = MPR;
double g_mpitem = MPIR;

UInt32 g_repl_type = REPL_TYPE;
UInt32 g_repl_cnt = REPLICA_CNT;

/******** Key storage for signing. ***********/
//ED25519 and RSA
string g_priv_key;							   //stores this node's private key
string g_public_key;						   //stores this node's public key
string g_pub_keys[NODE_CNT + CLIENT_NODE_CNT]; //stores public keys of other nodes.
//CMAC
string cmacPrivateKeys[NODE_CNT + CLIENT_NODE_CNT];
string cmacOthersKeys[NODE_CNT + CLIENT_NODE_CNT];
//ED25519
CryptoPP::ed25519::Verifier verifier[NODE_CNT + CLIENT_NODE_CNT];
CryptoPP::ed25519::Signer signer;

// Receiving keys
uint64_t receivedKeys[NODE_CNT + CLIENT_NODE_CNT];

/*********************************************/

// Entities for ensuring successful key exchange.
std::mutex keyMTX;
bool keyAvail = false;
uint64_t totKey = 0;

uint64_t indexSize = 2 * g_client_node_cnt * g_inflight_max;
#if RING_BFT || SHARPER
uint64_t g_min_invalid_nodes = (g_shard_size - 1) / 3; //min number of valid nodes
#endif

#if MINI_NET
uint64_t g_min_invalid_nodes = 1;
#else
uint64_t g_min_invalid_nodes = (g_node_cnt/g_net_cnt - 1) / 3; //min number of valid nodes
#endif

#if SHARPER || RING_BFT
bool g_involved_shard[] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
UInt32 g_involved_shard_cnt = INVOLVED_SHARDS_NUMBER;
#endif

// Funtion to calculate hash of a string.
string calculateHash(string str)
{
	byte const *pData = (byte *)str.data();
	unsigned int nDataLen = str.size();
	byte aDigest[CryptoPP::SHA256::DIGESTSIZE];

	CryptoPP::SHA256().CalculateDigest(aDigest, pData, nDataLen);
	return std::string((char *)aDigest, CryptoPP::SHA256::DIGESTSIZE);
}

// Entities for maintaining g_next_index.
uint64_t g_next_index = 0; //index of the next txn to be executed
std::mutex gnextMTX;
void inc_next_index()
{
	gnextMTX.lock();
	g_next_index++;
	gnextMTX.unlock();
}
//[Dakai]
void inc_next_index(uint64_t val) {
	gnextMTX.lock();
	g_next_index += val;
	gnextMTX.unlock();	
}

uint64_t curr_next_index()
{
	uint64_t cval;
	gnextMTX.lock();
	cval = g_next_index;
	gnextMTX.unlock();
	return cval;
}

#if IN_RECV
uint64_t recv_try_cnt = 0;
uint64_t recv_fail_cnt = 0;
#endif

// Entities for handling checkpoints.
uint32_t g_last_stable_chkpt = 0; //index of the last stable checkpoint
uint32_t g_txn_per_chkpt = TXN_PER_CHKPT;
uint64_t last_deleted_txn_man = 0;

uint64_t txn_per_chkpt()
{
	return g_txn_per_chkpt;
}

void set_curr_chkpt(uint64_t txn_id)
{
	if(txn_id > g_last_stable_chkpt)
		g_last_stable_chkpt  = txn_id;
}

uint64_t get_curr_chkpt()
{
	return g_last_stable_chkpt;
}

uint64_t get_last_deleted_txn()
{
	return last_deleted_txn_man;
}

void inc_last_deleted_txn(uint64_t del_range)
{
	if(del_range > last_deleted_txn_man)
 		last_deleted_txn_man = del_range;
}

// Information about batching threads.
std::mutex g_checkpointing_lock;
uint g_checkpointing_thd = 2;
uint64_t txn_chkpt_holding[2] = {0};
bool is_chkpt_holding[2] = {false}; 
bool is_chkpt_stalled[2] = {false};
sem_t chkpt_semaphore[2];

uint64_t expectedExecuteCount = g_batch_size - 2;
uint64_t expectedCheckpoint = TXN_PER_CHKPT - 5;
uint64_t expectedExecuteNetid = 0;

uint64_t get_expectedExecuteCount()
{
	return expectedExecuteCount;
}

uint64_t get_expectedExecuteNetId()
{
	return expectedExecuteNetid;
}
void set_expectedExecuteNetid(uint64_t val)
{
	expectedExecuteNetid = val;
}

void set_expectedExecuteCount(uint64_t val)
{
#if NET_BROADCAST
	expectedExecuteCount = val;
	expectedExecuteNetid += 1;
	if (expectedExecuteNetid % g_net_cnt == 0)
	{
		expectedExecuteCount += get_batch_size();
		expectedExecuteNetid = 0;
	}
	else
	{
		;
	}
#else
	expectedExecuteCount = val + get_batch_size();
#endif
	
	//expectedExecuteNetid = expectedExecuteNetid % g_net_cnt;
#if SEMA_TEST
	//cout << "test_v4:set_expectedExecuteCount::execute_msg_heap_top = "<<execute_msg_heap_top().first<<" "<<execute_msg_heap_top().second<<"\n";
	execute_msg_heap_pop();	//remvoe the last executed msg from the heap
	// if (execute_msg_heap.empty() == true){
	// 	cout << "test_v4:set_expectedExecuteCount::execute_msg_heap_top_after_pop is empty" <<"\n";
	// }
	// else {
	// 	cout << "test_v4:set_expectedExecuteCount::execute_msg_heap_top_after_pop = "<<execute_msg_heap_top().first<<" "<<execute_msg_heap_top().second<<"\n";
	// }
	if(make_pair(expectedExecuteCount, expectedExecuteNetid) == execute_msg_heap_top()){	//check whether the next msg to execute has been in the heap 
		sem_post(&execute_semaphore);
	}
#endif
}

// Variable used by all threads during setup, to mark they are ready
std::mutex batchMTX;
std::mutex next_idx_lock;
uint commonVar = 0;

//[Dakai]
// Variable used by Input thread at the primary to linearize batches.
#if !MULTI_ON
uint64_t next_idx = 0;
uint64_t get_and_inc_next_idx()
{
	next_idx_lock.lock();
	uint64_t val = next_idx++;
	next_idx_lock.unlock();
	return val;
}

#else
//uint64_t next_idx = g_node_id;
  uint64_t next_idx = g_node_id / g_net_cnt;	//the first idx of an instance is the g_node_id of its primary
  uint64_t get_and_inc_next_idx() {
	uint64_t val;
	next_idx_lock.lock();
  	val = next_idx;
	next_idx += get_totInstances() / g_net_cnt;
	//next_idx += get_totInstances(); //In RCC, not increase by 1 but the number of instances
	next_idx_lock.unlock();
  	return val;
  }
#endif


void set_next_idx(uint64_t val)
{
	next_idx_lock.lock();
	next_idx = val;
	next_idx_lock.unlock();
}

// Counters for input threads to access next socket (only used by replicas).
uint64_t sock_ctr[REM_THREAD_CNT] = {0};
uint64_t get_next_socket(uint64_t tid, uint64_t size)
{
	uint64_t abs_tid = tid % g_this_rem_thread_cnt;
	uint64_t nsock = (sock_ctr[abs_tid] + 1) % size;
	sock_ctr[abs_tid] = nsock;
	return nsock;
}

/** Global Utility functions: **/

// Destination for msgs.
vector<uint64_t> nodes_to_send(uint64_t beg, uint64_t end)
{
	vector<uint64_t> dest;
	for (uint64_t i = beg; i < end; i++)
	{
		if (i == g_node_id)
		{
			continue;
		}
		//test:send to the same net
		if (i % g_net_cnt == g_net_id)
        {
            dest.push_back(i);
        }
		//dest.push_back(i);
	}
	return dest;
}

// STORAGE OF CLIENT DATA
uint64_t ClientDataStore[SYNTH_TABLE_SIZE] = {0};

// Entities for MULTI_ON
#if MULTI_ON

uint64_t totInstances = MULTI_INSTANCES;
uint64_t multi_threads = MULTI_THREADS;
uint64_t get_totInstances() {
      return totInstances;
}
uint64_t get_multi_threads() {
      return multi_threads;
}

uint64_t current_primaries[MULTI_INSTANCES];

void set_primary(uint64_t nid, uint64_t idx) {
	current_primaries[idx] = nid;
}

uint64_t get_primary(uint64_t idx) {
	return current_primaries[idx];
}	

bool primaries[NODE_CNT] = {false};

// Statically setting the first f+1 replicas as primary.
void initialize_primaries() {
	for(uint64_t i=0; i<get_totInstances(); i++) {
	  primaries[i] = true;
	}
}

bool isPrimary(uint64_t id) {
	return primaries[id];
}
#else
bool primaries[NODE_CNT] = {false};
void initialize_primaries() {
	  primaries[0] = true;
}

bool isPrimary(uint64_t id) {
	return primaries[id];
}

#endif // MUTLI_ON


#if SEMA_TEST
// Entities for semaphore optimizations. The value of the semaphores means 
// the number of msgs in the corresponding queues of the worker_threads.
// Only worker_threads with msgs in their queues will be allocated with CPU resources.
sem_t worker_queue_semaphore[THREAD_CNT];
#if CONSENSUS == HOTSTUFF
// new_txn_semaphore is the number of instances that a replica is primary and has not sent a prepare msg.
sem_t new_txn_semaphore;
#endif
// execute_semaphore is whether the next msg to execute has been in the queue.
sem_t execute_semaphore;
// Entities for semaphore opyimizations on output_thread. The output_thread will be not allocated
// with CPU resources until there is a msg in its queue.
sem_t output_semaphore[SEND_THREAD_CNT];
// Semaphore indicating whether the setup is done
sem_t setup_done_barrier;
uint64_t init_msg_sent[SEND_THREAD_CNT] = {0};
void dec_init_msg_sent(uint64_t td_id){
	init_msg_sent[td_id]--;
}
uint64_t get_init_msg_sent(uint64_t td_id ){
	return init_msg_sent[td_id];
}
void init_init_msg_sent(){
    for(uint i = 0; i < g_node_cnt; i++){
        if(i==g_node_id)
            continue;
        init_msg_sent[i%SEND_THREAD_CNT] += 3;
    }
}


// A min heap storing txn_id of execute_msgs
std::priority_queue<pair<uint64_t,uint64_t> , vector<pair<uint64_t,uint64_t>>, greater<pair<uint64_t,uint64_t>> > execute_msg_heap;
std::mutex execute_msg_heap_lock;

void execute_msg_heap_push(pair<uint64_t,uint64_t> txn_id_and_net_id){
	//cout << "test_v4:execute_msg_heap_push::txn_id_and_net_id = "<<txn_id_and_net_id.first<<" "<<txn_id_and_net_id.second<<"\n";
    execute_msg_heap_lock.lock();
    execute_msg_heap.push(txn_id_and_net_id);
    execute_msg_heap_lock.unlock();
	//cout << "test_v4:execute_msg_heap_top_after_push::txn_id_and_net_id = "<<execute_msg_heap_top().first<<" "<<execute_msg_heap_top().second<<"\n";
}
std::pair<uint64_t,uint64_t> execute_msg_heap_top(){
	std::pair<uint64_t,uint64_t> value = make_pair(0, 0);
	execute_msg_heap_lock.lock();
	value = execute_msg_heap.top();
	execute_msg_heap_lock.unlock();
    return value;
}
void execute_msg_heap_pop(){
    execute_msg_heap_lock.lock();
    execute_msg_heap.pop();
    execute_msg_heap_lock.unlock();
}

#endif


// returns the current view.
uint64_t get_current_view(uint64_t thd_id)
{
	return get_view(thd_id);
}

//[Dakai]
#if VIEW_CHANGES || KDK_DEBUG5
// For updating view of different threads.
std::mutex newViewMTX[THREAD_CNT + REM_THREAD_CNT + SEND_THREAD_CNT];
uint64_t newView[THREAD_CNT + REM_THREAD_CNT + SEND_THREAD_CNT] = {0};
uint64_t get_view(uint64_t thd_id)
{
	uint64_t nchange = 0;
	newViewMTX[thd_id].lock();
	nchange = newView[thd_id];
	newViewMTX[thd_id].unlock();
	return nchange;
}

void set_view(uint64_t thd_id, uint64_t val)
{
	newViewMTX[thd_id].lock();
	newView[thd_id] = val;
	newViewMTX[thd_id].unlock();
}
#endif

// Size of the batch
uint64_t g_batch_size = BATCH_SIZE;
uint64_t batchSet[2 * CLIENT_NODE_CNT * MAX_TXN_IN_FLIGHT];
uint64_t get_batch_size()
{
	return g_batch_size;
}
#if SHARPER
uint32_t last_commited_txn = 0;
UInt32 g_shard_size = SHARD_SIZE;
UInt32 g_shard_cnt = NODE_CNT / SHARD_SIZE;
SpinLockMap<string, int> digest_directory;

// This variable is mainly used by the client to know its current primary.
uint32_t g_view[NODE_CNT / SHARD_SIZE] = {0};
std::mutex viewMTX[NODE_CNT / SHARD_SIZE];
void set_client_view(uint64_t nview, int shard)
{
	viewMTX[shard].lock();
	g_view[shard] = nview;
	viewMTX[shard].unlock();
}
bool is_primary_node(uint64_t thd_id, uint64_t node)
{
	return view_to_primary(get_current_view(thd_id), node) == node;
}

uint64_t get_shard_number(uint64_t i)
{
	if (i >= g_node_cnt && i < g_node_cnt + g_client_node_cnt)
	{
		int client_number = i - g_node_cnt;
		return (client_number / (g_client_node_cnt / g_shard_cnt));
	}
	else
		return (i / g_shard_size);
}
uint64_t get_client_view(int shard)
{
	uint64_t val;
	viewMTX[shard].lock();
	val = g_view[shard];
	viewMTX[shard].unlock();
	return val;
}
uint64_t view_to_primary(uint64_t view, uint64_t node)
{
	return get_shard_number(node) * g_shard_size + view;
}
uint64_t next_set_id(uint64_t prev)
{
	return prev + g_shard_cnt;
}

int is_in_same_shard(uint64_t first_id, uint64_t second_id)
{
	return (int)(first_id / g_shard_size) == (int)(second_id / g_shard_size);
}
bool is_local_request(uint64_t txn_id)
{
	if ((txn_id / g_batch_size) % (g_shard_size) == g_node_id / g_shard_size)
		return true;
	else
		return false;
}
#elif RING_BFT
UInt32 g_shard_size = SHARD_SIZE;
UInt32 g_shard_cnt = NODE_CNT / SHARD_SIZE;
SpinLockMap<string, uint64_t> digest_directory;
SpinLockMap<string, CommitCertificateMessage *> ccm_directory;
SpinLockSet<string> rcm_checklist;
SpinLockSet<string> ccm_checklist;
// This variable is mainly used by the client to know its current primary.
uint32_t g_view[NODE_CNT / SHARD_SIZE] = {0};
std::mutex viewMTX[NODE_CNT / SHARD_SIZE];
void set_client_view(uint64_t nview, int shard)
{
	viewMTX[shard].lock();
	g_view[shard] = nview;
	viewMTX[shard].unlock();
}
bool is_primary_node(uint64_t thd_id, uint64_t node)
{
	return view_to_primary(get_current_view(thd_id), node) == node;
}

bool is_sending_ccm(uint64_t node_id)
{
	if (node_id % g_shard_size < g_min_invalid_nodes * 2 + 1)
		return true;
	else
		return false;
}
uint64_t sending_ccm_to(uint64_t node_id, uint64_t shard_id)
{
	uint64_t result = node_id;
	uint64_t source_shard = get_shard_number(node_id);
	if (source_shard < shard_id)
	{
		result = result + (shard_id - source_shard) * g_shard_size;
	}
	else if (source_shard > shard_id)
	{
		result = result - (source_shard - shard_id) * g_shard_size;
	}
	else
	{
		assert(0);
	}
	return result;
}
uint64_t get_shard_number(uint64_t i)
{
	if (i >= g_node_cnt && i < g_node_cnt + g_client_node_cnt)
	{
		int client_number = i - g_node_cnt;
		return (client_number / (g_client_node_cnt / g_shard_cnt));
	}
	else
		return (i / g_shard_size);
}
uint64_t get_client_view(int shard)
{
	uint64_t val;
	viewMTX[shard].lock();
	val = g_view[shard];
	viewMTX[shard].unlock();
	return val;
}
uint64_t view_to_primary(uint64_t view, uint64_t node)
{
	return get_shard_number(node) * g_shard_size + view;
}
uint64_t next_set_id(uint64_t prev)
{
	return prev + g_shard_cnt;
}

int is_in_same_shard(uint64_t first_id, uint64_t second_id)
{
	return (int)(first_id / g_shard_size) == (int)(second_id / g_shard_size);
}
bool is_local_request(TxnManager *tman)
{
	uint64_t node_id = tman->client_id;
	assert(node_id < g_total_node_cnt);
	int client_number = node_id - g_node_cnt;
	int primary = client_number / (g_client_node_cnt / g_shard_cnt);
	if (is_in_same_shard(primary * g_shard_size, g_node_id))
	{
		return true;
	}
	return false;
}
#else
// This variable is mainly used by the client to know its current primary.
uint32_t g_view = 0;
std::mutex viewMTX;
void set_client_view(uint64_t nview)
{
	viewMTX.lock();
	g_view = nview;
	viewMTX.unlock();
}

uint64_t get_client_view()
{
	uint64_t val;
	viewMTX.lock();
	val = g_view;
	viewMTX.unlock();
	return val;
}
#endif

#if LOCAL_FAULT || VIEW_CHANGES
// Server parameters for tracking failed replicas
std::mutex stopMTX[SEND_THREAD_CNT];
vector<vector<uint64_t>> stop_nodes; // List of nodes that have stopped.

// Client parameters for tracking failed replicas.
std::mutex clistopMTX;
vector<uint64_t> stop_replicas; // For client we assume only one O/P thread.
#endif

#if LOCAL_FAULT
uint64_t num_nodes_to_fail = NODE_FAIL_CNT;
#endif

//Statistics global print variables.
double idle_worker_times[THREAD_CNT] = {0};

// Statistics to print output_thread_idle_times.
double output_thd_idle_time[SEND_THREAD_CNT] = {0};
double input_thd_idle_time[REM_THREAD_CNT] = {0};

//Maps that stores messages of each type.
#if FIX_CL_INPUT_THREAD_BUG
std::mutex client_response_lock;
#endif
SpinLockMap<uint64_t, uint64_t> client_responses_count;
SpinLockMap<uint64_t, ClientResponseMessage *> client_responses_directory;

// Payload for messages.
#if PAYLOAD_ENABLE
#if PAYLOAD == M100
uint64_t payload_size = 12800;
#elif PAYLOAD == M200
uint64_t payload_size = 25600;
#elif PAYLOAD == M400
uint64_t payload_size = 51200;
#endif
#endif

#if EXT_DB == SQL || EXT_DB == SQL_PERSISTENT
DataBase *db = new SQLite();
#elif EXT_DB == MEMORY
DataBase *db = new InMemoryDB();
#endif

#if STRONG_SERIAL
sem_t consensus_lock;
#endif