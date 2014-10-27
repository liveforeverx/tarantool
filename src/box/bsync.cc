/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#define MH_SOURCE 1
#include "box/bsync.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "cfg.h"
#include "fio.h"
#include "coio.h"
#include "coio_buf.h"
#include "memory.h"
#include "scoped_guard.h"
#include "box/box.h"
#include "box/txn.h"
#include "box/port.h"
#include "box/schema.h"
#include "box/space.h"
#include "box/tuple.h"
#include "box/request.h"
#include "box/replication.h"
#include "msgpuck/msgpuck.h"

#include "box/bsync_hash.h"
#include "box/iproto_constants.h"

#define BSYNC_MAX_HOSTS VCLOCK_MAX
#define BSYNC_PAIR_TIMEOUT 30

#define BSYNC_TRACE {\
	say_debug("[%p] %s:%d current state: nconnected=%d, state=%d,"\
		" leader=%d, bsync_fibers=%ld, txn_fibers=%ld, bsync_active=%ld," \
		" txn_active=%ld", \
	fiber(), __PRETTY_FUNCTION__, __LINE__, bsync_state.num_connected, \
	bsync_state.state, (int)bsync_state.leader_id, \
	bsync_state.bsync_fibers.size, bsync_state.txn_fibers.size, \
	bsync_state.bsync_fibers.active, bsync_state.txn_fibers.active); \
}

static void* bsync_thread(void*);
static void bsync_process_fiber(va_list ap);
static void bsync_start_election();
static void bsync_disconnected(uint8_t host_id, bool recovery);
static void bsync_check_consensus(uint8_t host_id);
static void bsync_process(struct bsync_txn_info *info);
static void bsync_process_wal(struct bsync_txn_info *info);
static void txn_process(struct bsync_txn_info *info);
static void bsync_process_connect(struct bsync_txn_info *info);
static void bsync_process_disconnect(struct bsync_txn_info *info);
static void bsync_start_event(struct bsync_txn_info *info);
static void bsync_process_follow(struct bsync_txn_info *info);
static void bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info);

static struct recovery_state *local_state;

static struct ev_loop* txn_loop;
static struct ev_loop* bsync_loop;
static struct ev_async txn_process_event;
static struct ev_async bsync_process_event;

struct bsync_send_elem {/* for save in host queue */
	bool system;
	uint8_t code;
	struct bsync_operation *op;

	struct rlist list;
};

struct bsync_region {
	struct region pool;
	struct rlist list;
};

struct bsync_incoming {
	struct fiber *f;
	const struct tt_uuid *uuid;
	int remote_id;
	struct rlist list;
};

struct bsync_common {
	struct bsync_region *region;
	struct bsync_key *dup_key;
};

static struct bsync_system_status_ {
	uint8_t leader_id;
	uint8_t local_id;

	bool have_snapshot;
	bool started;
	bool wal_started;
	bool join[BSYNC_MAX_HOSTS];
	bool iproto[BSYNC_MAX_HOSTS];
	bool fail[BSYNC_MAX_HOSTS];
	bool recovery;
	bool recovery_mode;
	uint8_t id2index[BSYNC_MAX_HOSTS];
	pthread_mutex_t mutex[BSYNC_MAX_HOSTS];
	pthread_cond_t cond[BSYNC_MAX_HOSTS];
	struct fiber *snapshot_fiber;

	struct rlist incoming_connections;
	struct rlist wait_start;

	struct vclock vclock;
} system_state;

struct bsync_txn_info;
typedef void
(*bsync_request_f)(struct bsync_txn_info *);

struct bsync_txn_info { /* txn information about operation */
	struct xrow_header *row;
	struct fiber *owner;
	struct bsync_operation *op;
	struct bsync_common *common;
	uint8_t connection;
	int64_t sign;
	int result;
	bool repeat;
	bool proxy;
	bsync_request_f process;

	struct rlist list;
	STAILQ_ENTRY(bsync_txn_info) fifo;
};
STAILQ_HEAD(bsync_fifo, bsync_txn_info);

enum bsync_operation_status {
	bsync_op_status_init = 0,
	bsync_op_status_accept = 1,
	bsync_op_status_wal = 2,
	bsync_op_status_submit = 3,
	bsync_op_status_yield = 4
};

struct bsync_operation {
	uint64_t sign;
	uint8_t host_id;
	uint8_t status;
	uint8_t accepted;
	uint8_t rejected;

	struct fiber *owner;
	struct bsync_common *common;
	struct bsync_txn_info *txn_data;
	struct xrow_header *row;

	struct rlist list;
};

enum bsync_host_flags {
	bsync_host_active_write = 0x01,
	bsync_host_rollback = 0x02,
	bsync_host_reconnect_sleep = 0x04,
	bsync_host_ping_sleep = 0x08,
	bsync_host_follow_fast = 0x10
};

enum bsync_host_state {
	bsync_host_disconnected = 0,
	bsync_host_recovery = 1,
	bsync_host_follow = 2,
	bsync_host_connected = 3
};

struct bsync_host_data {
	const char *name;
	int remote_id;
	uint8_t state;
	uint8_t flags;
	bool fiber_out_fail;
	struct fiber *fiber_out;
	struct fiber *fiber_in;
	int64_t sign;
	int64_t commit_sign;
	int64_t submit_sign;
	int64_t follow_sign;

	ssize_t send_queue_size;
	struct rlist send_queue;
	ssize_t recovery_queue_size;
	struct rlist recovery_queue;
	ssize_t op_queue_size;
	struct rlist op_queue;
	struct mh_bsync_t *active_ops;

	struct bsync_txn_info sysmsg;
	struct bsync_send_elem ping_msg;
};
static struct bsync_host_data bsync_index[BSYNC_MAX_HOSTS];

struct bsync_fiber_cache {
	size_t size;
	size_t active;
	struct rlist data;
};

static struct bsync_state_ {
	uint8_t local_id;
	uint8_t leader_id;
	uint8_t accept_id;
	uint8_t num_hosts;
	uint8_t num_connected;
	uint8_t state;
	uint8_t num_accepted;
	uint64_t wal_commit_sign;
	uint64_t wal_rollback_sign;

	ev_tstamp read_timeout;
	ev_tstamp write_timeout;
	ev_tstamp operation_timeout;
	ev_tstamp ping_timeout;
	ev_tstamp submit_timeout;
	ev_tstamp election_timeout;
	ssize_t max_host_queue;

	uint8_t iproxy_host;
	uint64_t iproxy_sign;
	const char** iproxy_pos;
	const char* iproxy_end;

	struct rlist proxy_queue;
	struct rlist txn_queue;
	struct rlist execute_queue; // executing operations
	struct rlist commit_queue; // submitted operations

	struct bsync_fiber_cache txn_fibers;
	struct bsync_fiber_cache bsync_fibers;

	struct rlist election_ops;

	struct bsync_fifo txn_proxy_queue;
	struct bsync_fifo txn_proxy_input;

	struct bsync_fifo bsync_proxy_queue;
	struct bsync_fifo bsync_proxy_input;

	struct slab_cache txn_slabc;
	struct mempool region_pool;
	struct slab_cache bsync_slabc;
	struct mempool system_send_pool;
	struct rlist region_free;
	struct rlist region_gc;

	/* TODO : temporary hack for support system operations on proxy side */
	struct cord cord;
	pthread_mutex_t mutex;
	pthread_mutex_t active_ops_mutex;
	pthread_cond_t cond;
	struct bsync_txn_info sysmsg;
} bsync_state;

#define BSYNC_LOCAL bsync_index[bsync_state.local_id]
#define BSYNC_LEADER bsync_index[bsync_state.leader_id]
#define BSYNC_REMOTE bsync_index[host_id]
#define BSYNC_LOCK(M) \
	tt_pthread_mutex_lock(&M); \
	auto guard = make_scoped_guard([&]() { \
		tt_pthread_mutex_unlock(&M); \
	})

static struct fiber*
bsync_fiber(struct bsync_fiber_cache *lst, void (*f)(va_list), ...)
{
	if (lst->size > 1000) {
		if (lst == &bsync_state.bsync_fibers)
			say_info("big size of bsync_fiber list: size=%ld, active=%ld", lst->size, lst->active);
		else
			say_info("big size of txn_fiber list: size=%ld, active=%ld", lst->size, lst->active);
	}
	struct fiber *result = NULL;
	if (! rlist_empty(&lst->data)) {
		result = rlist_shift_entry(&lst->data, struct fiber, state);
	} else {
		BSYNC_TRACE
		result = fiber_new("bsync_proc", f);
		++lst->size;
	}
	++lst->active;
	return result;
}

static struct bsync_region *
bsync_new_region()
{BSYNC_TRACE
	BSYNC_LOCK(bsync_state.mutex);
	if (!rlist_empty(&bsync_state.region_free)) {
		return rlist_shift_entry(&bsync_state.region_free,
			struct bsync_region, list);
	} else {
		struct bsync_region* region = (struct bsync_region *)
			mempool_alloc0(&bsync_state.region_pool);
		region_create(&region->pool, &bsync_state.txn_slabc);
		return region;
	}
}

static void
bsync_free_region(struct bsync_common *data)
{BSYNC_TRACE
	if (!data->region)
		return;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_add_tail_entry(&bsync_state.region_gc, data->region, list);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	data->region = NULL;
}

static void
bsync_dump_region()
{
	struct rlist region_gc;
	rlist_create(&region_gc);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_swap(&region_gc, &bsync_state.region_gc);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	if (rlist_empty(&region_gc))
		return;
	struct bsync_region *next =
		rlist_first_entry(&region_gc, struct bsync_region, list);
	struct bsync_region *cur = NULL;
	while (!rlist_empty(&region_gc)) {
		cur = next;
		next = rlist_next_entry(cur, list);
		region_reset(&cur->pool);
		rlist_move_entry(&bsync_state.region_free, cur, list);
	}
	rlist_create(&region_gc);
}

static uint8_t
bsync_max_host()
{
	uint8_t max_host_id = 0;
	for (uint8_t i = 1; i < bsync_state.num_hosts; ++i) {
		if (bsync_index[i].sign >= bsync_index[max_host_id].sign &&
			bsync_index[i].state > bsync_host_disconnected)
		{
			max_host_id = i;
		}
	}
	return max_host_id;
}

enum bsync_message_type {
	bsync_mtype_leader_proposal = 0,
	bsync_mtype_leader_promise = 1,
	bsync_mtype_leader_accept = 2,
	bsync_mtype_leader_submit = 3,
	bsync_mtype_leader_reject = 4,
	bsync_mtype_ping = 5,
	bsync_mtype_iproto_switch = 6,
	bsync_mtype_bsync_switch = 7,
	bsync_mtype_close = 8,
	bsync_mtype_sysend = 9,
	bsync_mtype_body = 10,
	bsync_mtype_submit = 11,
	bsync_mtype_reject = 12,
	bsync_mtype_proxy_request = 13,
	bsync_mtype_proxy_accept = 14,
	bsync_mtype_proxy_reject = 15,
	bsync_mtype_proxy_join = 16,
	bsync_mtype_rollback = 17,
	bsync_mtype_count = 18,
	bsync_mtype_none = 19
};

enum bsync_machine_state {
	bsync_state_election = 0,
	bsync_state_initial = 1,
	bsync_state_promise = 2,
	bsync_state_accept = 3,
	bsync_state_recovery = 4,
	bsync_state_ready = 5,
	bsync_state_rollback = 6,
	bsync_state_shutdown = 7
};

enum bsync_iproto_flags {
	bsync_iproto_commit_sign = 0x01,
};

static const char* bsync_mtype_name[] = {
	"leader_proposal",
	"leader_promise",
	"leader_accept",
	"leader_submit",
	"leader_reject",
	"ping",
	"iproto_switch",
	"bsync_switch",
	"close",
	"INVALID",
	"body",
	"submit",
	"reject",
	"proxy_request",
	"proxy_accept",
	"proxy_reject",
	"proxy_join",
	"rollback",
	"INVALID",
	"INVALID"
};

#define SWITCH_TO_BSYNC(cb) {\
	info->process = cb; \
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.bsync_proxy_queue, fifo) { \
			assert (tmp != info); \
		} \
	} \
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_queue, info, fifo); \
	if (was_empty) ev_async_send(bsync_loop, &bsync_process_event); \
	tt_pthread_mutex_unlock(&bsync_state.mutex); }

#define SWITCH_TO_TXN(info, cb) {\
	(info)->process = cb; \
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.txn_proxy_queue, fifo) { \
			assert (tmp != info); \
		} \
	} \
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, info, fifo); \
	if (was_empty) ev_async_send(txn_loop, &txn_process_event); \
	tt_pthread_mutex_unlock(&bsync_state.mutex); }

static bool
bsync_begin_op(struct bsync_key *key, uint32_t server_id)
{BSYNC_TRACE
	if (!local_state->remote[system_state.leader_id].localhost)
		return true;
	/*
	 * TODO : special analyze for operations with spaces (remove/create)
	 */
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t keys[BSYNC_MAX_HOSTS];
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.state != bsync_host_connected ||
			host_id == system_state.local_id)
		{
			keys[host_id] = -1;
			continue;
		}
		keys[host_id] = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
		if (keys[host_id] == mh_end(BSYNC_REMOTE.active_ops))
			continue;
		struct mh_bsync_node_t *node =
			mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
		if (server_id != 0 && node->val.remote_id != server_id) {
			if (node->val.remote_id == 0) {
				say_error("conflict with operations from %s and %s"
						" in %s. space is %d",
					bsync_index[server_id - 1].name,
					BSYNC_LOCAL.name,
					BSYNC_REMOTE.name,
					key->space_id);
			} else {
				say_error("conflict with operations from %s and %s"
						" in %s. space is %d",
					bsync_index[server_id - 1].name,
					bsync_index[node->val.remote_id - 1].name,
					BSYNC_REMOTE.name,
					key->space_id);
			}
			assert(node->val.remote_id != 0 ||
				node->key.space_id < 512 ||
				node->key.data[0] != '4');
			return false;
		}
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (keys[host_id] == -1)
			continue;
		if (keys[host_id] != mh_end(BSYNC_REMOTE.active_ops)) {
			struct mh_bsync_node_t *node =
				mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
			if (server_id == 0) {
				++node->val.local_ops;
			} else {
				node->val.remote_id = server_id;
				++node->val.remote_ops;
			}
			node->key.data = key->data;
		} else {
			struct mh_bsync_node_t node;
			node.key = *key;
			if (server_id == 0) {
				node.val.local_ops = 1;
				node.val.remote_ops = 0;
			} else {
				node.val.local_ops = 0;
				node.val.remote_ops = 1;
			}
			node.val.remote_id = server_id;
			mh_bsync_put(BSYNC_REMOTE.active_ops, &node, NULL, NULL);
		}
	}
	return true;
}

static void
bsync_end_op(uint8_t host_id, struct bsync_key *key, uint32_t server_id)
{BSYNC_TRACE
	if (!local_state->remote[system_state.leader_id].localhost)
		return;
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t k = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
	if (k == mh_end(BSYNC_REMOTE.active_ops))
		return;

	struct mh_bsync_node_t *node = mh_bsync_node(BSYNC_REMOTE.active_ops, k);
	if (server_id != 0)
		--node->val.remote_ops;
	else
		--node->val.local_ops;
	if ((node->val.local_ops + node->val.remote_ops) == 0)
		mh_bsync_del(BSYNC_REMOTE.active_ops, k, NULL);
}

struct bsync_parse_data {
	uint32_t space_id;
	bool is_tuple;
	const char *data;
	const char *end;
};

static void
bsync_space_cb(void *d, uint8_t key, uint32_t v)
{
	if (key == IPROTO_SPACE_ID)
		((struct bsync_parse_data *) d)->space_id = v;
}

static void
bsync_tuple_cb(void *d, uint8_t key, const char *v, const char *vend)
{
	((struct bsync_parse_data *) d)->data = v;
	((struct bsync_parse_data *) d)->end = vend;
	((struct bsync_parse_data *) d)->is_tuple = (key == IPROTO_TUPLE);
}

#define BSYNC_MAX_KEY_PART_LEN 256
static void
bsync_parse_dup_key(struct bsync_common *data, struct key_def *key,
		    struct tuple *tuple)
{
	data->dup_key = (struct bsync_key *)
		region_alloc(&data->region->pool, sizeof(struct bsync_key));
	data->dup_key->size = 0;
	data->dup_key->data = (char *)region_alloc(&data->region->pool,
					BSYNC_MAX_KEY_PART_LEN * key->part_count);
	char *i_data = data->dup_key->data;
	for (uint32_t p = 0; p < key->part_count; ++p) {
		if (key->parts[p].type == NUM) {
			uint32_t v = tuple_field_u32(tuple, key->parts[p].fieldno);
			memcpy(i_data, &v, sizeof(uint32_t));
			data->dup_key->size += sizeof(uint32_t);
			i_data += sizeof(uint32_t);
			continue;
		}
		const char *key_part =
			tuple_field_cstr(tuple, key->parts[p].fieldno);
		size_t key_part_len = strlen(key_part);
		data->dup_key->size += key_part_len;
		memcpy(i_data, key_part, key_part_len + 1);
		i_data += data->dup_key->size;
	}
	data->dup_key->space_id = key->space_id;
}

static int
bsync_find_incoming(int id, struct tt_uuid *uuid)
{BSYNC_TRACE
	struct bsync_incoming *inc;
	rlist_foreach_entry(inc, &system_state.incoming_connections, list)
	{
		if (!tt_uuid_is_equal(uuid, inc->uuid))
			continue;
		if (id >= 0)
			inc->remote_id = id;
		fiber_call(inc->f);
		return inc->remote_id;
	}
	if (id < 0) {
		for (int i = 0; i < local_state->remote_size; ++i) {
			if (tt_uuid_is_equal(
				&local_state->remote[i].server_uuid, uuid)
				&& local_state->remote[i].switched)
			{
				BSYNC_TRACE
				return i;
			}
		}
	}
	inc = (struct bsync_incoming *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_incoming));
	inc->f = fiber();
	inc->uuid = uuid;
	inc->remote_id = id;
	rlist_add_entry(&system_state.incoming_connections, inc, list);
	fiber_yield_timeout(BSYNC_PAIR_TIMEOUT);
	rlist_del_entry(inc, list);
	if (inc->remote_id >= 0) {
		assert(!ev_is_active(&local_state->remote[inc->remote_id].out));
		say_info("connected with %s",
			local_state->remote[inc->remote_id].source);
	}
	return inc->remote_id;
}

void
bsync_push_connection(int id)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	assert(!ev_is_active(&local_state->remote[id].out));
	if (local_state->remote[id].localhost) {
		system_state.local_id = id;
		bsync_index[id].sysmsg.proxy = true;
		struct bsync_txn_info *info = &bsync_index[id].sysmsg;
		info->sign = vclock_signature(&local_state->vclock);
		SWITCH_TO_BSYNC(bsync_process_connect);
		return;
	}
	if (bsync_find_incoming(id, &local_state->remote[id].server_uuid) < 0)
		tnt_raise(ClientError, ER_NO_CONNECTION);
}

void
bsync_init_in(uint8_t host_id, int fd)
{
	coio_init(&local_state->remote[host_id].in);
	local_state->remote[host_id].in.fd = fd;
	local_state->remote[host_id].writer = fiber();
}

void
bsync_switch_2_election(uint8_t host_id, struct recovery_state *state)
{
	struct bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	info->proxy = false;
	if (state)
		info->sign = vclock_signature(&state->vclock);
	else
		info->sign = 0;
	system_state.iproto[host_id] = true;
	SWITCH_TO_BSYNC(bsync_process_connect);
	fiber_yield();
	system_state.iproto[host_id] = false;
}

bool
bsync_process_join(int fd, struct tt_uuid *uuid)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	local_state->remote[host_id].switched = false;
	assert(BSYNC_REMOTE.state == bsync_host_disconnected);
	bsync_init_in(host_id, fd);
	system_state.join[host_id] = true;
	bsync_switch_2_election(host_id, NULL);
	BSYNC_TRACE
	if (local_state->remote[system_state.leader_id].localhost) {
		if (system_state.have_snapshot) {
			system_state.join[host_id] = false;
			box_on_cluster_join(uuid);
		} else {
			box_on_cluster_join(uuid);
			fiber_yield();
			system_state.join[host_id] = false;
		}
		say_info("start sending snapshot to %s", tt_uuid_str(uuid));
		BSYNC_TRACE
		return true;
	} else
		system_state.join[host_id] = false;
	return false;
}

bool
bsync_process_subscribe(int fd, struct tt_uuid *uuid,
			struct recovery_state *state)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	bool switched = local_state->remote[host_id].switched;
	local_state->remote[host_id].switched = false;
	bsync_init_in(host_id, fd);
	system_state.id2index[state->server_id] = host_id;
	say_info("set host_id %d to server_id %d", host_id, state->server_id);
	if (system_state.leader_id < BSYNC_MAX_HOSTS && switched &&
		local_state->remote[system_state.leader_id].localhost)
	{
		BSYNC_TRACE
		return true;
	}
	bsync_switch_2_election(host_id, state);
	BSYNC_TRACE
	return local_state->remote[system_state.leader_id].localhost;
}

int
bsync_join()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return 0;
	if (system_state.leader_id < BSYNC_MAX_HOSTS) {
		system_state.recovery_mode = (system_state.leader_id
			!= system_state.local_id);
		return system_state.leader_id;
	}
	system_state.have_snapshot = false;
	system_state.recovery = true;
	fiber_yield();
	system_state.recovery = false;
	if (system_state.snapshot_fiber) {
		say_info("wait for finish snapshot generating");
		fiber_join(system_state.snapshot_fiber);
		system_state.snapshot_fiber = NULL;
	}
	BSYNC_TRACE
	system_state.recovery_mode = (system_state.leader_id < BSYNC_MAX_HOSTS);
	return system_state.leader_id;
}

int
bsync_subscribe()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return 0;
	if (system_state.leader_id < BSYNC_MAX_HOSTS) {
		system_state.recovery_mode =
			(system_state.leader_id != system_state.local_id);
		return system_state.leader_id;
	}
	BSYNC_TRACE
	system_state.recovery = true;
	fiber_yield();
	system_state.recovery = false;
	system_state.recovery_mode = (system_state.leader_id < BSYNC_MAX_HOSTS);
	return system_state.leader_id;
}

int
bsync_ready_subscribe()
{BSYNC_TRACE
	if (system_state.leader_id == BSYNC_MAX_HOSTS)
		return -1;
	system_state.recovery_mode = (system_state.leader_id
		!= system_state.local_id);
	say_info("start to recovery from %s",
		local_state->remote[system_state.leader_id].source);
	return system_state.leader_id;
}

static void
bsync_txn_leader(struct bsync_txn_info *info)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	system_state.leader_id = info->connection;
	if (system_state.have_snapshot &&
		local_state->remote[info->connection].localhost &&
		local_state->remote[system_state.leader_id].reader != NULL)
	{
		fiber_call(local_state->remote[system_state.leader_id].reader);
	}
}

static void
bsync_txn_join(struct bsync_txn_info * /*info*/)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	assert(!system_state.have_snapshot);
	system_state.snapshot_fiber = fiber_new("generate_initial_snapshot",
		(fiber_func) box_generate_initial_snapshot);
	fiber_set_joinable(system_state.snapshot_fiber, true);
	fiber_call(system_state.snapshot_fiber);
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (system_state.join[i])
			fiber_call(local_state->remote[i].writer);
	}
	fiber_call(system_state.snapshot_fiber);
	fiber_call(local_state->remote[system_state.leader_id].reader);
}

static void
bsync_process_recovery(struct bsync_txn_info *info)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	if (local_state->remote[system_state.leader_id].localhost) {
		uint8_t hi = info->connection;
		local_state->remote[hi].switched = true;
		if (system_state.join[hi] && system_state.snapshot_fiber != NULL)
			return;
		if (system_state.iproto[hi])
			fiber_call(local_state->remote[hi].writer);
		if (system_state.join[hi])
			fiber_call(local_state->remote[hi].writer);
	} else {
		if (!system_state.recovery) {
			recovery_follow_remote(local_state);
		}
		say_info("start recover from %s",
			local_state->remote[system_state.leader_id].source);
		if (system_state.recovery) {
			assert(local_state->remote[system_state.leader_id].reader);
			fiber_call(local_state->remote[system_state.leader_id].reader);
		}
	}
}

static void
bsync_process_close(struct bsync_txn_info *info)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	evio_close(loop(), &local_state->remote[info->connection].in);
	evio_close(loop(), &local_state->remote[info->connection].out);
	local_state->remote[info->connection].switched = false;
	local_state->remote[info->connection].connected = false;
	if (local_state->remote[system_state.leader_id].localhost)
		return;
	if (system_state.iproto[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
	if (system_state.join[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
}

static void
bsync_process_reconnnect(struct bsync_txn_info *info)
{
	if (info->proxy) {BSYNC_TRACE
		STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
		system_state.leader_id = BSYNC_MAX_HOSTS;
		for (int i = 0; i < local_state->remote_size; ++i) {
			if (local_state->remote[i].localhost)
				continue;
			if (evio_is_active(&local_state->remote[i].in))
				evio_close(loop(), &local_state->remote[i].in);
			if (evio_is_active(&local_state->remote[i].out))
				evio_close(loop(), &local_state->remote[i].out);
			local_state->remote[i].switched = false;
			fiber_call(local_state->remote[i].connecter);
		}
	} else {BSYNC_TRACE
		bsync_process_close(info);
		fiber_call(local_state->remote[info->connection].connecter);
	}
}

bool
bsync_follow(struct recovery_state * r)
{BSYNC_TRACE
	/* try to switch to normal mode */
	if (!local_state->bsync_remote || !local_state->finalize)
		return false;
	int host_id = system_state.id2index[r->server_id];
	assert(host_id < BSYNC_MAX_HOSTS);
	BSYNC_LOCK(system_state.mutex[host_id]);
	bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	info->sign = vclock_signature(&r->vclock);
	SWITCH_TO_BSYNC(bsync_process_follow);
	tt_pthread_cond_wait(&system_state.cond[host_id],
		&system_state.mutex[host_id]);
	return info->proxy;
}

static bool
bsync_recovery_check(struct recovery_state *r)
{
	uint8_t host_id = BSYNC_MAX_HOSTS;
	{
		BSYNC_LOCK(system_state.mutex[r->server_id]);
		if (!system_state.fail[r->server_id])
			return false;
		/* reconnect to host */
		uint8_t host_id = system_state.id2index[r->server_id];
		system_state.id2index[r->server_id] = BSYNC_MAX_HOSTS;
		system_state.fail[r->server_id] = false;
	}
	if (evio_is_active(&local_state->remote[host_id].in))
		evio_close(loop(), &local_state->remote[host_id].in);
	if (evio_is_active(&local_state->remote[host_id].out))
		evio_close(loop(), &local_state->remote[host_id].out);
	struct bsync_txn_info *info = &bsync_index[host_id].sysmsg;
	info->proxy = false;
	SWITCH_TO_BSYNC(bsync_process_disconnect);
	fiber_call(local_state->remote[host_id].connecter);
	return true;
}

void
bsync_recovery_stop(struct recovery_state *r)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	if (r && bsync_recovery_check(r))
		return;
	int host_id = system_state.leader_id;
	/* switch to normal mode */
	if (r) {
		host_id = system_state.id2index[r->server_id];
	} else {
		system_state.recovery_mode = false;
		r = local_state;
	}
	struct bsync_txn_info *info = &bsync_index[host_id].sysmsg;
	info->sign = vclock_signature(&r->vclock);
	say_debug("bsync_recovery_stop for %s",
		local_state->remote[host_id].source);
	info->proxy = (host_id == system_state.local_id);
	SWITCH_TO_BSYNC(bsync_process_connect);
	if (!system_state.started) {
		system_state.started = true;
		vclock_copy(&system_state.vclock, &local_state->vclock);
		struct fiber *f = NULL;
		while (!rlist_empty(&system_state.wait_start)) {
			f = rlist_shift_entry(&system_state.wait_start,
						struct fiber, state);
			fiber_call(f);
		}
	}
}

void
bsync_recovery_fail(struct recovery_state *r)
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	BSYNC_LOCK(system_state.mutex[r->server_id]);
	system_state.fail[r->server_id] = true;
}

void
bsync_leave_local_standby_mode()
{BSYNC_TRACE
	if (!local_state->bsync_remote)
		return;
	system_state.have_snapshot = true;
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (!local_state->remote[i].connected ||
			local_state->remote[i].localhost ||
			local_state->remote[i].writer == NULL)
			continue;
		fiber_call(local_state->remote[i].writer);
	}
}

static void bsync_outgoing(va_list ap);
static void bsync_accept_handler(va_list ap);

static void
bsync_fill_lsn(struct xrow_header *row)
{
	if (row->server_id == 0) {
		row->server_id = local_state->server_id;
		if (system_state.recovery_mode)
			row->lsn = vclock_inc(&local_state->vclock, row->server_id);
		else
			row->lsn = vclock_inc(&system_state.vclock, row->server_id);
	} else {
		if (system_state.recovery_mode)
			vclock_follow(&local_state->vclock, row->server_id, row->lsn);
		else
			vclock_follow(&system_state.vclock, row->server_id, row->lsn);
	}
}

int
bsync_write(struct recovery_state *r, struct txn_stmt *stmt)
try {BSYNC_TRACE
	assert(local_state == NULL || local_state == r);
	if (bsync_state.state == bsync_state_rollback) {
		if (stmt->row->server_id != 0) {
			rlist_shift(&bsync_state.txn_queue);
			fiber_yield();
		}
		return 0;
	}
	if (!local_state->bsync_remote
		|| (!system_state.wal_started && !system_state.started))
	{
		BSYNC_TRACE
		return wal_write_lsn(r, stmt->row);
	}
	if (system_state.wal_started && !system_state.started
		&& !system_state.recovery_mode)
	{
		BSYNC_TRACE
		say_info("add operation to wait queue, reason: %d,%d,%d",
			system_state.wal_started ? 1 : 0,
			system_state.started ? 1 : 0,
			system_state.recovery_mode ? 1 : 0);
		rlist_add_tail_entry(&system_state.wait_start, fiber(), state);
		fiber_yield();
		rlist_del_entry(fiber(), state);
	}
	assert(stmt->row->server_id > 0 || (stmt->row->commit_sn == 0 &&
						stmt->row->rollback_sn == 0));
	bsync_dump_region();

	stmt->row->tm = ev_now(loop());
	stmt->row->sync = 0;
	struct bsync_txn_info *info = NULL;
	uint32_t server_id = stmt->row->server_id;
	bsync_fill_lsn(stmt->row);
	say_debug("receive row %d:%ld", stmt->row->server_id, stmt->row->lsn);
	if (server_id == 0 || system_state.recovery_mode) { /* local request */
		info = (struct bsync_txn_info *) region_alloc(&fiber()->gc,
			sizeof(struct bsync_txn_info));
		info->common = (struct bsync_common *) region_alloc(
			&fiber()->gc, sizeof(struct bsync_common));
		info->common->region = bsync_new_region();
		info->row = stmt->row;
		info->op = NULL;
		info->result = -1;
		info->proxy = false;
		info->process = bsync_process;
		if (stmt->new_tuple) {
			bsync_parse_dup_key(info->common,
				stmt->space->index[0]->key_def,
				stmt->new_tuple);
		} else {
			bsync_parse_dup_key(info->common,
				stmt->space->index[0]->key_def,
				stmt->old_tuple);
		}
		bool begin_result = bsync_begin_op(info->common->dup_key, server_id);
		(void) begin_result;
		assert(begin_result);
	} else { /* proxy request */
		info = rlist_shift_entry(&bsync_state.txn_queue,
			struct bsync_txn_info, list);
		say_debug("send request %d:%ld to bsync",
			info->op->row->server_id, info->op->row->lsn);
	}
	info->sign = vclock_signature(&system_state.vclock);
	info->owner = fiber();
	bool recovery_mode = system_state.recovery_mode;
	auto vclock_guard = make_scoped_guard([&](){
		if (info->result < 0 || recovery_mode) {
			if (info->result < 0) {
				say_debug("row %d:%ld rejected",
					stmt->row->server_id,
					stmt->row->lsn);
			}
			return;
		}
		say_debug("row %d:%ld commited", stmt->row->server_id, stmt->row->lsn);
		vclock_follow(&local_state->vclock, stmt->row->server_id,
				stmt->row->lsn);
	});
	if (system_state.recovery_mode) {
		BSYNC_TRACE
		SWITCH_TO_BSYNC(bsync_process_wal);
		fiber_yield();
		for (uint8_t i = 0; i < local_state->remote_size; ++i)
			bsync_end_op(i, info->common->dup_key, server_id);
		say_debug("result of recovery operation is %d", info->result);
		return info->result;
	}
	rlist_add_tail_entry(&bsync_state.execute_queue, info, list);
	do {
		info->repeat = false;
		SWITCH_TO_BSYNC(bsync_process);
		assert((fiber()->flags & FIBER_IS_DEAD) == 0);
		fiber_yield();
		BSYNC_TRACE
		if (info->repeat)
			fiber_yield();
	} while (info->repeat);
	if (bsync_state.state != bsync_state_rollback) {
		rlist_del_entry(info, list);
		if (info->result >= 0) {
			info->repeat = false;
			return info->result;
		}
		tt_pthread_mutex_lock(&bsync_state.mutex);
		bsync_state.state = bsync_state_rollback;
		/* rollback in reverse order local operations */
		say_info("conflict detected, start to rollback");
		struct bsync_txn_info *s;
		rlist_foreach_entry_reverse(s, &bsync_state.execute_queue, list) {
			s->result = -1;
			fiber_call(s->owner);
		}
		rlist_create(&bsync_state.execute_queue);
		STAILQ_INIT(&bsync_state.bsync_proxy_input);
		STAILQ_INIT(&bsync_state.bsync_proxy_queue);
		say_info("start to reapply commited ops");
		struct bsync_operation *oper;
		rlist_foreach_entry(oper, &bsync_state.commit_queue, list) {
			fiber_call(oper->txn_data->owner);
		}
		say_info("finish rollback execution");
		if (system_state.leader_id == BSYNC_MAX_HOSTS)
			bsync_state.state = bsync_state_initial;
		tt_pthread_cond_signal(&bsync_state.cond);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
	}
	info->owner = NULL;
	return info->result;
}
catch (...) {
	say_crit("bsync_write found unhandled exception");
	throw;
}

static void
bsync_send_data(struct bsync_host_data *host, struct bsync_send_elem *elem)
{BSYNC_TRACE
	/* TODO : check all send ops for memory leak case */
	if (host->state == bsync_host_disconnected)
		return;
	assert(elem->code < bsync_mtype_count);
	say_debug("add message %s to send queue %s",
		bsync_mtype_name[elem->code], host->name);
	rlist_add_tail_entry(&host->send_queue, elem, list);
	++host->send_queue_size;
	if ((host->flags & bsync_host_active_write) == 0 &&
		host->fiber_out != fiber())
	{
		fiber_call(host->fiber_out);
	}
}

static int
bsync_wal_write(struct xrow_header *row)
{BSYNC_TRACE
	if (bsync_state.state == bsync_state_shutdown)
		return -1;
	if (bsync_state.wal_commit_sign) {
		row->commit_sn = bsync_state.wal_commit_sign;
		BSYNC_LOCAL.sign = bsync_state.wal_commit_sign;
		bsync_state.wal_commit_sign = 0;
	}
	if (bsync_state.wal_rollback_sign) {
		row->rollback_sn = bsync_state.wal_rollback_sign;
		bsync_state.wal_rollback_sign = 0;
	}
	return wal_write(local_state, row);
}

static void
bsync_rollback_slave(struct bsync_operation *oper)
{BSYNC_TRACE
	struct bsync_operation *op;
	oper->txn_data->process = txn_process;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_foreach_entry(op, &bsync_state.commit_queue, list)
	{
		op->txn_data->repeat = true;
	}
	uint8_t state = bsync_state.state;
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper->txn_data, fifo);
	if (was_empty)
		ev_async_send(txn_loop, &txn_process_event);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	bsync_state.state = state;

	struct bsync_send_elem *elem = (struct bsync_send_elem *) region_alloc0(
		&fiber()->gc, sizeof(struct bsync_send_elem));
	elem->code = bsync_mtype_proxy_join;
	bsync_send_data(&BSYNC_LEADER, elem);
}

static void
bsync_queue_slave(struct bsync_operation *oper)
{BSYNC_TRACE
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
	elem->code = bsync_mtype_proxy_request;
	elem->op = oper;
	oper->txn_data->result = 0;
	say_debug("start to proceed request %d:%ld", oper->row->server_id,
		oper->row->lsn);
	rlist_add_tail_entry(&bsync_state.proxy_queue, oper, list);
	bsync_send_data(&BSYNC_LEADER, elem);
	/* wait accept or reject */
	fiber_yield();
	BSYNC_TRACE
	if (oper->txn_data->result < 0) {
		say_debug("request %d:%ld rejected", oper->row->server_id,
			oper->row->lsn);
		bsync_rollback_slave(oper);
		return;
	}
	say_debug("request %d:%ld accepted", oper->row->server_id,
		oper->row->lsn);
	oper->status = bsync_op_status_wal;
	int wal_result = bsync_wal_write(oper->txn_data->row);
	elem->code = wal_result < 0 ? bsync_mtype_reject : bsync_mtype_submit;
	rlist_add_tail_entry(&bsync_state.commit_queue, oper, list);
	bsync_send_data(&BSYNC_LEADER, elem);
	if (oper->status == bsync_op_status_wal) {
		oper->status = bsync_op_status_yield;
		oper->txn_data->result = wal_result;
		fiber_yield();
		BSYNC_TRACE
	}
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	bsync_free_region(oper->common);
}

static void
bsync_wait_slow(struct bsync_operation *oper)
{BSYNC_TRACE
	if (2 * oper->accepted > bsync_state.num_hosts) {
		bsync_state.wal_commit_sign = oper->sign;
	}
	while ((oper->accepted + oper->rejected) < bsync_state.num_hosts) {
		oper->status = bsync_op_status_yield;
		fiber_yield();
		BSYNC_TRACE
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.commit_sign == BSYNC_REMOTE.submit_sign ||
			bsync_state.local_id == host_id ||
			(BSYNC_REMOTE.flags & bsync_host_active_write) != 0)
		{
			continue;
		}
		if (BSYNC_REMOTE.fiber_out)
			fiber_call(BSYNC_REMOTE.fiber_out);
	}
	BSYNC_TRACE
}

static void
bsync_check_follow(uint8_t host_id)
{
	struct bsync_send_elem *elem = NULL;
	if (BSYNC_REMOTE.follow_sign > 0) {
		if (BSYNC_REMOTE.recovery_queue_size >
			2 * bsync_state.max_host_queue)
		{
			BSYNC_REMOTE.follow_sign = 0;
		}
		else
			return;
	}
	if (BSYNC_REMOTE.recovery_queue_size < bsync_state.max_host_queue)
		return;
	ssize_t remove_count = BSYNC_REMOTE.recovery_queue_size -
					bsync_state.max_host_queue;
	if (remove_count > 0 && (BSYNC_REMOTE.flags & bsync_host_follow_fast) != 0)
		remove_count = BSYNC_REMOTE.recovery_queue_size;
	for (ssize_t i = 0; i < remove_count; ++i) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.recovery_queue,
					struct bsync_send_elem, list);
		--BSYNC_REMOTE.recovery_queue_size;
		bsync_do_reject(host_id, elem);
	}
}

static void
bsync_check_slow(uint8_t host_id)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		bsync_check_follow(host_id);
		return;
	}
	struct bsync_send_elem *elem = NULL;
	ssize_t queue_size =
		BSYNC_REMOTE.send_queue_size + BSYNC_REMOTE.op_queue_size;
	if (queue_size <= bsync_state.max_host_queue ||
		bsync_state.max_host_queue <= 0)
	{
		return;
	}
	rlist_foreach_entry(elem, &BSYNC_REMOTE.op_queue, list) {
		bsync_do_reject(host_id, elem);
	}
	rlist_foreach_entry(elem, &BSYNC_REMOTE.send_queue, list) {
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
	}
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	if (BSYNC_REMOTE.state == bsync_host_connected)
		bsync_disconnected(host_id, false);
	else
		BSYNC_REMOTE.follow_sign = 0;
}

static void
bsync_queue_leader(struct bsync_operation *oper, bool proxy)
{BSYNC_TRACE
	if (!proxy) {
		oper->row = (struct xrow_header *)
			region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
		xrow_copy(oper->txn_data->row, oper->row);
	}
	oper->status = bsync_op_status_init;
	say_debug("start to proceed request %d:%ld", oper->row->server_id,
		oper->row->lsn);
	oper->rejected = oper->accepted = 0;
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id ||
			BSYNC_REMOTE.state < bsync_host_follow)
		{
			if (host_id != bsync_state.local_id) {
				say_debug("reject operation for host %d", host_id);
				++oper->rejected;
			}
			continue;
		}
		struct bsync_send_elem *elem = (struct bsync_send_elem *)
			region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
		elem->op = oper;
		if (oper->host_id == host_id)
			elem->code = bsync_mtype_proxy_accept;
		else
			elem->code = bsync_mtype_body;
		if (BSYNC_REMOTE.state == bsync_host_connected) {
			bsync_send_data(&BSYNC_REMOTE, elem);
		} else {
			rlist_add_tail_entry(&BSYNC_REMOTE.recovery_queue,
					     elem, list);
			++BSYNC_REMOTE.recovery_queue_size;
		}
		bsync_check_slow(host_id);
	}
	oper->status = bsync_op_status_wal;
	oper->txn_data->result = bsync_wal_write(oper->txn_data->row);
	if (oper->txn_data->result < 0)
		++oper->rejected;
	else
		++oper->accepted;
	ev_tstamp start = ev_now(loop());
	oper->status = bsync_op_status_yield;
	while (2 * oper->accepted <= bsync_state.num_hosts) {
		if (ev_now(loop()) - start > bsync_state.operation_timeout ||
			2 * oper->rejected > bsync_state.num_hosts)
		{
			break;
		}
		fiber_yield_timeout(bsync_state.operation_timeout);
	}
	oper->txn_data->result = (
		2 * oper->accepted > bsync_state.num_hosts ? 0 : -1);
	say_debug("num_accepted=%d, num_rejected=%d", oper->accepted, oper->rejected);
	if (oper->txn_data->result < 0)
		exit(1);
//	assert(oper->txn_data->result >= 0);
	if (!proxy) {
		SWITCH_TO_TXN(oper->txn_data, txn_process);
		bsync_wait_slow(oper);
		bsync_free_region(oper->common);
	}
	say_debug("finish request %d:%ld", oper->row->server_id,
		oper->row->lsn);
}

static void
bsync_proxy_processor()
{BSYNC_TRACE
	struct bsync_operation *oper = (struct bsync_operation *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_operation));
	struct bsync_send_elem *send = (struct bsync_send_elem *)
		region_alloc0(&fiber()->gc, sizeof(bsync_send_elem));
	send->op = oper;
	oper->txn_data = (struct bsync_txn_info *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_txn_info));
	oper->txn_data->common = oper->common = (struct bsync_common *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_common));
	oper->txn_data->row = (struct xrow_header *)
		region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
	oper->txn_data->op = oper;
	oper->txn_data->result = 0;
	oper->txn_data->owner = NULL;
	oper->txn_data->repeat = false;
	oper->txn_data->proxy = true;
	oper->txn_data->process = bsync_process;
	xrow_header_decode(oper->txn_data->row, bsync_state.iproxy_pos,
			   bsync_state.iproxy_end);
	oper->txn_data->row->commit_sn = oper->txn_data->row->rollback_sn = 0;
	struct iovec xrow_body[XROW_BODY_IOVMAX];
	memcpy(xrow_body, oper->txn_data->row->body,
		sizeof(oper->txn_data->row->body));
	for (int i = 0; i < oper->txn_data->row->bodycnt; ++i) {
		oper->txn_data->row->body[i].iov_base = region_alloc(
			&fiber()->gc, xrow_body[i].iov_len);
		memcpy(oper->txn_data->row->body[i].iov_base,
			xrow_body[i].iov_base, xrow_body[i].iov_len);
	}
	oper->row = oper->txn_data->row;
	oper->host_id = bsync_state.iproxy_host;
	bsync_state.iproxy_host = BSYNC_MAX_HOSTS;
	bsync_state.iproxy_pos = NULL;
	bsync_state.iproxy_end = NULL;
	oper->owner = fiber();
	bool slave_proxy = (bsync_state.leader_id != bsync_state.local_id);
	if (slave_proxy) {
		oper->sign = bsync_state.iproxy_sign;
		oper->status = bsync_op_status_wal;
		say_debug("[%p] start to apply request  %d:%ld to WAL", fiber(),
			oper->row->server_id, oper->row->lsn);
		oper->txn_data->result = bsync_wal_write(oper->txn_data->row);
		if (oper->txn_data->result >= 0) {
			rlist_add_tail_entry(&bsync_state.commit_queue, oper, list);
			say_debug("submit request %d:%ld",
				  oper->row->server_id, oper->row->lsn);
		} else {
			say_debug("reject request %d:%ld",
				  oper->row->server_id, oper->row->lsn);
		}
		send->code = (oper->txn_data->result < 0 ?
				bsync_mtype_reject : bsync_mtype_submit);
		bsync_send_data(&BSYNC_LEADER, send);
	}
	if (oper->txn_data->result >= 0) {
		say_debug("start to apply request %d:%ld to TXN",
			oper->row->server_id, oper->row->lsn);
		SWITCH_TO_TXN(oper->txn_data, txn_process);
		fiber_yield();
		BSYNC_TRACE
	}
	if (slave_proxy) {
		if (oper->status != bsync_op_status_submit) {
			oper->status = bsync_op_status_yield;
			fiber_yield();
			BSYNC_TRACE
		}
		SWITCH_TO_TXN(oper->txn_data, txn_process);
	} else if (oper->txn_data->result < 0) {
		uint8_t host_id = oper->host_id;
		oper->sign = oper->txn_data->sign;
		if (BSYNC_REMOTE.flags & bsync_host_rollback)
			return;
		BSYNC_REMOTE.flags |= bsync_host_rollback;
		/* drop all active operations from host */
		tt_pthread_mutex_lock(&bsync_state.mutex);
		struct bsync_txn_info *info;
		STAILQ_FOREACH(info, &bsync_state.txn_proxy_input, fifo) {
			if (info->op->host_id == oper->host_id)
				info->result = -1;
		}
		STAILQ_FOREACH(info, &bsync_state.txn_proxy_queue, fifo) {
			if (info->row->server_id == oper->host_id)
				info->result = -1;
		}
		tt_pthread_cond_signal(&bsync_state.cond);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
		send->code = bsync_mtype_proxy_reject;
		bsync_send_data(&BSYNC_REMOTE, send);
	} else {
		oper->sign = oper->txn_data->sign;
		bsync_queue_leader(oper, true);
		bsync_wait_slow(oper);
		SWITCH_TO_TXN(oper->txn_data, txn_process);
	}
	fiber_yield();
	bsync_free_region(oper->common);
}

static void
bsync_proceed_rollback(struct bsync_txn_info *info)
{BSYNC_TRACE
	info->result = -1;
	SWITCH_TO_BSYNC(bsync_process);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
}

static struct bsync_send_elem *
bsync_alloc_send(uint8_t type)
{
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		mempool_alloc0(&bsync_state.system_send_pool);
	elem->code = type;
	elem->system = true;
	return elem;
}

/*
 * Command handlers block
 */

static void
bsync_body(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	bsync_state.iproxy_sign = mp_decode_uint(ipos);
	if (bsync_state.state == bsync_state_shutdown) {
		*ipos = iend;
		return;
	}
	assert(host_id == bsync_state.leader_id);
	bsync_state.iproxy_host = host_id;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
	assert(*ipos == iend);
}

static void
bsync_do_submit(uint8_t host_id, struct bsync_send_elem *info)
{BSYNC_TRACE
	++info->op->accepted;
	bsync_end_op(host_id, info->op->common->dup_key, info->op->host_id);
	BSYNC_REMOTE.submit_sign = info->op->sign;
	if (info->op->status == bsync_op_status_yield)
		fiber_call(info->op->owner);
}

static void
bsync_submit(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	bsync_do_submit(host_id,
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
			struct bsync_send_elem, list));
	BSYNC_TRACE
}

static void
bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info)
{BSYNC_TRACE
	say_debug("reject operation %d:%ld for host %d",
		info->op->row->server_id, info->op->row->lsn, host_id);
	++info->op->rejected;
	bsync_end_op(host_id, info->op->common->dup_key, info->op->host_id);
	if (info->op->status == bsync_op_status_yield)
		fiber_call(info->op->owner);
}

static void
bsync_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	bsync_do_reject(host_id,
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
			struct bsync_send_elem, list));
}

static void
bsync_proxy_request(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	if ((BSYNC_REMOTE.flags & bsync_host_rollback) != 0 ||
		bsync_state.state != bsync_state_ready)
	{
		/*
		 * skip all proxy requests from node in conflict state or
		 * if we lost consensus
		 */
		*ipos = iend;
		return;
	}
	bsync_state.iproxy_host = host_id;
	bsync_state.iproxy_pos = ipos;
	bsync_state.iproxy_end = iend;
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
	assert(*ipos == iend);
}

static void
bsync_proxy_accept(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	assert(!rlist_empty(&bsync_state.proxy_queue));
	struct bsync_operation *op = rlist_shift_entry(&bsync_state.proxy_queue,
							bsync_operation, list);
	op->sign = mp_decode_uint(ipos);
	for (host_id = 0; host_id < bsync_state.num_hosts; ++host_id)
		bsync_end_op(host_id, op->common->dup_key, op->host_id);
	assert(*ipos == iend);
	fiber_call(op->owner);
}

static void
bsync_proxy_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) host_id; (void) ipos; (void) iend;
	assert(!rlist_empty(&bsync_state.proxy_queue));
	struct bsync_operation *op =
		rlist_shift_entry(&bsync_state.proxy_queue,
				  bsync_operation, list);
	for (host_id = 0; host_id < bsync_state.num_hosts; ++host_id)
		bsync_end_op(host_id, op->common->dup_key, op->host_id);
	assert(*ipos == iend);
	op->txn_data->result = -1;
	fiber_call(op->owner);
}

static void
bsync_proxy_join(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	*ipos = iend;
	BSYNC_REMOTE.flags &= ~bsync_host_rollback;
}

static void
bsync_commit(uint64_t commit_sign)
{BSYNC_TRACE
	struct bsync_operation *op = NULL;
	do {
		assert(!rlist_empty(&bsync_state.commit_queue));
		op = rlist_shift_entry(&bsync_state.commit_queue,
			bsync_operation, list);
		if (op->status == bsync_op_status_wal) {
			op->status = bsync_op_status_submit;
		} else {
			assert(op->status == bsync_op_status_yield);
			fiber_call(op->owner);
		}
	} while (op->sign < commit_sign);
	bsync_state.wal_commit_sign = commit_sign;
}

static void
bsync_rollback(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) host_id;
	(void) ipos;
	(void) iend;
	assert(bsync_state.local_id != bsync_state.leader_id);
	uint64_t gsn = mp_decode_uint(ipos);
	(void) gsn;
	/* TODO:
	 * 1. rollback from proxy queue
	 * 2. rollback from commit queue
	 * 3. push commit point to WAL
	 * 4. push rollback point to WAL
	 */
}

/*
 * Fibers block
 */

static void
bsync_election_ops();

static void
bsync_txn_proceed_request(struct bsync_txn_info *info)
{BSYNC_TRACE
	struct request req;
	request_create(&req, info->row->type);
	request_decode(&req, (const char*) info->row->body[0].iov_base,
		info->row->body[0].iov_len);
	req.header = info->row;
	info->op->common->region = bsync_new_region();
	struct bsync_parse_data data;
	request_header_decode(info->row, bsync_space_cb, bsync_tuple_cb, &data);
	struct space *space = space_cache_find(data.space_id);
	assert(space && space->index_count > 0 &&
		space->index[0]->key_def->iid == 0);
	struct tuple *tuple = NULL;
	if (data.is_tuple) {
		tuple = tuple_new(space->format, data.data, data.end);
		space_validate_tuple(space, tuple);
	} else {
		const char *key = req.key;
		uint32_t part_count = mp_decode_array(&key);
		tuple = space->index[0]->findByKey(key, part_count);
		assert(tuple || local_state->remote[system_state.leader_id].localhost);
		if (!tuple) {
			bsync_proceed_rollback(info);
			return;
		}
	}
	TupleGuard guard(tuple);
	bsync_parse_dup_key(info->common, space->index[0]->key_def, tuple);
	if (bsync_begin_op(info->common->dup_key, info->op->host_id)) {
		rlist_add_tail_entry(&bsync_state.txn_queue, info, list);
		try {
			box_process(&req, &null_port);
		} catch (Exception *e) {
			if (info->op->host_id != system_state.leader_id)
				throw;
			say_debug("row %d:%ld generate exception",
				  info->row->server_id, info->row->lsn);
			info->owner = fiber();
			fiber_yield_timeout(bsync_state.operation_timeout);
			assert(info->repeat);
		}
	} else {
		BSYNC_TRACE
		bsync_proceed_rollback(info);
	}
}

static void
bsync_txn_process_fiber(va_list /* ap */)
{
restart: BSYNC_TRACE
	struct bsync_txn_info *info = STAILQ_FIRST(
		&bsync_state.txn_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	if (info->result < 0) {
		SWITCH_TO_BSYNC(bsync_process);
	} else {
		do {
			try {
				bsync_txn_proceed_request(info);
			} catch (Exception *e) {
				say_error(
					"bsync cant proceed request, reason: %s",
					e->errmsg());
				abort();
			}
			fiber_gc();
			if (info->repeat)
				info->repeat = false;
			else
				break;
		} while (true);
		if (info->proxy) {
			SWITCH_TO_BSYNC(bsync_process);
		}
	}
	assert(bsync_state.txn_fibers.active > 0);
	--bsync_state.txn_fibers.active;
	rlist_add_tail_entry(&bsync_state.txn_fibers.data, fiber(), state);
	fiber_yield();
	BSYNC_TRACE
	goto restart;
}

static void
bsync_txn_process(ev_loop * /*loop*/, ev_async */*watcher*/, int /*event*/)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.txn_proxy_input,
		&bsync_state.txn_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	while (!STAILQ_EMPTY(&bsync_state.txn_proxy_input)) {
		struct bsync_txn_info *info = STAILQ_FIRST(
			&bsync_state.txn_proxy_input);
		if (info->owner) {BSYNC_TRACE
			STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
			fiber_call(info->owner);
		} else {BSYNC_TRACE
			info->process(info);
		}
	}
}

static void
txn_process(struct bsync_txn_info */*info*/)
{
	fiber_call(bsync_fiber(&bsync_state.txn_fibers, bsync_txn_process_fiber));
}

static void
bsync_process_fiber(va_list /* ap */)
{
restart:BSYNC_TRACE
	struct bsync_txn_info *info = NULL;
	struct bsync_operation *oper = NULL;
	if (bsync_state.state == bsync_state_shutdown)
		goto exit;
	if (bsync_state.iproxy_end) {
		assert(bsync_state.state == bsync_state_ready ||
			bsync_state.state == bsync_state_recovery);
		bsync_proxy_processor();
		goto exit;
	}
	info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	oper = (struct bsync_operation *) region_alloc(&fiber()->gc,
		sizeof(struct bsync_operation));
	oper->common = (struct bsync_common *) region_alloc(&fiber()->gc,
		sizeof(struct bsync_common));
	oper->txn_data = info;
	oper->txn_data->op = oper;
	oper->sign = oper->txn_data->sign;
	oper->common->dup_key = oper->txn_data->common->dup_key;
	oper->common->region = oper->txn_data->common->region;
	oper->owner = fiber();
	oper->host_id = bsync_state.local_id;
	if (bsync_state.state != bsync_state_ready) {
		rlist_add_tail_entry(&bsync_state.election_ops, oper, list);
		fiber_yield_timeout(bsync_state.operation_timeout);
		if (bsync_state.state != bsync_state_ready) {
			BSYNC_TRACE
			info->result = -1;
			info->op = NULL;
			SWITCH_TO_TXN(oper->txn_data, txn_process);
			goto exit;
		}
	}
	if (bsync_state.leader_id == bsync_state.local_id) {
		bsync_queue_leader(oper, false);
	} else {
		oper->row = oper->txn_data->row;
		bsync_queue_slave(oper);
	}
exit:
	--bsync_state.bsync_fibers.active;
	BSYNC_TRACE
	fiber_gc();
	BSYNC_TRACE
	rlist_add_tail_entry(&bsync_state.bsync_fibers.data, fiber(), state);
	fiber_yield();
	goto restart;
}

static void
bsync_shutdown_fiber(va_list /* ap */)
{BSYNC_TRACE
	if (bsync_state.wal_commit_sign > 0 ||
		bsync_state.wal_rollback_sign > 0)
	{
		struct xrow_header *row = (struct xrow_header *)
			region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
		row->type = IPROTO_WAL_FLAG;
		bsync_wal_write(row);
		fiber_gc();
	}
	BSYNC_TRACE
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_close));
	}
	BSYNC_TRACE
	bsync_state.state = bsync_state_shutdown;
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		if ((bsync_index[i].flags & bsync_host_ping_sleep) != 0)
			fiber_call(bsync_index[i].fiber_out);
		bsync_index[i].state = bsync_host_disconnected;
	}
	BSYNC_TRACE
	recovery_delete(local_state);
	ev_break(bsync_loop, 1);
	BSYNC_TRACE
}

static void
bsync_shutdown(struct bsync_txn_info * /* info */)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	fiber_call(fiber_new("bsync_shutdown", bsync_shutdown_fiber));
}

static void
bsync_process_wal_fiber(va_list ap)
{BSYNC_TRACE
	struct bsync_txn_info * info = va_arg(ap, struct bsync_txn_info *);
	info->result = bsync_wal_write(info->row);
	bsync_free_region(info->common);
	SWITCH_TO_TXN(info, txn_process);
	fiber_gc();
}

static void
bsync_process_wal(struct bsync_txn_info * info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	fiber_start(fiber_new("bsync_wal", bsync_process_wal_fiber), info);
}

static void
bsync_process(struct bsync_txn_info * /* info */)
{BSYNC_TRACE
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
}

static bool
bsync_has_consensus()
{
	return 2 * bsync_state.num_connected > bsync_state.num_hosts
		&& bsync_state.local_id < BSYNC_MAX_HOSTS;
}

static void
bsync_stop_io(uint8_t host_id, bool close)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	BSYNC_REMOTE.state = bsync_host_disconnected;
	--bsync_state.num_connected;
	if (BSYNC_REMOTE.fiber_out && BSYNC_REMOTE.fiber_out != fiber() &&
		(BSYNC_REMOTE.flags & bsync_host_ping_sleep) != 0)
	{
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
	if (BSYNC_REMOTE.fiber_in && BSYNC_REMOTE.fiber_in != fiber()) {
		BSYNC_REMOTE.fiber_in = NULL;
	}
	if (BSYNC_REMOTE.fiber_out && BSYNC_REMOTE.fiber_out != fiber()) {
		say_debug("wait for outgoing connection to %s flags is %d",
			  BSYNC_REMOTE.name, BSYNC_REMOTE.flags);
		fiber_join(BSYNC_REMOTE.fiber_out);
	}
	if (close) {
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, bsync_process_close);
	}
}

static void
bsync_stop_io_fiber(va_list ap)
{
	uint8_t host_id = va_arg(ap, int);
	bool close = va_arg(ap, int);
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, close);
}

static void
bsync_start_event(struct bsync_txn_info * /* info */)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	ev_async_start(local_state->writer->txn_loop,
		&local_state->writer->write_event);
}

static void
bsync_cleanup_follow(uint8_t host_id, uint64_t sign)
{
	while (BSYNC_REMOTE.recovery_queue_size > 0) {
		struct bsync_send_elem *elem =
			rlist_shift_entry(&BSYNC_REMOTE.recovery_queue,
				struct bsync_send_elem, list);
		if (elem->op->sign <= sign) {
			--BSYNC_REMOTE.recovery_queue_size;
			bsync_do_submit(host_id, elem);
		} else {
			rlist_add_entry(&BSYNC_REMOTE.recovery_queue, elem, list);
			break;
		}
	}
}

static void
bsync_process_connect(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	uint8_t host_id = info->connection;
	say_info("bsync_process_connect with %s, signature is %ld",
		BSYNC_REMOTE.name, info->sign);
	BSYNC_REMOTE.sign = BSYNC_REMOTE.commit_sign =
		BSYNC_REMOTE.submit_sign = info->sign;
	bool do_switch = true;
	if (info->proxy) {
		say_info("set local_id to %d (%s)", host_id, BSYNC_REMOTE.name);
		bsync_state.local_id = host_id;
		if (BSYNC_LOCAL.state == bsync_host_connected)
			return;
	} else if ((bsync_state.leader_id != bsync_state.local_id &&
		bsync_state.leader_id == host_id &&
		bsync_state.state == bsync_state_recovery &&
		bsync_state.leader_id < BSYNC_MAX_HOSTS) ||
		(bsync_state.leader_id == bsync_state.local_id &&
			BSYNC_REMOTE.follow_sign > 0))
	{
		uint8_t num_connected = bsync_state.num_connected;
		if (BSYNC_REMOTE.state == bsync_host_disconnected)
			++num_connected;
		if (2 * num_connected > bsync_state.num_hosts &&
			bsync_state.state < bsync_state_ready)
		{
			bsync_state.state = bsync_state_ready;
		}
		do_switch = false;
	} else if (bsync_state.leader_id == bsync_state.local_id &&
		BSYNC_REMOTE.state == bsync_host_follow)
	{
		uint64_t follow_sign = 0;
		if (!BSYNC_REMOTE.recovery_queue_size > 0) {
			follow_sign =
				rlist_first_entry(&BSYNC_REMOTE.recovery_queue,
					struct bsync_send_elem, list)->op->sign;
		}
		if (info->sign > (follow_sign - 1))
			do_switch = false;
	}
	if (!do_switch) {
		BSYNC_REMOTE.follow_sign = 0;
		BSYNC_REMOTE.state = bsync_host_connected;
	}
	auto guard = make_scoped_guard([&]() {
		bsync_start_election();
		bsync_election_ops();
	});
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		++bsync_state.num_connected;
	if (info->proxy) {
		BSYNC_REMOTE.state = bsync_host_connected;
		return;
	} else if (do_switch) {
		BSYNC_REMOTE.state = bsync_host_recovery;
	}
	assert(host_id != bsync_state.local_id && host_id < BSYNC_MAX_HOSTS);
	bool has_leader = bsync_state.leader_id < BSYNC_MAX_HOSTS;
	bool local_leader = has_leader &&
		bsync_state.leader_id == bsync_state.local_id;
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	say_debug("push to bsync loop out_fd=%d, in_fd=%d",
		local_state->remote[host_id].out.fd,
		local_state->remote[host_id].in.fd);
	assert(!ev_is_active(&local_state->remote[host_id].in));
	assert(!ev_is_active(&local_state->remote[host_id].out));
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_outgoing),
			local_state->remote[host_id].out.fd, host_id);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_accept_handler),
			local_state->remote[host_id].in.fd, host_id);
	if (has_leader && !local_leader && host_id != bsync_state.leader_id) {
		fiber_start(fiber_new("bsync_stop_io", bsync_stop_io_fiber),
			host_id, 1);
	}
	if (!local_leader)
		return;
	/* TODO : support hot recovery from send queue */
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_leader_submit));
	if (do_switch) {
		bsync_send_data(&BSYNC_REMOTE,
				bsync_alloc_send(bsync_mtype_iproto_switch));
	} else {
		bsync_cleanup_follow(host_id, info->sign);
		while (!rlist_empty(&BSYNC_REMOTE.recovery_queue)) {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.recovery_queue,
						struct bsync_send_elem, list);
			bsync_send_data(&BSYNC_REMOTE, elem);
		}
		BSYNC_REMOTE.recovery_queue_size = 0;
		BSYNC_REMOTE.state = bsync_host_connected;
	}
}

static void
bsync_process_disconnect(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	int host_id = info->connection;
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		BSYNC_REMOTE.follow_sign = 0;
		while (!rlist_empty(&BSYNC_REMOTE.recovery_queue)) {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.recovery_queue,
						struct bsync_send_elem, list);
			bsync_do_reject(host_id, elem);
		}
		BSYNC_REMOTE.recovery_queue_size = 0;
	}
	--bsync_state.num_connected;
	if (bsync_state.local_id == bsync_state.leader_id &&
		2 * bsync_state.num_connected <= bsync_state.num_hosts)
	{
		bsync_state.leader_id = BSYNC_MAX_HOSTS;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
		bsync_state.state = bsync_state_initial;
		BSYNC_REMOTE.flags = 0;
	}
	BSYNC_REMOTE.state = bsync_host_disconnected;
}

static void
bsync_process_follow(struct bsync_txn_info *info)
{BSYNC_TRACE
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	int host_id = info->connection;
	uint64_t cleanup_sign = 0;
	tt_pthread_mutex_lock(&system_state.mutex[host_id]);
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	if (BSYNC_LOCAL.sign <= info->sign) {
		info->proxy = true;
	} else if (BSYNC_REMOTE.recovery_queue_size == 0) {
		info->proxy = false;
	} else {
		uint64_t last_sign =
			rlist_last_entry(&BSYNC_REMOTE.recovery_queue,
					struct bsync_send_elem, list)->op->sign;
		info->proxy = (last_sign <= info->sign);
	}
	if (info->proxy) {
		BSYNC_REMOTE.follow_sign = info->sign;
		BSYNC_REMOTE.state = bsync_host_follow;
	} else {
		say_debug("dont switch to sync because of %ld < %ld, queue size is %ld",
			info->sign, BSYNC_LOCAL.sign, BSYNC_REMOTE.recovery_queue_size);
		BSYNC_REMOTE.state = bsync_host_follow;
	}
	cleanup_sign = info->sign;
	tt_pthread_cond_signal(&system_state.cond[host_id]);
	tt_pthread_mutex_unlock(&system_state.mutex[host_id]);
	bsync_cleanup_follow(host_id, cleanup_sign);
}

static void
bsync_process_loop(struct ev_loop */*loop*/, ev_async */*watcher*/, int /*event*/)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.bsync_proxy_input,
		&bsync_state.bsync_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	struct bsync_txn_info *info = NULL;
	while (!STAILQ_EMPTY(&bsync_state.bsync_proxy_input)) {
		BSYNC_TRACE
		info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
		if (info->op) {
			BSYNC_TRACE
			STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input,
				fifo);
			if (bsync_state.state != bsync_state_ready)
				continue;
			fiber_call(info->op->owner);
		} else {
			info->process(info);
		}
	}
}

/*
 * send leader ID to TXN
 * if we dont have snapshot start to generate and send it
 * if we have snapshot check for SLAVE for ready state
 * if SLAVE is ready switch it to ready state
 * else switch it to recovery state, send iproto_switch and switch to TXN
 */

static void
bsync_set_leader(uint8_t host_id)
{BSYNC_TRACE
	say_info("new leader are %s", BSYNC_REMOTE.name);
	bsync_state.leader_id = host_id;
	bsync_state.sysmsg.connection = host_id;
	SWITCH_TO_TXN(&bsync_state.sysmsg, bsync_txn_leader);
	if (BSYNC_LOCAL.sign == 0) {
		bsync_state.state = bsync_state_recovery;
		if (bsync_state.leader_id == bsync_state.local_id) {
			SWITCH_TO_TXN(&BSYNC_LOCAL.sysmsg, bsync_txn_join);
		}
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			if (bsync_state.leader_id != bsync_state.local_id) {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_close));
			} else {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_iproto_switch));
			}
		}
		return;
	}
	if (host_id != bsync_state.local_id) {
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			bsync_send_data(&bsync_index[i],
				bsync_alloc_send(bsync_mtype_close));
		}
		if (BSYNC_REMOTE.sign > BSYNC_LOCAL.sign || BSYNC_LOCAL.sign == 0) {
			bsync_state.state = bsync_state_recovery;
		} else {
			bsync_state.state = bsync_state_ready;
			bsync_election_ops();
		}
		return;
	}
	/* start recovery */
	uint8_t num_up = 1;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		if (bsync_index[i].sign == BSYNC_LOCAL.sign) {
			++num_up;
			continue;
		}
		/* stop bsync io fibers and switch host to recovery state */
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_iproto_switch));
	}
	say_info("%s num_up = %d", __PRETTY_FUNCTION__, num_up);
	if (2 * num_up > bsync_state.num_hosts) {
		bsync_state.state = bsync_state_ready;
		bsync_election_ops();
	} else {
		bsync_state.state = bsync_state_recovery;
	}
}

/*
 * Leader election block
 */

static void
bsync_start_election()
{BSYNC_TRACE
	if (bsync_state.leader_id < BSYNC_MAX_HOSTS || !bsync_has_consensus()
		|| bsync_state.state == bsync_state_recovery)
		return;
	if (bsync_state.state == bsync_state_election) {
		if (bsync_state.num_connected == bsync_state.num_hosts)
			bsync_state.state = bsync_state_initial;
		else
			return;
	}
	assert(bsync_state.state < bsync_state_recovery);
	say_info("consensus connected");
	uint8_t max_host_id = bsync_max_host();
	say_info("next leader should be %s", bsync_index[max_host_id].name);
	if (max_host_id != bsync_state.local_id) {
		bsync_send_data(&bsync_index[max_host_id],
			bsync_alloc_send(bsync_mtype_leader_proposal));
		return;
	}
	bsync_state.num_accepted = 1;
	if (bsync_state.state == bsync_state_promise)
		return;
	else
		bsync_state.state = bsync_state_promise;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == max_host_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_promise));
	}
}

static void
bsync_check_consensus(uint8_t host_id)
{BSYNC_TRACE
	BSYNC_REMOTE.sysmsg.proxy = false;
	if (host_id != bsync_state.leader_id &&
		(bsync_state.state >= bsync_state_recovery ||
			2 * bsync_state.num_connected > bsync_state.num_hosts) &&
		(bsync_state.local_id != bsync_state.leader_id ||
			2 * bsync_state.num_connected > bsync_state.num_hosts))
	{
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, bsync_process_reconnnect);
		return;
	}
	BSYNC_TRACE
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	bsync_state.state = bsync_state_initial;
	BSYNC_REMOTE.sysmsg.proxy = true;
	while (!rlist_empty(&bsync_state.election_ops)) {
		struct bsync_operation *oper =
			rlist_shift_entry(&bsync_state.election_ops,
				struct bsync_operation, list);
		fiber_call(oper->owner);
	}
	/* drop all recovery queue */
	struct bsync_send_elem *elem = NULL;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		while (!rlist_empty(&bsync_index[i].recovery_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].recovery_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].op_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].op_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].send_queue)) {
			elem =rlist_shift_entry(&bsync_index[i].send_queue,
						struct bsync_send_elem, list);
			switch (elem->code) {
			case bsync_mtype_body:
			case bsync_mtype_proxy_accept:
				bsync_do_reject(i, elem);
				break;
			case bsync_mtype_rollback:
				/* TODO */
				break;
			}
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
		}
		bsync_index[i].flags &= ~bsync_host_follow_fast;
		bsync_index[i].follow_sign = 0;
		bsync_index[i].op_queue_size = 0;
		bsync_index[i].send_queue_size = 0;
		bsync_index[i].recovery_queue_size = 0;
		bsync_stop_io(i, false);
	}
	SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, bsync_process_reconnnect);
}

static void
bsync_disconnected(uint8_t host_id, bool recovery)
{BSYNC_TRACE
	if (BSYNC_REMOTE.state == bsync_host_disconnected ||
		BSYNC_REMOTE.state == bsync_host_follow ||
		bsync_state.state == bsync_state_shutdown)
		return;
	bsync_stop_io(host_id, false);
	if (host_id == bsync_state.accept_id) {
		bsync_state.state = bsync_state_initial;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
	}
	{
		BSYNC_LOCK(bsync_state.active_ops_mutex);
		mh_bsync_clear(BSYNC_REMOTE.active_ops);
	}
	/* TODO : clean up wait queue using fiber_call() */
	say_warn("disconnecting host %s", BSYNC_REMOTE.name);
	rlist_create(&BSYNC_REMOTE.recovery_queue);
	BSYNC_REMOTE.recovery_queue_size = 0;
	struct bsync_send_elem *elem;
	bool do_reject = (bsync_state.local_id != bsync_state.leader_id ||
			  !recovery);
	if (do_reject) {
		rlist_foreach_entry(elem, &BSYNC_REMOTE.op_queue, list) {
			bsync_do_reject(host_id, elem);
		}
	} else {
		rlist_swap(&BSYNC_REMOTE.op_queue, &BSYNC_REMOTE.recovery_queue);
		BSYNC_REMOTE.recovery_queue_size = BSYNC_REMOTE.op_queue_size;
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 struct bsync_send_elem, list);
		switch (elem->code) {
		case bsync_mtype_body:
		case bsync_mtype_proxy_accept:
			if (do_reject)
				bsync_do_reject(host_id, elem);
			else {
				assert(!elem->system);
				rlist_add_tail_entry(&BSYNC_REMOTE.recovery_queue,
							elem, list);
				++BSYNC_REMOTE.recovery_queue_size;
			}
			break;
		case bsync_mtype_rollback:
			/* TODO */
			break;
		}
		if (elem->system) {
			mempool_free(&bsync_state.system_send_pool, elem);
		}
	}
	if (!rlist_empty(&BSYNC_REMOTE.recovery_queue)) {
		BSYNC_REMOTE.state = bsync_host_follow;
		BSYNC_REMOTE.flags |= bsync_host_follow_fast;
	}
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	if (host_id == bsync_state.leader_id) {
		if (!rlist_empty(&bsync_state.proxy_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.proxy_queue,
				struct bsync_operation, list);
			oper->txn_data->result = -1;
			fiber_call(oper->owner);
			rlist_create(&bsync_state.proxy_queue);
		}
	} else if (bsync_state.local_id == bsync_state.leader_id) {
		while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
			elem =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
						struct bsync_send_elem, list);
			switch (elem->code) {
			case bsync_mtype_body:
			case bsync_mtype_proxy_accept:
				bsync_do_reject(host_id, elem);
				break;
			case bsync_mtype_rollback:
				/* TODO */
				break;
			}
			if (elem->system) {
				mempool_free(&bsync_state.system_send_pool, elem);
			}
		}
	}
	bsync_check_consensus(host_id);
	bsync_start_election();
}

static void
bsync_leader_proposal(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	say_info("receive leader proposal from %s", BSYNC_REMOTE.name);
	if (bsync_state.state != bsync_state_promise) {
		if (bsync_state.state == bsync_state_election)
			bsync_state.state = bsync_state_initial;
		bsync_start_election();
	} else if (bsync_state.state >= bsync_state_recovery &&
		bsync_state.state < bsync_state_shutdown &&
		bsync_state.leader_id == bsync_state.local_id)
	{
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
}

static void
bsync_leader_promise(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	uint8_t max_host_id = bsync_max_host();
	if (bsync_state.state >= bsync_state_recovery
		&& bsync_state.state < bsync_state_shutdown
		&& bsync_state.leader_id == bsync_state.local_id) {
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	} else if (host_id != max_host_id || bsync_state.state > bsync_state_initial) {
		say_warn("reject leader promise from %s, next leader should be %s",
		BSYNC_REMOTE.name, bsync_index[max_host_id].name);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_reject));
	} else {
		say_info("accept leader promise from %s", BSYNC_REMOTE.name);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_accept));
		bsync_state.state = bsync_state_accept;
		bsync_state.accept_id = host_id;
	}
}

static void
bsync_election_ops()
{BSYNC_TRACE
	while (!rlist_empty(&bsync_state.election_ops)
		&& bsync_state.state == bsync_state_ready) {
		struct bsync_operation *oper = rlist_shift_entry(
			&bsync_state.election_ops, struct bsync_operation,
			list);
		fiber_call(oper->owner);
	}
}

static void
bsync_leader_accept(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	if (bsync_state.state != bsync_state_promise)
		return;
	if (2 * ++bsync_state.num_accepted <= bsync_state.num_hosts)
		return;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
	bsync_set_leader(bsync_state.local_id);
}

static void
bsync_leader_submit(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	if (host_id != bsync_state.leader_id)
		bsync_set_leader(host_id);
}

static void
bsync_leader_reject(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign =
		BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void
bsync_ping(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.sign = mp_decode_uint(ipos);
}

static void
bsync_iproto_switch(uint8_t host_id, const char **ipos, const char *iend)
{BSYNC_TRACE
	(void) ipos; (void) iend;
	if (bsync_state.leader_id != bsync_state.local_id) {
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_iproto_switch));
	}
	bsync_stop_io(host_id, false);
	if (BSYNC_REMOTE.fiber_out_fail) {
		bsync_check_consensus(host_id);
	} else {
		BSYNC_REMOTE.state = bsync_host_recovery;
		++bsync_state.num_connected;
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, bsync_process_recovery);
	}
}

static void
bsync_close(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) ipos; (void) iend;
	if (BSYNC_REMOTE.state != bsync_host_disconnected && BSYNC_REMOTE.fiber_out)
		bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, false);
}

/*
 * Network block
 */

/*
 * Leader election queue:
 *
 * 1. possible SLAVE to LEADER: proposal
 * 2. possible LEADER to SLAVE: promise
 * 3. possible SLAVE to LEADER: accept or reject
 * 4. accepted LEADER to SLAVE: submit
 * 5. recovery/join variants:
 *
 * if leader dont have enough information for hot warmup slave:
 * 6. LEADER to SLAVE: iproto_switch
 * 7. SLAVE to LEADER: iproto_switch
 * 8. LEADER switch SLAVE connections to JOIN or SUBSCRIBE
 *
 * else
 * 6. LEADER start to send send_queue
 *
 * In case of SLAVE connected to exists consensus:
 * 1. SLAVE ignore any messages from new host
 * 2. LEADER send to host submit
 * 3. LEADER switch to p.5
 * 4. SLAVE switch to p.5
 */
typedef void
(*bsync_handler_t)(uint8_t host_id, const char** ipos, const char* iend);

static bsync_handler_t bsync_handlers[] = {
	bsync_leader_proposal,
	bsync_leader_promise,
	bsync_leader_accept,
	bsync_leader_submit,
	bsync_leader_reject,
	bsync_ping,
	bsync_iproto_switch,
	NULL,
	bsync_close,
	NULL,
	bsync_body,
	bsync_submit,
	bsync_reject,
	bsync_proxy_request,
	bsync_proxy_accept,
	bsync_proxy_reject,
	bsync_proxy_join,
	bsync_rollback
};

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov);

static void
bsync_decode_extended_header(uint8_t /* host_id */, const char **pos)
{
	uint32_t len = mp_decode_uint(pos);
	const char *end = *pos + len;
	if (!len)
		return;
	uint32_t flags = mp_decode_uint(pos);
	assert(flags);
	if (flags & bsync_iproto_commit_sign)
		bsync_commit(mp_decode_uint(pos));
	/* ignore all unknown data from extended header */
	*pos = end;
}

static void
bsync_breadn(struct ev_io *coio, struct ibuf *in, size_t sz)
{
	coio_breadn_timeout(coio, in, sz, bsync_state.read_timeout);
	if (errno == ETIMEDOUT) {
		tnt_raise(SocketError, coio->fd, "timeout");
	}
}

static uint32_t
bsync_read_package(struct ev_io *coio, struct ibuf *in, uint8_t host_id, bool tm)
{
	/* Read fixed header */
	if (ibuf_size(in) < 1) {
		if (tm) {BSYNC_TRACE
			bsync_breadn(coio, in, 1);
		} else
			coio_breadn(coio, in, 1);
	}
	/* Read length */
	if (mp_typeof(*in->pos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	if (tm) {BSYNC_TRACE}
	ssize_t to_read = mp_check_uint(in->pos, in->end);
	if (to_read > 0)
		bsync_breadn(coio, in, to_read);
	if (tm) {BSYNC_TRACE}
	uint32_t len = mp_decode_uint((const char **) &in->pos);
	if (len == 0)
		return 0;
	/* Read header and body */
	to_read = len - ibuf_size(in);
	if (to_read > 0)
		bsync_breadn(coio, in, to_read);
	if (tm) {BSYNC_TRACE}
	const char *pos = (const char *)in->pos;
	bsync_decode_extended_header(host_id, (const char **) &in->pos);
	return len - (in->pos - pos);
}

static void
bsync_incoming(struct ev_io *coio, struct iobuf *iobuf, uint8_t host_id)
{BSYNC_TRACE
	struct ibuf *in = &iobuf->in;
	while (BSYNC_REMOTE.state > bsync_host_disconnected &&
		bsync_state.state != bsync_state_shutdown)
	{
		/* cleanup buffer */
		iobuf_reset(iobuf);
		fiber_gc();
		uint32_t len = bsync_read_package(coio, in, host_id, false);
		/* proceed message */
		const char* iend = (const char *)in->pos + len;
		const char **ipos = (const char **)&in->pos;
		if (BSYNC_REMOTE.fiber_in != fiber())
			return;
		if (BSYNC_REMOTE.state > bsync_host_disconnected) {
			uint32_t type = mp_decode_uint(ipos);
			assert(type < bsync_mtype_count);
			say_debug("receive message from %s, type %s, length %d",
				BSYNC_REMOTE.name, bsync_mtype_name[type], len);
			assert(type < sizeof(bsync_handlers));
			if (type == bsync_mtype_close &&
				(bsync_state.local_id == bsync_state.leader_id ||
				host_id == bsync_state.leader_id))
			{
				BSYNC_TRACE
				bsync_disconnected(host_id, true);
			}
			(*bsync_handlers[type])(host_id, ipos, iend);
			if (type == bsync_mtype_iproto_switch)
				break;
		} else {
			*ipos = iend;
			say_warn("receive message from disconnected host %s",
			BSYNC_REMOTE.name);
		}
	}
}

static void
bsync_accept_handler(va_list ap)
{BSYNC_TRACE
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	struct iobuf *iobuf = iobuf_new(BSYNC_REMOTE.name);
	auto iobuf_guard = make_scoped_guard([&]() {
		iobuf_delete(iobuf);
	});
	BSYNC_REMOTE.fiber_in = fiber();
	fiber_set_joinable(fiber(), true);
	struct bsync_send_elem *elem = bsync_alloc_send(bsync_mtype_bsync_switch);
	auto elem_guard = make_scoped_guard([&](){
		mempool_free(&bsync_state.system_send_pool, elem);
	});
	struct iovec iov;
	encode_sys_request(host_id, elem, &iov);
	try {
		ssize_t total_sent = coio_writev_timeout(&coio, &iov, 1, -1,
							bsync_state.write_timeout);
		if (errno == ETIMEDOUT) {
			tnt_raise(SocketError, coio.fd, "timeout");
		}
		fiber_gc();
		say_info("sent to %s bsync_switch, num bytes is %ld",
			 BSYNC_REMOTE.name, total_sent);
		bsync_incoming(&coio, iobuf, host_id);
	} catch (Exception *e) {
		if (fiber() == BSYNC_REMOTE.fiber_in) {
			e->log();
			BSYNC_TRACE
			bsync_disconnected(host_id, true);
		}
	}
	BSYNC_REMOTE.fiber_in = NULL;
	say_debug("stop incoming fiber from %s", BSYNC_REMOTE.name);
}

static int
bsync_extended_header_size(uint8_t host_id)
{
	if (BSYNC_REMOTE.submit_sign > BSYNC_REMOTE.commit_sign)
		return mp_sizeof_uint(bsync_iproto_commit_sign) +
			mp_sizeof_uint(BSYNC_REMOTE.submit_sign);
	else
		return 0;
}

static char *
bsync_extended_header_encode(uint8_t host_id, char *pos)
{
	pos = mp_encode_uint(pos, bsync_extended_header_size(host_id));
	if (BSYNC_REMOTE.submit_sign > BSYNC_REMOTE.commit_sign) {
		pos = mp_encode_uint(pos, bsync_iproto_commit_sign);
		pos = mp_encode_uint(pos, BSYNC_REMOTE.submit_sign);
		BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.submit_sign;
	}
	return pos;
}

static uint64_t
bsync_mp_real_size(uint64_t size)
{
	return mp_sizeof_uint(size) + size;
}

static void
bsync_writev(struct ev_io *coio, struct iovec *iov, int iovcnt, uint8_t host_id)
{
	BSYNC_REMOTE.flags |= bsync_host_active_write;
	auto guard = make_scoped_guard([&]() {
		BSYNC_REMOTE.flags &= ~bsync_host_active_write;
	});
	coio_writev_timeout(coio, iov, iovcnt, -1, bsync_state.write_timeout);
	if (errno == ETIMEDOUT) {
		tnt_raise(SocketError, coio->fd, "timeout");
	}
}

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov)
{
	if (elem->code == bsync_mtype_none) {
		elem->code = bsync_mtype_ping;
	}
	ssize_t size = mp_sizeof_uint(elem->code)
		+ bsync_mp_real_size(bsync_extended_header_size(host_id));
	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
		size += mp_sizeof_uint(BSYNC_LOCAL.sign);
	/* no break */
	default:
		break;
	}
	iov->iov_len = mp_sizeof_uint(size) + size;
	iov->iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char *pos = (char *) iov[0].iov_base;
	pos = mp_encode_uint(pos, size);
	pos = bsync_extended_header_encode(host_id, pos);
	pos = mp_encode_uint(pos, elem->code);

	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
		pos = mp_encode_uint(pos, BSYNC_LOCAL.sign);
	/* no break */
	default:
		break;
	}
	return 1;
}

static int
encode_request(uint8_t host_id, struct bsync_send_elem *elem, struct iovec *iov)
{
	int iovcnt = 1;
	ssize_t bsize = bsync_mp_real_size(bsync_extended_header_size(host_id))
		+ mp_sizeof_uint(elem->code);
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join)
	{
		bsize += mp_sizeof_uint(elem->op->sign);
	}
	ssize_t fsize = bsize;
	if (elem->code == bsync_mtype_body || elem->code == bsync_mtype_proxy_request) {
		iovcnt += xrow_header_encode(elem->op->row, iov + 1);
		for (int i = 1; i < iovcnt; ++i) {
			bsize += iov[i].iov_len;
		}
	}
	iov[0].iov_len = mp_sizeof_uint(bsize) + fsize;
	iov[0].iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char* pos = (char*) iov[0].iov_base;
	pos = mp_encode_uint(pos, bsize);
	pos = bsync_extended_header_encode(host_id, pos);
	pos = mp_encode_uint(pos, elem->code);
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join)
	{
		pos = mp_encode_uint(pos, elem->op->sign);
	}
	return iovcnt;
}

#define BUFV_IOVMAX 200*XROW_IOVMAX
static void
bsync_send(struct ev_io *coio, uint8_t host_id)
{BSYNC_TRACE
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		struct iovec iov[BUFV_IOVMAX];
		int iovcnt = 0;
		do {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
						  struct bsync_send_elem, list);
			--BSYNC_REMOTE.send_queue_size;
			if (elem->code == bsync_mtype_body ||
				elem->code == bsync_mtype_proxy_accept)
			{
				++BSYNC_REMOTE.op_queue_size;
				rlist_add_tail_entry(&BSYNC_REMOTE.op_queue,
						elem, list);
			}
			assert(elem->code < bsync_mtype_count);
			say_debug("send to %s message with type %s, lsn %ld",
				BSYNC_REMOTE.name, bsync_mtype_name[elem->code],
				elem->op ? elem->op->row->lsn : -1);
			if (elem->code < bsync_mtype_sysend)
				iovcnt += encode_sys_request(host_id, elem,
							     iov + iovcnt);
			else
				iovcnt += encode_request(host_id, elem,
							 iov + iovcnt);
			bool stop = (elem->code == bsync_mtype_iproto_switch);
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
			if (stop) {
				while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
					elem = rlist_shift_entry(
						&BSYNC_REMOTE.send_queue,
						struct bsync_send_elem, list);
					assert(elem->system);
					mempool_free(&bsync_state.system_send_pool, elem);
				}
				break;
			}
		} while (!rlist_empty(&BSYNC_REMOTE.send_queue) &&
			 (iovcnt + XROW_IOVMAX) < BUFV_IOVMAX);
		bsync_writev(coio, iov, iovcnt, host_id);
	}
}

static void
bsync_outgoing(va_list ap)
{BSYNC_TRACE
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	BSYNC_REMOTE.fiber_out = fiber();
	fiber_set_joinable(fiber(), true);
	ev_tstamp prev = ev_now(loop());
	BSYNC_REMOTE.fiber_out_fail = false;
	say_debug("start outgoing to %s, fd=%d", BSYNC_REMOTE.name, coio.fd);
	try {
		{
			BSYNC_TRACE
			const char *source = recovery->remote[host_id].source;
			struct iobuf *iobuf = iobuf_new(source);
			BSYNC_REMOTE.flags |= bsync_host_active_write;
			BSYNC_TRACE
			auto iobuf_guard = make_scoped_guard([&]() {
				BSYNC_REMOTE.flags &= ~bsync_host_active_write;
				iobuf_delete(iobuf);
				fiber_gc();
			});
			BSYNC_TRACE
			bsync_read_package(&coio, &iobuf->in, host_id, true);
			uint32_t type = mp_decode_uint((const char **)&iobuf->in.pos);
			if (type != bsync_mtype_bsync_switch) {
				tnt_raise(SocketError, coio.fd, "incorrect handshake");
			}
			say_info("receive from %s bsync_switch", BSYNC_REMOTE.name);
		}
		while (BSYNC_REMOTE.state > bsync_host_disconnected &&
			bsync_state.state != bsync_state_shutdown)
		{
			if (bsync_extended_header_size(host_id) > 0 &&
				rlist_empty(&BSYNC_REMOTE.send_queue) &&
				bsync_state.state != bsync_state_shutdown)
			{
				fiber_yield_timeout(bsync_state.submit_timeout);
			}
			ev_tstamp now = ev_now(loop());
			while (bsync_state.state == bsync_state_ready &&
				(now - prev) < 0.001)
			{
				fiber_yield_timeout(0.001 + prev - now);
				now = ev_now(loop());
			}
			bool was_empty = rlist_empty(&BSYNC_REMOTE.send_queue);
			bsync_send(&coio, host_id);
			prev = ev_now(loop());
			fiber_gc();
			if (was_empty && BSYNC_REMOTE.state > bsync_host_disconnected &&
				bsync_state.state != bsync_state_shutdown)
			{
				if (BSYNC_REMOTE.state == bsync_host_connected) {
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					fiber_yield_timeout(bsync_state.ping_timeout);
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
					if (BSYNC_REMOTE.send_queue_size != 0)
						continue;
					bsync_send_data(&BSYNC_REMOTE, &BSYNC_REMOTE.ping_msg);
				} else {
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					fiber_yield();
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
				}
			}
		}
	} catch (Exception *e) {
		if (fiber() == BSYNC_REMOTE.fiber_out) {
			e->log();
			BSYNC_REMOTE.fiber_out_fail = true;
			BSYNC_TRACE
			bsync_disconnected(host_id, true);
		}
	}
	BSYNC_REMOTE.fiber_out = NULL;
	say_debug("stop outgoing fiber to %s", BSYNC_REMOTE.name);
}

/*
 * System block:
 * 1. initialize local variables;
 * 2. read cfg;
 * 3. start/stop cord and event loop
 */

static void
bsync_cfg_push_host(uint8_t host_id, const char *source)
{
	BSYNC_REMOTE.name = source;
	BSYNC_REMOTE.fiber_in = NULL;
	BSYNC_REMOTE.fiber_out = NULL;
	BSYNC_REMOTE.sign = 0;
	BSYNC_REMOTE.submit_sign = 0;
	BSYNC_REMOTE.commit_sign = 0;
	BSYNC_REMOTE.follow_sign = 0;
	BSYNC_REMOTE.flags = 0;
	BSYNC_REMOTE.state = bsync_host_disconnected;
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	rlist_create(&BSYNC_REMOTE.recovery_queue);
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	BSYNC_REMOTE.recovery_queue_size = 0;
	BSYNC_REMOTE.active_ops = mh_bsync_new();
	if (BSYNC_REMOTE.active_ops == NULL)
	panic("out of memory");
	memset(&BSYNC_REMOTE.sysmsg, 0, sizeof(struct bsync_txn_info));
	BSYNC_REMOTE.sysmsg.connection = host_id;
	BSYNC_REMOTE.ping_msg.code = bsync_mtype_ping;
	BSYNC_REMOTE.ping_msg.op = NULL;
	BSYNC_REMOTE.ping_msg.system = false;

	pthread_mutexattr_t errorcheck;
	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&system_state.mutex[host_id], &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);
	(void) tt_pthread_cond_init(&system_state.cond[host_id], NULL);
}

static void
bsync_cfg_read()
{
	bsync_state.num_connected = 0;
	bsync_state.state = bsync_state_election;
	bsync_state.local_id = BSYNC_MAX_HOSTS;
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	system_state.leader_id = BSYNC_MAX_HOSTS;
	system_state.local_id = BSYNC_MAX_HOSTS;

	bsync_state.read_timeout = cfg_getd("replication.read_timeout");
	bsync_state.write_timeout = cfg_getd("replication.write_timeout");
	bsync_state.operation_timeout = cfg_getd(
		"replication.operation_timeout");
	bsync_state.ping_timeout = cfg_getd("replication.ping_timeout");
	bsync_state.election_timeout = cfg_getd("replication.election_timeout");
	bsync_state.submit_timeout = cfg_getd("replication.submit_timeout");
	bsync_state.num_hosts = cfg_getarr_size("replication.source");
	bsync_state.max_host_queue = cfg_geti("replication.max_host_queue");
}

void
bsync_init(struct recovery_state *r)
{
	local_state = r;
	if (!r->bsync_remote) {
		return;
	}
	bsync_state.iproxy_end = NULL;
	bsync_state.iproxy_pos = NULL;
	bsync_state.wal_commit_sign = 0;
	bsync_state.wal_rollback_sign = 0;
	memset(&bsync_state.sysmsg, 0, sizeof(struct bsync_txn_info));
	/* I. Initialize the state. */
	pthread_mutexattr_t errorcheck;

	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck,
		PTHREAD_MUTEX_ERRORCHECK);
#endif
	tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_RECURSIVE);
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&bsync_state.mutex, &errorcheck);
	(void) tt_pthread_mutex_init(&bsync_state.active_ops_mutex,
		&errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);

	(void) tt_pthread_cond_init(&bsync_state.cond, NULL);

	STAILQ_INIT(&bsync_state.txn_proxy_input);
	STAILQ_INIT(&bsync_state.txn_proxy_queue);
	STAILQ_INIT(&bsync_state.bsync_proxy_input);
	STAILQ_INIT(&bsync_state.bsync_proxy_queue);
	bsync_state.txn_fibers.size = 0;
	bsync_state.txn_fibers.active = 0;
	bsync_state.bsync_fibers.size = 0;
	bsync_state.bsync_fibers.active = 0;
	rlist_create(&bsync_state.txn_fibers.data);
	rlist_create(&bsync_state.bsync_fibers.data);
	rlist_create(&bsync_state.proxy_queue);
	rlist_create(&bsync_state.commit_queue);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.execute_queue);
	rlist_create(&bsync_state.election_ops);
	rlist_create(&bsync_state.region_free);
	rlist_create(&bsync_state.region_gc);
	rlist_create(&system_state.incoming_connections);
	rlist_create(&system_state.wait_start);
	system_state.wal_started = false;

	slab_cache_create(&bsync_state.txn_slabc, &runtime);
	mempool_create(&bsync_state.region_pool, &bsync_state.txn_slabc,
		sizeof(struct bsync_region));

	system_state.have_snapshot = true;
	system_state.recovery = false;
	system_state.started = false;
	system_state.snapshot_fiber = NULL;
	system_state.recovery_mode = false;
	memset(system_state.iproto, 0, sizeof(system_state.iproto));
	memset(system_state.fail, 0, sizeof(system_state.fail));
	memset(system_state.join, 0, sizeof(system_state.join));
	memset(system_state.id2index, BSYNC_MAX_HOSTS,
		sizeof(system_state.id2index));

	ev_async_init(&txn_process_event, bsync_txn_process);
	ev_async_init(&bsync_process_event, bsync_process_loop);

	txn_loop = loop();

	ev_async_start(loop(), &txn_process_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	for (int i = 0; i < r->remote_size; ++i) {
		bsync_cfg_push_host(i, r->remote[i].source);
	}
	tt_pthread_mutex_lock(&bsync_state.mutex);
	if (!cord_start(&bsync_state.cord, "bsync", bsync_thread, NULL)) {
		tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
	} else {
		local_state = NULL;
	}
}

void
bsync_start(struct recovery_state *r)
{BSYNC_TRACE
	if (!r->bsync_remote) {
		ev_async_start(local_state->writer->txn_loop,
			&local_state->writer->write_event);
		return;
	}
	system_state.wal_started = true;
	assert(local_state == r);
	local_state->writer->txn_loop = bsync_loop;
	struct bsync_txn_info *info = NULL;
	if (system_state.local_id < BSYNC_MAX_HOSTS) {
		info = &bsync_index[system_state.local_id].sysmsg;
		info->sign = vclock_signature(&local_state->vclock);
		info->repeat = false;
		info->proxy = true;
		SWITCH_TO_BSYNC(bsync_process_connect);
	}
	info = &bsync_state.sysmsg;
	SWITCH_TO_BSYNC(bsync_start_event);
}

static void
bsync_election(va_list /* ap */)
{
	fiber_yield_timeout(bsync_state.election_timeout);
	if (bsync_state.state != bsync_state_election)
		return;
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void*
bsync_thread(void*)
{BSYNC_TRACE
	tt_pthread_mutex_lock(&bsync_state.mutex);
	iobuf_init();
	slab_cache_create(&bsync_state.bsync_slabc, &runtime);
	mempool_create(&bsync_state.system_send_pool, &bsync_state.bsync_slabc,
		sizeof(struct bsync_send_elem));
	if (bsync_state.num_hosts == 1) {
		bsync_state.leader_id = bsync_state.local_id = 0;
		bsync_index[0].state = bsync_host_connected;
		bsync_state.state = bsync_state_ready;
	}
	bsync_loop = loop();
	ev_async_start(loop(), &bsync_process_event);
	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	fiber_call(fiber_new("bsync_election", bsync_election));
	try {
		ev_run(loop(), 0);
	} catch (...) {
		say_crit("bsync_thread found unhandled exception");
		throw;
	}
	ev_async_stop(bsync_loop, &bsync_process_event);
	say_info("bsync stopped");
	slab_cache_destroy(&bsync_state.bsync_slabc);
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{BSYNC_TRACE
	assert(local_state == r);
	if (!local_state->bsync_remote) {
		recovery_delete(r);
		return;
	}
	struct bsync_txn_info *info = &bsync_state.sysmsg;
	SWITCH_TO_BSYNC(bsync_shutdown);
	if (&bsync_state.cord != cord() && cord_join(&bsync_state.cord)) {
		panic_syserror("BSYNC writer: thread join failed");
	}
	ev_async_stop(txn_loop, &txn_process_event);
	bsync_state.txn_fibers.size = bsync_state.txn_fibers.active = 0;
	bsync_state.bsync_fibers.size = bsync_state.bsync_fibers.active = 0;
	rlist_create(&bsync_state.txn_fibers.data);
	rlist_create(&bsync_state.bsync_fibers.data);
	rlist_create(&bsync_state.proxy_queue);
	rlist_create(&bsync_state.commit_queue);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.execute_queue);
	rlist_create(&bsync_state.election_ops);
	rlist_create(&bsync_state.region_free);
	rlist_create(&bsync_state.region_gc);
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		rlist_create(&BSYNC_REMOTE.op_queue);
		BSYNC_REMOTE.op_queue_size = 0;
		rlist_create(&BSYNC_REMOTE.send_queue);
		BSYNC_REMOTE.send_queue_size = 0;
		rlist_create(&BSYNC_REMOTE.recovery_queue);
		mh_bsync_delete(BSYNC_REMOTE.active_ops);
		tt_pthread_mutex_destroy(&system_state.mutex[host_id]);
	}
	tt_pthread_mutex_destroy(&bsync_state.mutex);
	tt_pthread_mutex_destroy(&bsync_state.active_ops_mutex);
	slab_cache_destroy(&bsync_state.txn_slabc);
}
