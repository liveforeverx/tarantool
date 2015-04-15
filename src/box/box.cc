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
#include "box/box.h"

#include <say.h>
#include "iproto.h"
#include "iproto_constants.h"
#include "recovery.h"
#include "replication.h"
#include "replica.h"
#include <stat.h>
#include <tarantool.h>
#include "tuple.h"
#include "lua/call.h"
#include "session.h"
#include "schema.h"
#include "engine.h"
#include "memtx_engine.h"
#include "sophia_engine.h"
#include "space.h"
#include "port.h"
#include "request.h"
#include "txn.h"
#include "user.h"
#include "cfg.h"
#include "iobuf.h"
#include "evio.h"
#include "bsync.h"
#include "authentication.h"

static void process_ro(struct request *request, struct port *port);
static void box_leave_local_standby_mode(void *data __attribute__((unused)));
box_process_func box_process = process_ro;

struct recovery_state *recovery;

static struct evio_service binary; /* iproto binary listener */

int snapshot_pid = 0; /* snapshot processes pid */
bool ready_to_leave_local_standby_mode = false;
char *current_uri = NULL;

static void
process_ro(struct request *request, struct port *port)
{
	if (!iproto_type_is_select(request->type))
		tnt_raise(LoggedError, ER_READONLY);
	return process_rw(request, port);
}

void
box_set_ro(bool ro)
{
	box_process = ro ? process_ro : process_rw;
}

bool
box_is_ro(void)
{
	return box_process == process_ro;
}

static void
recover_row(struct recovery_state *r, void *param, struct xrow_header *row)
{
	(void) param;
	(void) r;
	assert(r == recovery);
	if (row->bodycnt == 0 && (row->commit_sn || row->rollback_sn))
		return;
	assert(row->bodycnt == 1); /* always 1 for read */
	struct request request;
	request_create(&request, row->type);
	request_decode(&request, (const char *) row->body[0].iov_base,
		row->body[0].iov_len);
	request.header = row;
	process_rw(&request, &null_port);
}

/* {{{ configuration bindings */

static void
box_check_uri(const char *source, const char *option_name)
{
	if (source == NULL)
		return;
	struct uri uri;

	/* URI format is [host:]service */
	if (uri_parse(&uri, source) || !uri.service) {
		tnt_raise(ClientError, ER_CFG, option_name,
			  "expected host:service or /unix.socket");
	}
}

static enum wal_mode
box_check_wal_mode(const char *mode_name)
{
	assert(mode_name != NULL); /* checked in Lua */
	int mode = strindex(wal_mode_STRS, mode_name, WAL_MODE_MAX);
	if (mode == WAL_MODE_MAX)
		tnt_raise(ClientError, ER_CFG, "wal_mode", mode_name);
	return (enum wal_mode) mode;
}

static void
box_check_readahead(int readahead)
{
	enum { READAHEAD_MIN = 128, READAHEAD_MAX = 2147483648 };
	if (readahead < READAHEAD_MIN || readahead > READAHEAD_MAX) {
		tnt_raise(ClientError, ER_CFG, "readahead",
			  "specified value is out of bounds");
	}
}

static int
box_check_rows_per_wal(int rows_per_wal)
{
	/* check rows_per_wal configuration */
	if (rows_per_wal <= 1) {
		tnt_raise(ClientError, ER_CFG, "rows_per_wal",
			  "the value must be greater than one");
	}
	return rows_per_wal;
}

void
box_check_config()
{
	box_check_wal_mode(cfg_gets("wal_mode"));
	box_check_uri(cfg_gets("listen"), "listen");
	box_check_uri(cfg_gets("replication_source"), "replication_source");
	box_check_readahead(cfg_geti("readahead"));
	box_check_rows_per_wal(cfg_geti("rows_per_wal"));
	box_check_wal_mode(cfg_gets("wal_mode"));
}

extern "C" void
box_set_replication_source(const char *source)
{
	box_check_uri(source, "replication_source");
	if (recovery->bsync_remote)
		tnt_raise(ClientError, ER_CFG, "replication_source",
			  "cant switch from sync replication to async");
	bool old_is_replica = recovery->remote[0].reader;
	bool new_is_replica = source != NULL;
	if (old_is_replica != new_is_replica ||
	    (old_is_replica &&
	     (strcmp(source, recovery->remote[0].source) != 0))) {

		if (recovery->finalize) {
			if (old_is_replica)
				recovery_stop_remote(recovery);
			recovery_reset_remote(recovery);
			recovery_add_remote(recovery, source);
			if (recovery_has_remote(recovery))
				recovery_follow_remote(recovery);
		} else {
			/*
			 * Do nothing, we're in local hot
			 * standby mode, the server
			 * will automatically begin following
			 * the remote when local hot standby
			 * mode is finished, see
			 * box_leave_local_hot_standby_mode()
			 */
		}
	}
}

extern "C" void
box_set_listen(const char *uri)
{
/*	if (current_uri == uri || (uri != NULL &&
				current_uri != NULL &&
				strcmp(uri, current_uri) == 0))
		return;
*/	box_check_uri(uri, "listen");
	if (evio_service_is_active(&binary))
		evio_service_stop(&binary);

	if (uri != NULL) {
		evio_service_start(&binary, uri);
	} else {
		box_leave_local_standby_mode(NULL);
	}
	if (current_uri)
		free(current_uri);
	if (uri) {
		int len = strlen(uri);
		current_uri = (char *)malloc(len + 1);
		strcpy(current_uri, uri);
	} else {
		current_uri = NULL;
	}
}

extern "C" void
box_set_wal_mode(const char *mode_name)
{
	enum wal_mode mode = box_check_wal_mode(mode_name);
	if (mode != recovery->wal_mode &&
	    (mode == WAL_FSYNC || recovery->wal_mode == WAL_FSYNC)) {
		tnt_raise(ClientError, ER_CFG, "wal_mode",
			  "cannot switch to/from fsync");
	}
	/**
	 * Really update WAL mode only after we left local hot standby,
	 * since local hot standby expects it to be NONE.
	 */
	if (recovery->finalize)
		recovery_update_mode(recovery, mode);
}

extern "C" void
box_set_log_level(int level)
{
	say_set_log_level(level);
}

extern "C" void
box_set_io_collect_interval(double interval)
{
	ev_set_io_collect_interval(loop(), interval);
}

extern "C" void
box_set_snap_io_rate_limit(double limit)
{
	recovery_update_io_rate_limit(recovery, limit);
}

extern "C" void
box_set_too_long_threshold(double threshold)
{
	too_long_threshold = threshold;
}

extern "C" void
box_set_readahead(int readahead)
{
	box_check_readahead(readahead);
	iobuf_set_readahead(readahead);
}

/* }}} configuration bindings */

static void
box_leave_local_standby_mode(void *data __attribute__((unused)))
{
	if (!ready_to_leave_local_standby_mode) {
		ready_to_leave_local_standby_mode = true;
		return;
	}
	if (recovery->finalize) {
		/*
		 * Nothing to do: this happens when the server
		 * binds to both ports, and one of the callbacks
		 * is called first.
		 */
		return;
	}
	try {
	        int rows_per_wal = box_check_rows_per_wal(cfg_geti("rows_per_wal"));
	        enum wal_mode wal_mode = box_check_wal_mode(cfg_gets("wal_mode"));
	        recovery_finalize(recovery, wal_mode, rows_per_wal);
	} catch (Exception *e) {
		e->log();
		panic("unable to successfully finalize recovery");
	}

	/*
	 * notify engines about end of recovery.
	*/
	engine_end_recovery();

	stat_cleanup(stat_base, IPROTO_TYPE_STAT_MAX);
	box_set_wal_mode(cfg_gets("wal_mode"));

	bsync_leave_local_standby_mode();

	if (recovery_has_remote(recovery))
		recovery_follow_remote(recovery);

	title("running", NULL);
	say_info("ready to accept requests");
}

/**
 * Execute a request against a given space id with
 * a variable-argument tuple described in format.
 *
 * @example: you want to insert 5 into space 1:
 * boxk(IPROTO_INSERT, 1, "%u", 5);
 *
 * @note Since this is for internal use, it has
 * no boundary or misuse checks.
 */
void
boxk(enum iproto_type type, uint32_t space_id, const char *format, ...)
{
	struct request req;
	va_list ap;
	request_create(&req, type);
	req.space_id = space_id;
	char buf[128];
	char *data = buf;
	data = mp_encode_array(data, strlen(format)/2);
	va_start(ap, format);
	while (*format) {
		switch (*format++) {
		case 'u':
			data = mp_encode_uint(data, va_arg(ap, unsigned));
			break;
		case 's':
		{
			char *arg = va_arg(ap, char *);
			data = mp_encode_str(data, arg, strlen(arg));
			break;
		}
		default:
			break;
		}
	}
	va_end(ap);
	assert(data <= buf + sizeof(buf));
	req.tuple = buf;
	req.tuple_end = data;
	process_rw(&req, &null_port);
}

void
box_authenticate(const char *user_name, uint32_t len,
		 const char *tuple, const char */* tuple_end */)
{
	uint32_t scramble_len = 0;
	const char *scramble = xrow_decode_scramble(&tuple, &scramble_len);
	if (recovery->finalize) {
		authenticate(user_name, len, scramble, scramble_len);
		return;
	}
	char name_buf[BOX_NAME_MAX + 1];
	/* \0 - to correctly print user name the error message. */
	snprintf(name_buf, sizeof(name_buf), "%.*s", len, user_name);
	int user_id = 0;
	for (; user_id < recovery->remote_size; ++user_id) {
		if (recovery->remote[user_id].uri.login_len != len)
			continue;
		if (memcmp(user_name, recovery->remote[user_id].uri.login, len))
			continue;
		break;
	}
	if (user_id == recovery->remote_size)
		tnt_raise(ClientError, ER_NO_SUCH_USER, name_buf);
	struct remote *r = &recovery->remote[user_id];
	if (scramble == NULL && r->uri.password_len == 0)
		return;
	if (scramble_len != SCRAMBLE_SIZE)
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "invalid scramble size");
	char local_scramble[SCRAMBLE_SIZE];
	struct session *session = current_session();
	scramble_prepare(local_scramble, current_session()->salt,
			 r->uri.password, r->uri.password_len);
	if (memcmp(local_scramble, scramble, SCRAMBLE_SIZE))
		tnt_raise(ClientError, ER_PASSWORD_MISMATCH, name_buf);

	credentials_init(&session->credentials, admin_user);
	session->credentials.universal_access = 0xFF;
}

/**
 * @brief Called when recovery/replication wants to add a new server
 * to cluster.
 * cluster_add_server() is called as a commit trigger on cluster
 * space and actually adds the server to the cluster.
 * @param server_uuid
 */
void
box_on_cluster_join(const tt_uuid *server_uuid)
{
	/** Find the largest existing server id. */
	struct space *space = space_cache_find(SC_CLUSTER_ID);
	class Index *index = index_find(space, 0);
	struct iterator *it = index->position();
	index->initIterator(it, ITER_LE, NULL, 0);
	struct tuple *tuple = it->next(it);
	/** Assign a new server id. */
	uint32_t server_id = tuple ? tuple_field_u32(tuple, 0) + 1 : 1;
	if (server_id >= VCLOCK_MAX)
		tnt_raise(ClientError, ER_REPLICA_MAX, server_id);
	say_info("join new server to cluster: %s", tt_uuid_str(server_uuid));
	boxk(IPROTO_INSERT, SC_CLUSTER_ID, "%u%s",
	     (unsigned) server_id, tt_uuid_str(server_uuid));
}

bool
box_process_join(int fd, struct xrow_header *header)
{
	/* Check permissions */
	access_check_universe(PRIV_R);
	access_check_space(space_cache_find(SC_CLUSTER_ID), PRIV_W);

	assert(header->type == IPROTO_JOIN);
	struct tt_uuid server_uuid = uuid_nil;
	xrow_decode_join(header, &server_uuid);
	if (tt_uuid_is_equal(&server_uuid, &recovery->server_uuid)) {
		tnt_raise(ClientError, ER_SERVER_ID_IS_LOCAL);
	}
	/* Process JOIN request via replication relay */
	if (!recovery->bsync_remote) {
		box_on_cluster_join(&server_uuid);
	}
	if (bsync_process_join(fd, &server_uuid)) {
		replication_join(fd, header);
		return recovery->bsync_remote;
	}
	return false;
}

void
box_process_subscribe(int fd, struct xrow_header *header)
{
	/* Check permissions */
	access_check_universe(PRIV_R);

	/* process SUBSCRIBE request via replication relay */
	replication_subscribe(recovery, fd, header, recovery->server_uuid);
}

/** Replace the current server id in _cluster */
static void
box_set_server_uuid(bool create)
{
	struct recovery_state *r = recovery;
	if (create)
		tt_uuid_create(&r->server_uuid);
	assert(r->server_id == 0);
	if (vclock_has(&r->vclock, 1))
		vclock_del_server(&r->vclock, 1);
	boxk(IPROTO_REPLACE, SC_CLUSTER_ID, "%u%s",
	     1, tt_uuid_str(&r->server_uuid));
	/* Remove surrogate server */
	vclock_del_server(&r->vclock, 0);
	assert(r->server_id == 1);
	assert(vclock_has(&r->vclock, 1));
}

/** Insert a new cluster into _schema */
static void
box_set_cluster_uuid()
{
	tt_uuid uu;
	/* Generate a new cluster UUID */
	tt_uuid_create(&uu);
	/* Save cluster UUID in _schema */
	boxk(IPROTO_REPLACE, SC_SCHEMA_ID, "%s%s", "cluster",
	     tt_uuid_str(&uu));
}

void
box_free(void)
{
	if (recovery == NULL)
		return;
	session_free();
	user_cache_free();
	schema_free();
	tuple_free();
	bsync_writer_stop(recovery);
	recovery = NULL;
	engine_shutdown();
	stat_free();
}

static void
engine_init()
{
	/*
	 * Sic: order is important here, since
	 * memtx must be the first to participate
	 * in snapshotting (in enigne_foreach order),
	 * so it must be registered first.
	 */
	MemtxEngine *memtx = new MemtxEngine();
	engine_register(memtx);

	SophiaEngine *sophia = new SophiaEngine();
	sophia->init();
	engine_register(sophia);
}

static inline void
box_init(void)
{
	tuple_init(cfg_getd("slab_alloc_arena"),
		   cfg_geti("slab_alloc_minimal"),
		   cfg_geti("slab_alloc_maximal"),
		   cfg_getd("slab_alloc_factor"));
	bool is_iproto_init = false;
	try {
		engine_init();

		schema_init();
		user_cache_init();
		/*
		 * The order is important: to initialize sessions,
		 * we need to access the admin user, which is used
		 * as a default session user when running triggers.
		 */
		session_init();

		title("loading", NULL);

		/* recovery initialization */
		recovery = recovery_new(cfg_gets("snap_dir"),
					cfg_gets("wal_dir"),
					recover_row, NULL);
		recovery_add_remote(recovery,
				    cfg_gets("replication_source"));
		recovery_setup_panic(recovery,
				     cfg_geti("panic_on_snap_error"),
				     cfg_geti("panic_on_wal_error"));
		recovery->bsync_remote = cfg_geti("replication.bsync");
		int source_len = cfg_getarr_size("replication.source");
		for (int i = 0; i < source_len; ++i) {
			recovery_add_remote(recovery,
				cfg_getarr_elem("replication.source", i));
		}
		bsync_init(recovery);
		if (recovery->remote_size > 1 && !recovery->bsync_remote) {
			tnt_raise(ClientError, ER_CFG,
				"cant execute many JOIN commands");
		}
		if (recovery_has_data(recovery)) {
			/* Tell Sophia engine LSN it must recover to. */
			int64_t checkpoint_id =
				recovery_last_checkpoint(recovery);
			engine_recover_to_checkpoint(checkpoint_id);
		} else if (recovery_has_remote(recovery)) {
			/* Initialize a new replica */
			tt_uuid_create(&recovery->server_uuid);
			engine_begin_join();
			if (recovery->bsync_remote) {
				iproto_init(&binary, &recovery->server_uuid);
				box_set_listen(cfg_gets("listen"));
				is_iproto_init = true;
			}
			if (replica_bootstrap(recovery)) {
				int64_t checkpoint_id = vclock_signature(&recovery->vclock);
				engine_checkpoint(checkpoint_id);
				xdir_scan(&recovery->snap_dir);
			}
		} else {
			/* Initialize the first server of a new cluster */
			recovery_bootstrap(recovery);
			box_set_cluster_uuid();
			box_set_server_uuid(true);
			int64_t checkpoint_id = vclock_signature(&recovery->vclock);
			engine_checkpoint(checkpoint_id);		}
		fiber_gc();
	} catch (Exception *e) {
		e->log();
		panic("can't initialize storage: %s", e->errmsg());
	}
	fiber_gc();

	title("orphan", NULL);
	recovery_follow_local(recovery, "hot_standby",
			      cfg_getd("wal_dir_rescan_delay"));
	title("hot_standby", NULL);

	if (!is_iproto_init)
		iproto_init(&binary, &recovery->server_uuid);
	/**
	 * listen is a dynamic option, so box_set_listen()
	 * will be called after box_init() as long as there
	 * is a value for listen in the configuration table.
	 *
	 * However, if cfg.listen is nil, box_set_listen() will
	 * not be called - which means that we need to leave
	 * local hot standby here. The idea is to leave
	 * local hot standby immediately if listen is not given,
	 * and only after binding to the listen uri otherwise.
	 */
	if (cfg_gets("listen") == NULL || ready_to_leave_local_standby_mode)
	{
		ready_to_leave_local_standby_mode = true;
		box_leave_local_standby_mode(NULL);
	}
	ready_to_leave_local_standby_mode = true;
}

void
box_load_cfg()
{
	try {
        	box_init();
	} catch (Exception *e) {
        	e->log();
                panic("can't initialize storage: %s", e->errmsg());
	}
}

static const char* box_initial_call = "make_scheme";
static void
box_call_initial()
{
	struct request request;
	struct iobuf *buf = iobuf_new("initial");
	int len = strlen(box_initial_call);
	char *key = (char *)region_alloc(&fiber()->gc, len + 4);
	request.key_end = mp_encode_str(key, box_initial_call, len);
	request.key = key;
	char *tuple = (char *)region_alloc(&fiber()->gc, 16);
	request.tuple_end = mp_encode_array(tuple, 0);
	request.tuple = tuple;
	request.header = (struct xrow_header *)
		region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
	try {
		box_lua_call(&request, &buf->out);
	} catch (ClientError *e) {
		if (e->errcode() != ER_NO_SUCH_PROC)
			throw;
	}
	iobuf_reset(buf);
}

void
box_generate_initial_snapshot(void *data __attribute__((unused)))
{
	tt_uuid local_uuid = recovery->server_uuid;
	tt_uuid_clear(&recovery->server_uuid);
	recovery_bootstrap(recovery);
	recovery->server_uuid = local_uuid;
	box_set_cluster_uuid();
	box_set_server_uuid(false);
	box_call_initial();
	fiber_yield();
	box_snapshot();
	xdir_scan(&recovery->snap_dir);
}

void
box_atfork()
{
	if (recovery == NULL)
		return;
	recovery_atfork(recovery);
}

int
box_snapshot()
{
	/* create snapshot file */
	int64_t checkpoint_id = vclock_signature(&recovery->vclock);
	return engine_checkpoint(checkpoint_id);
}

const char *
box_status(void)
{
    return status;
}
