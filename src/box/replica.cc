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
#include "replica.h"
#include "recovery.h"
#include "tarantool.h"

#include "xlog.h"
#include "fiber.h"
#include "scoped_guard.h"
#include "coio_buf.h"
#include "recovery.h"
#include "xrow.h"
#include "scramble.h"
#include "iproto_constants.h"
#include "msgpuck/msgpuck.h"
#include "box/bsync.h"
#include "box/cluster.h"

static const int RECONNECT_DELAY = 1.0;

static void
remote_read_row(struct ev_io *coio, struct iobuf *iobuf,
		struct xrow_header *row)
{
	struct ibuf *in = &iobuf->in;

	/* Read fixed header */
	if (ibuf_size(in) < 1)
		coio_breadn(coio, in, 1);

	/* Read length */
	if (mp_typeof(*in->pos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	ssize_t to_read = mp_check_uint(in->pos, in->end);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);

	uint32_t len = mp_decode_uint((const char **) &in->pos);

	/* Read header and body */
	to_read = len - ibuf_size(in);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);

	xrow_header_decode(row, (const char **) &in->pos, in->pos + len);
}

static void
remote_write_row(struct ev_io *coio, const struct xrow_header *row)
{
	struct iovec iov[XROW_IOVMAX];
	int iovcnt = xrow_to_iovec(row, iov);
	coio_writev(coio, iov, iovcnt, 0);
}

static void
remote_connect(struct recovery_state *r, struct iobuf *iobuf, int remote_id)
{
	char greeting[IPROTO_GREETING_SIZE];

	struct remote *remote = &r->remote[remote_id];
	struct uri *uri = &remote->uri;
	struct ev_io *coio = &remote->out;
	/*
	 * coio_connect() stores resolved address to \a &remote->addr
	 * on success. &remote->addr_len is a value-result argument which
	 * must be initialized to the size of associated buffer (addrstorage)
	 * before calling coio_connect(). Since coio_connect() performs
	 * DNS resolution under the hood it is theoretically possible that
	 * remote->addr_len will be different even for same uri.
	 */
	remote->addr_len = sizeof(remote->addrstorage);
	/* Prepare null-terminated strings for coio_connect() */
	coio_connect(coio, uri, &remote->addr, &remote->addr_len);
	assert(coio->fd >= 0);
	coio_readn(coio, greeting, sizeof(greeting));

	char salt[SCRAMBLE_SIZE];
	xrow_decode_greeting(greeting, salt, &remote->server_uuid);
	/* Perform authentication if user provided at least login */
	say_info("connected to %s (%s), fd=%d",
		 sio_strfaddr(&remote->addr, remote->addr_len),
		 tt_uuid_str(&remote->server_uuid), coio->fd);

	if (!remote->uri.login)
		return;

	/* Authenticate */
	say_debug("authenticating...");
	struct xrow_header row;
	xrow_encode_auth(&row, salt, uri->login,
			 uri->login_len, uri->password,
			 uri->password_len);
	remote_write_row(coio, &row);
	remote_read_row(coio, iobuf, &row);
	if (row.type != IPROTO_OK)
		xrow_decode_error(&row); /* auth failed */

	/* auth successed */
	say_info("authenticated to %s", remote->source);
}

void
replica_bootstrap_host(va_list ap)
{
	struct recovery_state *r = va_arg(ap, struct recovery_state *);
	int rid = va_arg(ap, int);
	fiber_set_name(fiber(), r->remote[rid].source);
	struct xrow_header request;
	struct iobuf *iobuf = iobuf_new(fiber_name(fiber()));
	auto coio_guard = make_scoped_guard([&] {
		iobuf_delete(iobuf);
	});
restart:
	coio_init(&r->remote[rid].out);
	if (r->join) {
		xrow_encode_join(&request, &r->server_uuid);
	} else {
		xrow_encode_subscribe(&request, &cluster_id,
					&r->server_uuid, &r->vclock);
	}
	for (;;) {
		try {
			r->remote[rid].status = "connecting";
			remote_connect(r, iobuf, rid);
			if (tt_uuid_is_equal(&r->remote[rid].server_uuid,
					&r->server_uuid))
			{
				evio_close(loop(), &r->remote[rid].out);
				r->remote[rid].localhost = true;
				say_info("localhost found, disconnecting from %s",
					tt_uuid_str(&r->server_uuid));
				bsync_push_connection(rid);
				return;
			}
			/* Send INIT request */
			remote_write_row(&r->remote[rid].out, &request);
			r->remote[rid].connected = true;
			r->remote[rid].status = "connected";
			bsync_push_connection(rid);
			break;
		} catch (FiberCancelException *e) {
			throw;
		} catch (Exception *e) {
			if (! r->remote[rid].warning_said) {
				say_error("can't connect to replica");
				e->log();
				say_info("will retry every %i second",
					 RECONNECT_DELAY);
				r->remote[rid].warning_said = true;
			}
			iobuf_reset(iobuf);
			evio_close(loop(), &r->remote[rid].out);
			fiber_sleep(RECONNECT_DELAY);
		}
	}
	if (!r->bsync_remote) {
		fiber_call(r->remote[0].reader);
	}
	fiber_gc();
	fiber_yield();
	if (evio_is_active(&r->remote[rid].out))
		evio_close(loop(), &r->remote[rid].out);
	r->remote[rid].warning_said = false;
	r->remote[rid].connected = false;
	goto restart;
}

static void
replica_bootstrap_cluster(struct recovery_state *r)
{
	for (int i = 0; i < r->remote_size; ++i) {
		assert(!r->remote[i].connected);
		if (r->remote[i].connecter) {
			fiber_call(r->remote[i].connecter);
		} else {
			r->remote[i].connecter = fiber_new(r->remote[i].source,
							replica_bootstrap_host);
			fiber_start(r->remote[i].connecter, r, i);
		}
	}
}

bool
replica_bootstrap(struct recovery_state *r)
{
	say_info("bootstrapping a replica");
	assert(recovery_has_remote(r));
	r->join = true;
	for (int i = 0; i < r->remote_size; ++i)
		r->remote[i].reader = fiber();
	/* Generate JOIN request */
	replica_bootstrap_cluster(r);
	struct remote *remote = NULL;
	if (r->bsync_remote) {
		remote = &r->remote[bsync_join()];
		if (remote->localhost)
			return false;
	} else {
		remote = &r->remote[0];
		fiber_yield();
		say_info("bootstrap finished");
	}
	say_info("start to receive snapshot from %s", remote->source);
	struct iobuf *iobuf = iobuf_new(fiber_name(fiber()));
	auto coio_guard = make_scoped_guard([&] {
		iobuf_delete(iobuf);
		if (!r->bsync_remote) {
			evio_close(loop(), &remote->out);
			remote->connected = false;
		}
	});
	/* Add a surrogate server id for snapshot rows */
	vclock_add_server(&r->vclock, 0);
	struct xrow_header response;
	/* master socket closed by guard */
	while (true) {
		remote_read_row(&remote->out, iobuf, &response);

		if (response.type == IPROTO_OK) {
			/* End of stream */
			say_info("done");
			break;
		} else if (iproto_type_is_dml(response.type)) {
			/* Regular snapshot row  (IPROTO_INSERT) */
			recovery_apply_row(r, &response);
		} else /* error or unexpected packet */ {
			xrow_decode_error(&response);  /* rethrow error */
		}
	}

	/* Decode end of stream packet */
	struct vclock vclock;
	vclock_create(&vclock);
	assert(response.type == IPROTO_OK);
	xrow_decode_vclock(&response, &vclock);

	/* Replace server vclock using data from snapshot */
	vclock_copy(&r->vclock, &vclock);
	return true;
}

static void
pull_from_remote(va_list ap)
{
	char name[FIBER_NAME_MAX];
	struct recovery_state *r = va_arg(ap, struct recovery_state *);
	struct xrow_header row;
	bool join = r->join;
	r->join = false;
	for (int i = 0; i < r->remote_size; ++i)
		r->remote[i].reader = fiber();
	struct remote *remote = NULL;
	struct iobuf *iobuf = iobuf_new(fiber_name(fiber()));
	auto coio_guard = make_scoped_guard([&] {
		iobuf_delete(iobuf);
		for (int i = 0; i < r->remote_size; ++i) {
			r->remote[i].reader = NULL;
		}
		if (remote) {
			if (remote->localhost) {
				say_info("dont need recovery, switch to normal mode");
			} else if (!r->bsync_remote) {
				remote->connected = false;
				evio_close(loop(), &remote->out);
				say_info("replication from %s stopped",
					remote->source);
			} else {
				say_info("recovery from %s stopped, switch to normal mode",
					remote->source);
				ev_io_stop(loop(), &remote->out);
			}
			bsync_recovery_stop(NULL);
		}
	});
	ev_loop *loop = loop();
	if (!r->bsync_remote) {
		remote = &r->remote[0];
	}
	if (!r->remote[0].connecter) {
		replica_bootstrap_cluster(r);
		if (r->bsync_remote) {
			remote = &r->remote[bsync_subscribe()];
			if (remote->localhost)
				return;
		} else {
			fiber_yield();
		}
	} else if (r->bsync_remote) {
		/* start recovery after execute JOIN */
		int rid = bsync_ready_subscribe();
		if (rid >= 0) {
			remote = &r->remote[rid];
			if (join) {
				xrow_header request;
				xrow_encode_subscribe(&request, &cluster_id,
						&r->server_uuid, &r->vclock);
				remote_write_row(&remote->out, &request);
			}
		} else {
			remote = &r->remote[bsync_subscribe()];
			if (remote->localhost)
				return;
		}
	}
	while (true) {
		const char *err = NULL;
		try {
			if (! remote || ! evio_is_active(&remote->out)) {
				err = "can't connect to cluster";
				if (remote->connecter)
					fiber_call(remote->connecter);
				else
					fiber_testcancel();
				if (r->bsync_remote) {
					remote = &r->remote[bsync_subscribe()];
					if (remote->localhost)
						return;
				} else {
					fiber_yield();
				}
				const char *uri = uri_format(&remote->uri);
				say_crit("starting replication from %s", uri);
				snprintf(name, sizeof(name), "replica/%s", uri);
				fiber_set_name(fiber(), name);
			}
			err = "can't read row";
			/**
			 * If there is an error in subscribe, it's
			 * sent directly in response to subscribe.
			 * If subscribe is successful, there is no
			 * "OK" response, but a stream of rows.
			 * from the binary log.
			 */
			remote_read_row(&remote->out, iobuf, &row);
			err = NULL;
			remote->lag = ev_now(loop) - row.tm;
			remote->last_row_time = ev_now(loop);

			if (iproto_type_is_error(row.type))
				xrow_decode_error(&row);  /* error */
			else if (row.type == IPROTO_OK) {
				say_info("stop async recovery, switch to sync mode");
				break;
			} else
				recovery_apply_row(r, &row);

			iobuf_reset(iobuf);
			fiber_gc();
		} catch (ClientError *e) {
			remote->status = "stopped";
			throw;
		} catch (FiberCancelException *e) {
			remote->status = "off";
			throw;
		} catch (Exception *e) {
			remote->status = "disconnected";
			if (remote && ! remote->warning_said) {
				if (err != NULL)
					say_info("%s", err);
				e->log();
				say_info("will retry every %i second",
					RECONNECT_DELAY);
				remote->warning_said = true;
			}
			evio_close(loop, &remote->out);
		}

		/* Put fiber_sleep() out of catch block.
		 *
		 * This is done to avoid situation, when two or more
		 * fibers yield's inside their try/catch blocks and
		 * throws an exceptions. Seems like exception unwinder
		 * stores some global state while being inside a catch
		 * block.
		 *
		 * This could lead to incorrect exception processing
		 * and crash the server.
		 *
		 * See: https://github.com/tarantool/tarantool/issues/136
		*/
		if (! evio_is_active(&remote->out))
			fiber_sleep(RECONNECT_DELAY);
	}
}

void
recovery_follow_remote(struct recovery_state *r)
{
//	assert(r->remote[0].reader == NULL);
	assert(recovery_has_remote(r));

	struct fiber *f = fiber_new("", pull_from_remote);
	/**
	 * So that we can safely grab the status of the
	 * fiber any time we want.
	 */
	fiber_set_joinable(f, true);

	r->remote[0].reader = f;
	fiber_start(f, r);
}

void
recovery_stop_remote(struct recovery_state *r)
{
	say_info("shutting down the replica");
	struct fiber *f = r->remote[0].reader;
	struct fiber *connecter = r->remote[0].connecter;
	r->remote[0].reader = NULL;
	r->remote[0].connecter = NULL;
	fiber_cancel(f);
	fiber_cancel(connecter);
	/**
	 * If the remote died from an exception, don't throw it
	 * up.
	 */
	Exception::cleanup(&f->exception);
	fiber_join(f);
	r->remote[0].status = "off";
	say_info("replica stopped");
}

void
recovery_reset_remote(struct recovery_state *r)
{
	r->remote_size = 0;
}

void
recovery_add_remote(struct recovery_state *r, const char *source)
{
	int remote_id = r->remote_size++;
	r->remote[remote_id].reader = NULL;
	r->remote[remote_id].writer = NULL;
	r->remote[remote_id].connecter = NULL;
	r->remote[remote_id].status = "off";
	if (source == NULL) {
		r->remote[remote_id].source[0] = '\0';
		--r->remote_size;
		return;
	}
	snprintf(r->remote[remote_id].source, sizeof(r->remote[remote_id].source), "%s", source);
	int rc = uri_parse(&r->remote[remote_id].uri, r->remote[remote_id].source);
	r->remote[remote_id].connected = false;
	r->remote[remote_id].switched = false;
	r->remote[remote_id].localhost = false;
	memset(&r->remote[remote_id].server_uuid, 0, sizeof(struct tt_uuid));
	/* URI checked by box_check_replication_source() */
	assert(rc == 0 && r->remote[remote_id].uri.service != NULL);
	(void) rc;
}

bool
recovery_has_remote(struct recovery_state *r)
{
	return r->remote_size > 0;
}

void
recovery_init_remote(struct recovery_state *r)
{
	r->remote_size = 0;
}
