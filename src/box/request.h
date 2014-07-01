#ifndef TARANTOOL_BOX_REQUEST_H_INCLUDED
#define TARANTOOL_BOX_REQUEST_H_INCLUDED
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
#include <stdbool.h>
#include "iproto_constants.h"

struct txn;
struct port;

typedef void (*request_execute_f)(struct request *, struct txn *, struct port *);
enum { REQUEST_IOVMAX = IPROTO_PACKET_BODY_IOVMAX };

struct request
{
	/*
	 * Either log row, or network header, or NULL, depending
	 * on where this packet originated from: the write ahead
	 * log/snapshot, client request, or a Lua request.
	 */
	struct iproto_header *header;
	/**
	 * Request type - IPROTO type code
	 */
	uint32_t type;
	uint32_t space_id;
	uint32_t index_id;
	uint32_t offset;
	uint32_t limit;
	uint32_t iterator;
	/** Search key or proc name. */
	const char *key;
	const char *key_end;
	/** Insert/replace tuple or proc argument or update operations. */
	const char *tuple;
	const char *tuple_end;
	/** Base field offset, e.g. 0 for C and 1 for Lua */
	int field_base;

	request_execute_f execute;
};

void
request_create(struct request *request, uint32_t code);

void
request_decode(struct request *request, const char *data, uint32_t len);

int
request_encode(struct request *request, struct iovec *iov);

#endif /* TARANTOOL_BOX_REQUEST_H_INCLUDED */
