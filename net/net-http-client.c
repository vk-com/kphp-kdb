/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2012 Vkontakte Ltd
              2012 Nikolai Durov
              2012 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>

#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-http-client.h"

/*
 *
 *		HTTP CLIENT INTERFACE
 *
 */

#define	CLIENT_VERSION	"lynx/0.2.39"

extern int verbosity;

int outbound_http_connections;
long long outbound_http_queries, http_bad_response_headers;

#define	MAX_KEY_LEN	1000
#define	MAX_VALUE_LEN	1048576

int htc_wakeup (struct connection *c);
int htc_parse_execute (struct connection *c);
int htc_alarm (struct connection *c);
int htc_do_wakeup (struct connection *c);
int htc_init_outbound (struct connection *c);
int htc_connected (struct connection *c);
int htc_check_ready (struct connection *c);
int htc_close_connection (struct connection *c, int who);

conn_type_t ct_http_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "http_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = htc_parse_execute,
  .close = htc_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = htc_init_outbound,
  .connected = htc_connected,
  .check_ready = htc_check_ready,
  .wakeup = htc_wakeup,
  .alarm = htc_alarm
};

enum http_response_parse_state {
  htrp_start,
  htrp_readtospace,
  htrp_readtocolon,
  htrp_readint,
  htrp_skipspc,
  htrp_skiptoeoln,
  htrp_skipspctoeoln,
  htrp_eoln,
  htrp_wantlf,
  htrp_wantlastlf,
  htrp_linestart,
  htrp_fatal,
  htrp_done
};

int htc_default_execute (struct connection *c, int op);
int htc_default_becomes_ready (struct connection *c);


struct http_client_functions default_http_client = {
  .execute = htc_default_execute,
  .htc_becomes_ready = htc_default_becomes_ready
};

int htc_default_execute (struct connection *c, int op) {
  struct htc_data *D = HTC_DATA(c);

  vkprintf (1, "http_client_execute: op=%d, header_size=%d\n", op, D->header_size);

  return SKIP_ALL_BYTES;
}

int htc_default_becomes_ready (struct connection *c) {
  return 0;
}

int htc_default_check_ready (struct connection *c) {
  return c->ready = cr_ok;
}

int htc_init_outbound (struct connection *c) {
  return 0;
}

int htc_connected (struct connection *c) {
  ++outbound_http_connections;

  if (HTC_FUNC(c)->htc_becomes_ready != NULL) {
    HTC_FUNC(c)->htc_becomes_ready (c);
  }

  return 0;
}

int htc_check_ready (struct connection *c) {
  return HTC_FUNC(c)->htc_check_ready (c);
}

int htc_close_connection (struct connection *c, int who) {
  outbound_http_connections--;
  
  server_reader (c);

  if (HTC_FUNC(c)->htc_close != NULL) {
    HTC_FUNC(c)->htc_close (c, who);
  } 

  return client_close_connection (c, who);
}


int htc_parse_execute (struct connection *c) {
  struct htc_data *D = HTC_DATA(c);
  char *ptr, *ptr_s, *ptr_e;
  int len;
  long long tt;

  while (c->status == conn_wait_answer || c->status == conn_reading_answer) {
    len = nbit_ready_bytes (&c->Q);
    ptr = ptr_s = nbit_get_ptr (&c->Q);
    ptr_e = ptr + len;
    if (len <= 0) {
      break;
    }

    while (ptr < ptr_e && c->parse_state != htrp_done) {

      switch (c->parse_state) {
      case htrp_start:
          //fprintf (stderr, "htrp_start: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	memset (D, 0, sizeof (*D));
	D->response_code = htrt_none;
	D->data_size = -1;
	c->parse_state = htrp_readtospace;

      case htrp_readtospace:
          //fprintf (stderr, "htrp_readtospace: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	while (ptr < ptr_e && ((unsigned) *ptr > ' ')) {
	  if (D->wlen < 15) {
	    D->word[D->wlen] = *ptr;
	  }
	  D->wlen++;
	  ptr++;
	}
	if (D->wlen > 4096) {
	  c->parse_state = htrp_fatal;
	  break;
	}
	if (ptr == ptr_e) {
	  break;
	}
	c->parse_state = htrp_skipspc;
	D->response_words++;
	if (D->response_words == 1) {
	  if (!memcmp (D->word, "HTTP/1.0", 8)) {
	    D->http_ver = HTTP_V10;
	    D->response_flags &= ~RF_KEEPALIVE;
	  } else if (!memcmp (D->word, "HTTP/1.1", 8)) {
	    D->http_ver = HTTP_V11;
	    D->response_flags |= RF_KEEPALIVE;
	  } else {
	    c->parse_state = htrp_skiptoeoln;
	    D->response_flags |= RF_ERROR;
	  }
	} else if (D->response_words == 2) {
	  D->response_code = htrt_error;
	  if (D->wlen == 3) {
	    D->word[3] = 0;
	    char *tmp;
	    D->response_code = strtoul (D->word, &tmp, 10);
	    if (tmp != D->word + 3 || D->response_code < 100 || D->response_code > 599) {
	      D->response_code = htrt_error;
	    }
	  }
	  if (D->response_code == htrt_error) {
	    D->response_flags |= RF_ERROR;
	  }
	  c->parse_state = htrp_skiptoeoln;
	} else {
	  assert (D->response_flags & (RF_LOCATION | RF_CONNECTION));
	  if (D->wlen) {
	    if (D->response_flags & RF_LOCATION) {
	      D->location_offset = D->header_size;
	      D->location_size = D->wlen;
	    } else {
	      if (D->wlen == 10 && !strncasecmp (D->word, "keep-alive", 10)) {
		D->response_flags |= RF_KEEPALIVE;
	      } else if (D->wlen == 5 && !strncasecmp (D->word, "close", 5)) {
		D->response_flags &= ~RF_KEEPALIVE;
	      }
	    }
	  }
	  D->response_flags &= ~(RF_LOCATION | RF_CONNECTION);
	  c->parse_state = htrp_skipspctoeoln;
	}
	D->header_size += D->wlen;
	break;

      case htrp_skipspc:
      case htrp_skipspctoeoln:
	//fprintf (stderr, "htrp_skipspc[toeoln]: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	while (D->header_size < MAX_HTTP_HEADER_SIZE && ptr < ptr_e && (*ptr == ' ' || (*ptr == '\t' && D->response_words >= 8))) {
	  D->header_size++;
	  ptr++;
	}
	if (D->header_size >= MAX_HTTP_HEADER_SIZE) {
	  c->parse_state = htrp_fatal;
	  break;
	}
	if (ptr == ptr_e) {
	  break;
	}
	if (c->parse_state == htrp_skipspctoeoln) {
	  c->parse_state = htrp_eoln;
	  break;
	}
	if (D->response_words < 2) {
	  D->wlen = 0;
	  c->parse_state = htrp_readtospace;
	} else {
	  assert (D->response_words >= 3);
	  if (D->response_flags & RF_DATASIZE) {
	    if (D->data_size != -1) {
	      c->parse_state = htrp_skiptoeoln;
	      D->response_flags |= RF_ERROR;
	    } else {
	      c->parse_state = htrp_readint;
	      D->data_size = 0;
	    }
	  } else if (D->response_flags & (RF_LOCATION | RF_CONNECTION)) {
	    D->wlen = 0;
	    c->parse_state = htrp_readtospace;
	  } else {
	    c->parse_state = htrp_skiptoeoln;
	  }
	}
	break;
      case htrp_readtocolon:
	//fprintf (stderr, "htrp_readtocolon: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	while (ptr < ptr_e && *ptr != ':' && *ptr > ' ') {
	  if (D->wlen < 15) {
	    D->word[D->wlen] = *ptr;
	  }
	  D->wlen++;
	  ptr++;
	}
	if (D->wlen > 4096) {
	  c->parse_state = htrp_fatal;
	  break;
	}
	if (ptr == ptr_e) {
	  break;
	}

	if (*ptr != ':') {
	  D->header_size += D->wlen;
	  c->parse_state = htrp_skiptoeoln;
	  D->response_flags |= RF_ERROR;
	  break;
	}

	ptr++;

	if (D->wlen == 8 && !strncasecmp (D->word, "location", 8)) {
	  D->response_flags |= RF_LOCATION;
	} else if (D->wlen == 10 && !strncasecmp (D->word, "connection", 10)) {
	  D->response_flags |= RF_CONNECTION;
	} else if (D->wlen == 14 && !strncasecmp (D->word, "content-length", 14)) {
	  D->response_flags |= RF_DATASIZE;
	} else {
	  D->response_flags &= ~(RF_LOCATION | RF_DATASIZE | RF_CONNECTION);
	}

	D->header_size += D->wlen + 1;
	c->parse_state = htrp_skipspc;
	break;

      case htrp_readint:	
	//fprintf (stderr, "htrp_readint: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);

	tt = D->data_size;
	while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
	  if (tt >= 0x7fffffffL / 10) {
	    D->response_flags |= RF_ERROR;
	    c->parse_state = htrp_skiptoeoln;
	    break;
	  }
	  tt = tt*10 + (*ptr - '0');
	  ptr++;
	  D->header_size++;
	  D->response_flags &= ~RF_DATASIZE;
	}

	D->data_size = tt;
	if (ptr == ptr_e) {
	  break;
	}

	if (D->response_flags & RF_DATASIZE) {
	  D->response_flags |= RF_ERROR;
	  c->parse_state = htrp_skiptoeoln;
	} else {
	  c->parse_state = htrp_skipspctoeoln;
	}
	break;

      case htrp_skiptoeoln:
	//fprintf (stderr, "htrp_skiptoeoln: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);

	while (D->header_size < MAX_HTTP_HEADER_SIZE && ptr < ptr_e && (*ptr != '\r' && *ptr != '\n')) {
	  D->header_size++;
	  ptr++;
	}
	if (D->header_size >= MAX_HTTP_HEADER_SIZE) {
	  c->parse_state = htrp_fatal;
	  break;
	}
	if (ptr == ptr_e) {
	  break;
	}

	c->parse_state = htrp_eoln;

      case htrp_eoln:

	if (ptr == ptr_e) {
	  break;
	}
	if (*ptr == '\r') {
	  ptr++;
	  D->header_size++;
	}
	c->parse_state = htrp_wantlf;

      case htrp_wantlf:
	//fprintf (stderr, "htrp_wantlf: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);

	if (ptr == ptr_e) {
	  break;
	}
	if (++D->response_words < 8) {
	  D->response_words = 8;
	  if (D->response_flags & RF_ERROR) {
	    c->parse_state = htrp_fatal;
	    break;
	  }
	}

	if (*ptr != '\n') {
	  D->response_flags |= RF_ERROR;
	  c->parse_state = htrp_skiptoeoln;
	  break;
	}

	ptr++;
	D->header_size++;

	c->parse_state = htrp_linestart;

      case htrp_linestart:
	//fprintf (stderr, "htrp_linestart: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);

	if (ptr == ptr_e) {
	  break;
	}

	if (!D->first_line_size) {
	  D->first_line_size = D->header_size;
	}

	if (*ptr == '\r') {
	  ptr++;
	  D->header_size++;
	  c->parse_state = htrp_wantlastlf;
	  break;
	}
	if (*ptr == '\n') {
	  c->parse_state = htrp_wantlastlf;
	  break;
	}

	if (D->response_flags & RF_ERROR) {
	  c->parse_state = htrp_skiptoeoln;
	} else {
	  D->wlen = 0;
	  c->parse_state = htrp_readtocolon;
	}
	break;

      case htrp_wantlastlf:
	//fprintf (stderr, "htrp_wantlastlf: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);

	if (ptr == ptr_e) {
	  break;
	}
	if (*ptr != '\n') {
	  c->parse_state = htrp_fatal;
	  break;
	}
	ptr++;
	D->header_size++;

	if (!D->first_line_size) {
	  D->first_line_size = D->header_size;
	}

	c->parse_state = htrp_done;

      case htrp_done:
	//fprintf (stderr, "htrp_done: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	break;

      case htrp_fatal:
	//fprintf (stderr, "htrp_fatal: ptr=%p (%.8s), hsize=%d, qf=%d, words=%d\n", ptr, ptr, D->header_size, D->response_flags, D->response_words);
	D->response_flags |= RF_ERROR;
	c->parse_state = htrp_done;
	break;

      default:
	assert (0);
      }
    }

    len = ptr - ptr_s;
    nbit_advance (&c->Q, len);

    if (c->parse_state == htrp_done) {
      if (D->header_size >= MAX_HTTP_HEADER_SIZE) {
        D->response_flags |= RF_ERROR;
      }

      if (D->response_flags & RF_ERROR) {
	D->response_code = -1;
        http_bad_response_headers++;
      }

      c->status = conn_running;
      if (!HTC_FUNC(c)->execute) {
	HTC_FUNC(c)->execute = htc_default_execute;
      }
      int res = HTC_FUNC(c)->execute (c, D->response_code);
      outbound_http_queries++;
      if (res > 0) {
	c->status = conn_reading_answer;
	return res;	// need more bytes
      } else if (res < 0) {
	if (res == SKIP_ALL_BYTES) {
	  assert (advance_skip_read_ptr (&c->In, D->header_size) == D->header_size);
	  if (D->data_size > 0) {
	    len = advance_skip_read_ptr (&c->In, D->data_size);
	    if (len < D->data_size) {
	      c->parse_state = htrp_start;
	      if (c->status == conn_running) {
		c->status = conn_wait_answer;
	      }
	      return len - D->data_size;
	    }
	  }
	} else {
	  D->response_flags &= ~RF_ERROR;
	}
      }
      if (c->status == conn_running) {
	c->status = conn_wait_answer;
      }

      if (D->response_flags & RF_ERROR) {
        assert (c->status == conn_wait_answer);
        D->response_flags &= ~RF_KEEPALIVE;
      }
      if (c->status == conn_wait_answer && !(D->response_flags & RF_KEEPALIVE)) {
        c->status = conn_write_close;
        c->parse_state = -1;
        return 0;
      }
      if (c->status != conn_wait_aio) {
        c->parse_state = htrp_start;
      }
      nbit_set (&c->Q, &c->In);
    }
  }
  if (c->status == conn_reading_answer || c->status == conn_wait_aio) {
    return NEED_MORE_BYTES;
  }
  return 0;
}


int htc_wakeup (struct connection *c) {
  if (c->status == conn_wait_net || c->status == conn_wait_aio) {
    c->status = conn_expect_query;
    HTC_FUNC(c)->htc_wakeup (c);
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int htc_alarm (struct connection *c) {
  HTC_FUNC(c)->htc_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int hts_do_wakeup (struct connection *c) {
  //struct htc_data *D = HTC_DATA(c);
  assert (0);
  return 0;
}

/*
 *
 *		USEFUL HTTP FUNCTIONS
 *
 */



/*
 *
 *		END (HTTP CLIENT)
 *
 */

