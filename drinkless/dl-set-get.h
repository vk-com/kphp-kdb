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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#include "dl-utils.h"

typedef struct {
  char *text;
  int len, random_tag;
} message;

void msg_free (message *msg) {
 if (msg->text != NULL) {
    dl_free (msg->text, msg->len + 1);
    msg->text = NULL;
  }
  msg->len = -1;
  msg->random_tag = -1;
}

char *msg_get_buf (message *msg) {
  if (msg == NULL) {
    return NULL;
  }

  return msg->text;
}

int msg_reinit (message *msg, int len, int random_tag) {
  msg->text = dl_realloc (msg->text, len + 1, msg->len + 1);
  if (msg->text == NULL) {
    msg->len = -1;
    return -1;
  }

  msg->len = len;
  msg->text[len] = 0;
  msg->random_tag = random_tag;

  return 0;
}

int msg_verify (message *msg, int random_tag) {
  if (msg == NULL) {
    return -1;
  }

  if (msg->random_tag != random_tag) {
    msg_free (msg);

    return -1;
  }

  return 0;
}

#define	MESSAGE(c) ((message *) ((c)->custom_data + sizeof (struct mcs_data)))


int engine_init_accepted (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "engine_init_accepted\n");
  }
  memset (MESSAGE(c), 0, sizeof (message));
  MESSAGE(c)->len = -1;

  return mcs_init_accepted (c);
}

int engine_close_connection (struct connection *c, int who) {
  if (verbosity > 1) {
    fprintf (stderr, "engine_close_connection\n");
  }
  msg_free (MESSAGE(c));

  return server_close_connection (c, who);
}


#define mcs_init_accepted       engine_init_accepted
#define server_close_connection engine_close_connection

