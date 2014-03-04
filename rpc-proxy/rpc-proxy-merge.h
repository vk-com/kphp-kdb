/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Vitaliy Valtman
*/

#ifndef __RPC_PROXY_MERGE__
#define __RPC_PROXY_MERGE__


struct gather_entry {
  int bytes;
  int *data;
};


struct gather;
struct gather_methods {
  void *(*on_start)(void);
  int (*on_send)(struct gather *G, int n);
  void (*on_error)(struct gather *G, int n);
  void (*on_answer)(struct gather *G, int n);
  void (*on_timeout)(struct gather *G, int n);
  void (*on_end)(struct gather *G);
  void (*on_send_end)(struct gather *G);
};

struct gather {
  double start_time;
  long long qid;
  int tot_num;
  int wait_num;
  int sent_num;
  int not_sent_num;
  int received_num;
  int timeouted_num;
  int errors_num;
  int saved_query_len;
  enum tl_type in_type; 
  void *extra;
  void *saved_query;
  struct tl_query_header *header;
  struct rpc_cluster *cluster;
  //void *in;
  struct process_id pid;
  struct gather_methods *methods;
  struct gather_entry List[0];
};

void merge_terminate_gather (struct gather *G);
int merge_init_response (struct gather *G);
void merge_forward (struct gather_methods *methods);
void merge_delete (struct gather *G);
void merge_save_query_remain (struct gather *G);

extern int gathers_working;
extern long long gathers_total;
extern struct rpc_query_type merge_query_type;

int default_gather_on_send (struct gather *G, int num);
#endif
