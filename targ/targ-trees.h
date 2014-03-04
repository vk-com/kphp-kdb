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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Nikolai Durov
              2011-2012 Andrei Lopatin
*/

#ifndef __TARG_TREES_H__
#define __TARG_TREES_H__

#ifdef _LP64
#define	MAXLONG	(~(1L << 63))
#else
#define	MAXLONG	(~(1L << 31))
#endif

typedef unsigned treeref_t;
typedef int *treespace_t;

struct intree_node {
  treeref_t left, right;
  int x, z;
};

#define TS_MAGIC	0xeba3a4ac

struct treespace_header {
  int resvd[8];		// for SIGSEGV
  int magic;
  unsigned node_ints;
  unsigned used_ints;
  unsigned alloc_ints;
  int free_queue_cnt;
  int resvd2[3];
  int free_queue[0];
};

#define TNODE(__TS,__N)	((struct intree_node *) ((__TS) + (__N)))
#define TS_NODE(__N)	TNODE(TS,__N)

#define THEADER(__TS)	((struct treespace_header *) (__TS))
#define TS_HEADER	THEADER(TS)

typedef struct intree_node *intree_t;

typedef int (*intree_traverse_func_t)(intree_t node);

treespace_t create_treespace (unsigned treespace_ints, int node_ints);
// void clear_treespace (treespace_t TS);
// void free_treespace (treespace_t TS);

treeref_t new_intree_node (treespace_t TS);
void free_intree_node (treespace_t TS, treeref_t N);

treeref_t intree_lookup (treespace_t TS, treeref_t T, int x);
treeref_t intree_insert (treespace_t TS, treeref_t T, treeref_t N);
treeref_t intree_remove (treespace_t TS, treeref_t T, int x, treeref_t *N);
treeref_t intree_delete (treespace_t TS, treeref_t T, int x);
treeref_t intree_incr_z (treespace_t TS, treeref_t T, int x, int dz, int *nodes_num);
int intree_free (treespace_t TS, treeref_t T);
void intree_split (treespace_t TS, treeref_t T, int x, treeref_t *L, treeref_t *R);
treeref_t intree_merge (treespace_t TS, treeref_t L, treeref_t R);
int intree_traverse (treespace_t TS, treeref_t T, intree_traverse_func_t traverse_node);
int intree_unpack (treespace_t TS, treeref_t T, int *A);
treeref_t intree_build_from_list (treespace_t TS, int *A, int nodes);
int intree_check_tree (treespace_t TS, treeref_t T);
int get_treespace_free_stats (treespace_t TS);
int get_treespace_free_detailed_stats (treespace_t TS, int *where);
 
#endif
