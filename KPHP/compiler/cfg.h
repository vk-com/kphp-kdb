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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#pragma once

#include "gentree.h"
namespace cfg {
  struct CFGCallbackBase {
    virtual void split_var (VarPtr var, vector < vector <VertexPtr> > &parts) = 0;
    virtual void unused_vertices (vector <VertexPtr *> &v) = 0;
    virtual FunctionPtr get_function() = 0;
    virtual void uninited (VertexPtr) = 0;
    virtual ~CFGCallbackBase() {
    }
  };

  //just simple int id type
  struct IdBase {
    int id;
    IdBase();
  };

  typedef Id <IdBase> Node;
  typedef vector <Node> NodesList;
  typedef Range <vector <Node>::iterator> NodesRange;

  typedef enum {usage_write_t, usage_read_t} UsageType;
  struct UsageData {
    int id;
    int part_id;
    UsageType type;
    bool weak_write_flag;
    VertexPtr v;
    Node node;
    explicit UsageData (UsageType type, VertexPtr v);
  };
  typedef Id <UsageData> UsagePtr;
  typedef Range <vector <UsagePtr>::iterator> UsagesRange;

  struct SubTreeData {
    VertexPtr v;
    bool recursive_flag;
    SubTreeData (VertexPtr v, bool recursive_flag);
  };
  typedef Id <SubTreeData> SubTreePtr;

  struct VertexUsage {
    bool used;
    bool used_rec;
    VertexUsage();
  };

  struct VarSplitData {
    int n;

    IdGen <UsagePtr> usage_gen;
    IdMap <UsagePtr> parent;

    VarSplitData();
  };

  typedef Id<VarSplitData> VarSplitPtr;

  class CFG {
    CFGCallbackBase *callback;
    FunctionPtr cur_function;
    IdGen <Node> node_gen;
    IdMap <vector <Node> > node_next, node_prev;
    IdMap <vector <UsagePtr> > node_usages;
    IdMap <vector <SubTreePtr> > node_subtrees;
    IdMap <VertexUsage> vertex_usage;
    int cur_dfs_mark;
    Node current_start;
    Node current_finish;

    IdMap <int> node_was;
    IdMap <UsagePtr> node_mark;
    IdMap <VarSplitPtr> var_split_data;

    vector <vector <Node> > continue_nodes;
    vector <vector <Node> > break_nodes;
    void create_cfg_enter_cycle();
    void create_cfg_exit_cycle (Node continue_dest, Node break_dest);
    void create_cfg_add_break_node (Node v, int depth);
    void create_cfg_add_continue_node (Node v, int depth);

    vector <vector <Node> > exception_nodes;
    void create_cfg_begin_try();
    void create_cfg_end_try (Node to);
    void create_cfg_register_exception (Node from);

    VarSplitPtr get_var_split (VarPtr var, bool force);
    Node new_node();
    UsagePtr new_usage (UsageType type, VertexPtr v);
    void add_usage (Node node, UsagePtr usage);
    SubTreePtr new_subtree (VertexPtr v, bool recursive_flag);
    void add_subtree (Node node, SubTreePtr subtree);
    void add_edge (Node from, Node to);
    void collect_ref_vars (VertexPtr v, set <VarPtr> *ref);
    void find_splittable_vars (FunctionPtr func, vector <VarPtr> *splittable_vars);
    void collect_vars_usage (VertexPtr tree_node, Node writes, Node reads, bool *throws_flag);
    void create_full_cfg (VertexPtr tree_node, Node *res_start, Node *res_finish);
    void create_cfg (VertexPtr tree_node, Node *res_start, Node *res_finish,
         bool set_flag = false, bool weak_write_flag = false);

    void calc_used (Node v);
    void confirm_usage (VertexPtr, bool recursive_flags);
    void collect_unused (VertexPtr *v, vector <VertexPtr *> *unused_vertices);

    UsagePtr search_uninited (Node v, VarPtr var);

    bool try_uni_usages (UsagePtr usage, UsagePtr another_usage);
    void compress_usages (vector <UsagePtr> *usages);
    void dfs (Node v, UsagePtr usage);
    void process_var (VarPtr v);
    void process_node (Node v);
    int register_vertices (VertexPtr v, int N);
    void process_function (FunctionPtr func);
  public:
    void run (CFGCallbackBase *new_callback);
  };
}
