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

#include "cfg.h"
#include "stage.h"
//Control Flow Graph constructions
namespace cfg {
  //just simple int id type
  IdBase::IdBase() : id (-1) {
  }

  UsageData::UsageData (UsageType type, VertexPtr v)
    : id (-1), part_id (-1), type (type), weak_write_flag (false), v (v), node() {
  }

  SubTreeData::SubTreeData (VertexPtr v, bool recursive_flag) : v (v), recursive_flag (recursive_flag) {
  }

  VertexUsage::VertexUsage() : used (false), used_rec (false) {
  }

  VarSplitData::VarSplitData()
    : n(0) {
    usage_gen.add_id_map (&parent);
  }

  VarSplitPtr CFG::get_var_split (VarPtr var, bool force) {
    if (get_index (var) < 0) {
      return VarSplitPtr();
    }
    VarSplitPtr res = var_split_data[var];
    if (res.is_null() && force) {
      res = VarSplitPtr (new VarSplitData ());
      var_split_data[var] = res;
    }
    return res;
  }
  Node CFG::new_node () {
    Node res = Node (new IdBase());
    node_gen.init_id (&res);
    return res;
  }

  UsagePtr CFG::new_usage (UsageType type, VertexPtr v) {
    VarPtr var = v->get_var_id();
    kphp_assert (!var.is_null());
    VarSplitPtr var_split = get_var_split (var, false);
    if (var_split.is_null()) {
      return UsagePtr();
    }
    UsagePtr res = UsagePtr (new UsageData (type, v));
    var_split->usage_gen.init_id (&res);
    var_split->parent[res] = res;
    return res;
  }

  void CFG::add_usage (Node node, UsagePtr usage) {
    if (usage.is_null()) {
      return;
    }
    //hope that one node will contain usages of the same type
    kphp_assert (node_usages[node].empty() || node_usages[node].back()->type == usage->type);
    node_usages[node].push_back (usage);
    usage->node = node;

//    VertexPtr v = usage->v; //TODO assigned but not used
  }

  SubTreePtr CFG::new_subtree (VertexPtr v, bool recursive_flag) {
    SubTreePtr res = SubTreePtr (new SubTreeData (v, recursive_flag));
    return res;
  }

  void CFG::add_subtree (Node node, SubTreePtr subtree) {
    kphp_assert (node.not_null() && subtree.not_null());
    node_subtrees[node].push_back (subtree);
  }

  void CFG::add_edge (Node from, Node to) {
    if (from.not_null() && to.not_null()) {
      //fprintf (stderr, "add_edge %d->%d\n", from->id, to->id);
      node_next[from].push_back (to);
      node_prev[to].push_back (from);
    }
  }

  void CFG::collect_ref_vars (VertexPtr v, set <VarPtr> *ref) {
    if (v->type() == op_var && v->ref_flag) {
      ref->insert (v->get_var_id());
    }
    for (VertexRange i = all (*v); !i.empty(); i.next()) {
      collect_ref_vars (*i, ref);
    }
  }

  struct XPred {
    set <VarPtr> *ref;
    XPred (set <VarPtr> *ref):ref(ref){};
    bool operator ()(const VarPtr &x) {return ref->count (x);}
  };

  void CFG::find_splittable_vars (FunctionPtr func, vector <VarPtr> *splittable_vars) {
    for (__typeof (all (func->local_var_ids)) i = all (func->local_var_ids); !i.empty(); i.next()) {
      splittable_vars->push_back (*i);
    }
    VertexAdaptor <meta_op_function> func_root = func->root;
    VertexAdaptor <op_func_param_list> params = func_root->params();
    for (__typeof (all (func->param_ids)) i = all (func->param_ids); !i.empty(); i.next()) {
      VarPtr var = *i;

      VertexAdaptor <op_func_param> param = params->params()[var->param_i];
      VertexPtr init = param->var();
      kphp_assert (init->type() == op_var);
      if (!init->ref_flag) {
        splittable_vars->push_back (var);
      }
    }

    //todo: references in foreach
    set <VarPtr> ref;
    collect_ref_vars (func->root, &ref);
    splittable_vars->erase (std::remove_if (splittable_vars->begin(), splittable_vars->end(), XPred(&ref)),
                            splittable_vars->end());
  }

  void CFG::collect_vars_usage (VertexPtr tree_node, Node writes, Node reads, bool *throws_flag) {
    //TODO: a lot of problems
    //is_set, unset, reference arguments...

    if (tree_node->type() == op_throw) {
      *throws_flag = true;
    }
    //TODO: only if function has throws flag
    if (tree_node->type() == op_func_call) {
      *throws_flag = tree_node->get_func_id()->root->throws_flag;
    }

    if (tree_node->type() == op_set) {
      VertexAdaptor <op_set> set_op = tree_node;
      if (set_op->lhs()->type() == op_var) {
        add_usage (writes, new_usage (usage_write_t, set_op->lhs()));
        collect_vars_usage (set_op->rhs(), writes, reads, throws_flag);
        return;
      }
    }
    if (tree_node->type() == op_var) {
      add_usage (reads, new_usage (usage_read_t, tree_node));
    }
    for (VertexRange i = all(*tree_node); !i.empty(); i.next()) {
      collect_vars_usage (*i, writes, reads, throws_flag);
    }
  }

  void CFG::create_cfg_enter_cycle() {
    continue_nodes.resize (continue_nodes.size() + 1);
    break_nodes.resize (break_nodes.size() + 1);
  }

  void CFG::create_cfg_exit_cycle (Node continue_dest, Node break_dest) {
    for (NodesRange i = all (continue_nodes.back()); !i.empty(); i.next()) {
      add_edge (*i, continue_dest);
    }
    for (NodesRange i = all (break_nodes.back()); !i.empty(); i.next()) {
      add_edge (*i, break_dest);
    }
    continue_nodes.pop_back();
    break_nodes.pop_back();
  }

  void CFG::create_cfg_add_break_node (Node v, int depth) {
    kphp_assert (depth >= 1);
    int i = (int)break_nodes.size() - depth;
    kphp_assert (i >= 0);
    break_nodes[i].push_back (v);
  }

  void CFG::create_cfg_add_continue_node (Node v, int depth) {
    kphp_assert (depth >= 1);
    int i = (int)continue_nodes.size() - depth;
    kphp_assert (i >= 0);
    continue_nodes[i].push_back (v);
  }

  void CFG::create_cfg_begin_try() {
    exception_nodes.resize (exception_nodes.size() + 1);
  }
  void CFG::create_cfg_end_try (Node to) {
    for (NodesRange i = all (exception_nodes.back()); !i.empty(); i.next()) {
      add_edge (*i, to);
    }
    exception_nodes.pop_back();
  }
  void CFG::create_cfg_register_exception (Node from) {
    if (exception_nodes.empty()) {
      return;
    }
    exception_nodes.back().push_back (from);
  }

  void CFG::create_full_cfg (VertexPtr tree_node, Node *res_start, Node *res_finish) {
    stage::set_location (tree_node->location);
    Node start = new_node(),
         finish = new_node(),
         writes = new_node(),
         reads = new_node();

    bool throws_flag = false;
    collect_vars_usage (tree_node, writes, reads, &throws_flag);
    compress_usages (&node_usages[writes]);
    compress_usages (&node_usages[reads]);

    add_subtree (start, new_subtree (tree_node, true));

    //add_edge (start, finish);
    add_edge (start, writes);
    add_edge (start, reads);
    add_edge (writes, reads);
    add_edge (writes, finish);
    add_edge (reads, finish);
    //TODO: (reads->writes) (finish->start)

    *res_start = start;
    *res_finish = finish;
    if (throws_flag) {
      create_cfg_register_exception (*res_finish);
    }
  }

  void CFG::create_cfg (VertexPtr tree_node, Node *res_start, Node *res_finish, bool write_flag, bool weak_write_flag) {
    stage::set_location (tree_node->location);
    bool recursive_flag = false;
    switch (tree_node->type()) {
      case op_seq_comma:
      case op_seq: {
        Node a, b, end;
        if (tree_node->empty()) {
          a = new_node();
          *res_start = a;
          *res_finish = a;
          break;
        }
        VertexRange args = tree_node.as <meta_op_varg_>()->args();
        create_cfg (args[0], res_start, &b);
        end = b;
        for (int i = 1; i < (int)tree_node->size(); i++) {
          create_cfg (args[i], &a, &b);
          add_edge (end, a);
          end = b;
        }
        *res_finish = end;
        break;
      }
      case op_log_not: {
        create_cfg (tree_node.as <op_log_not>()->expr(), res_start, res_finish);
        break;
      }
      case op_index: {
        Node var_start, var_finish;
        VertexAdaptor <op_index> index = tree_node;
        create_cfg (index->array(), &var_start, &var_finish, false, write_flag || weak_write_flag);
        Node start = var_start;
        Node finish = var_finish;
        if (index->has_key()) {
          Node index_start, index_finish;
          create_cfg (index->key(), &index_start, &index_finish);
          add_edge (index_finish, start);
          start = index_start;
        }
        *res_start = start;
        *res_finish = finish;
        break;
      }
      case op_log_and:
      case op_log_or: {
        Node first_start, first_finish, second_start, second_finish;
        VertexAdaptor <meta_op_binary_op> op = tree_node;
        create_cfg (op->lhs(), &first_start, &first_finish);
        create_cfg (op->rhs(), &second_start, &second_finish);
        Node finish = new_node();
        add_edge (first_finish, second_start);
        add_edge (second_finish, finish);
        add_edge (first_finish, finish);
        *res_start = first_start;
        *res_finish = finish;
        break;
      }
      case op_func_call: {
        FunctionPtr func = tree_node->get_func_id();
        VertexRange params;
        bool params_inited = false;
        if (func.not_null() && !func->varg_flag) {
          params = func->root.as <meta_op_function>()->params().
                              as <op_func_param_list>()->params();
          params_inited = true;
        }

        Node start, a, b;
        start = new_node();
        *res_start = start;

        VertexAdaptor <op_func_call> call = tree_node;
        VertexRange cur_params = call->args();
        int ii = 0;
        FOREACH_VERTEX (cur_params, i) {
          bool weak_write_flag = false;

          if (params_inited && params[ii]->type() == op_func_param &&
              params[ii].as <op_func_param>()->var()->ref_flag) {
            weak_write_flag = true;
          }

          VertexPtr cur = *i;
          kphp_assert (cur.not_null());
          create_cfg (cur, &a, &b, false, weak_write_flag);
          add_edge (start, a);
          start = b;

          ii++;
        }
        *res_finish = start;

        //if function has throws flag
        if (func->root->throws_flag) {
          create_cfg_register_exception (*res_finish);
        }
        break;
      }
      case op_return: {
        VertexAdaptor <op_return> return_op = tree_node;
        Node tmp;

        create_cfg (return_op->expr(), res_start, &tmp);
        *res_finish = Node();
        break;
      }
      case op_set: {
        VertexAdaptor <op_set> set_op = tree_node;
        Node a, b;
        create_cfg (set_op->rhs(), res_start, &a);
        create_cfg (set_op->lhs(), &b, res_finish, true);
        add_edge (a, b);
        break;
      }
      case op_list: {
        VertexAdaptor <op_list> list = tree_node;
        Node prev;
        create_cfg (list->array(), res_start, &prev);
        VertexRange list_params = list->list();
        for (int i = (int)list_params.size() - 1; i >= 0; i--) {
          Node a, b;
          create_cfg (list_params[i], &a, &b, true);
          add_edge (prev, a);
          prev = b;
        }
        *res_finish = prev;
        break;
      }
      case op_var: {
        Node res = new_node();
        UsagePtr usage = new_usage (write_flag ? usage_write_t : usage_read_t, tree_node);
        if (usage.not_null()) {
          usage->weak_write_flag = weak_write_flag;
        }
        add_usage (res, usage);
        *res_start = *res_finish = res;
        break;
      }
      case op_if: {
        VertexAdaptor <op_if> if_op = tree_node;
        Node finish = new_node();
        Node cond, if_start, if_finish;
        create_cfg (if_op->cond(), res_start, &cond);
        create_cfg (if_op->true_cmd(), &if_start, &if_finish);
        add_edge (cond, if_start);
        add_edge (if_finish, finish);
        if (if_op->has_false_cmd()) {
          Node else_start, else_finish;
          create_cfg (if_op->false_cmd(), &else_start, &else_finish);
          add_edge (cond, else_start);
          add_edge (else_finish, finish);
        } else {
          add_edge (cond, finish);
        }

        *res_finish = finish;
        break;
      }
      case op_break: {
        VertexAdaptor <op_break> break_op = tree_node;
        recursive_flag = true;
        Node start = new_node(), finish = Node();
        create_cfg_add_break_node (start, atoi (break_op->expr()->get_string().c_str()));

        *res_start = start;
        *res_finish = finish;
        break;
      }
      case op_continue: {
        VertexAdaptor <op_continue> continue_op = tree_node;
        recursive_flag = true;
        Node start = new_node(), finish = Node();
        create_cfg_add_continue_node (start, atoi (continue_op->expr()->get_string().c_str()));

        *res_start = start;
        *res_finish = finish;
        break;
      }
      case op_for: {
        create_cfg_enter_cycle();

        VertexAdaptor <op_for> for_op = tree_node;

        Node init_start, init_finish;
        create_cfg (for_op->pre_cond(), &init_start, &init_finish);

        Node cond_start, cond_finish;
        create_cfg (for_op->cond(), &cond_start, &cond_finish);

        Node inc_start, inc_finish;
        create_cfg (for_op->post_cond(), &inc_start, &inc_finish);

        Node action_start, action_finish_pre, action_finish = new_node();
        create_cfg (for_op->cmd(), &action_start, &action_finish_pre);
        add_edge (action_finish_pre, action_finish);

        add_edge (init_finish, cond_start);
        add_edge (cond_finish, action_start);
        add_edge (action_finish, inc_start);
        add_edge (inc_finish, cond_start);

        Node finish = new_node();
        add_edge (cond_finish, finish);

        *res_start = init_start;
        *res_finish = finish;

        create_cfg_exit_cycle (action_finish, finish);
        break;
      }
      case op_do:
      case op_while: {
        create_cfg_enter_cycle();

        VertexPtr cond, cmd;
        if (tree_node->type() == op_do) {
          VertexAdaptor <op_do> do_op = tree_node;
          cond = do_op->cond();
          cmd = do_op->cmd();
        } else if (tree_node->type() == op_while) {
          VertexAdaptor <op_while> while_op = tree_node;
          cond = while_op->cond();
          cmd = while_op->cmd();
        } else {
          kphp_fail();
        }


        Node cond_start, cond_finish;
        create_cfg (cond, &cond_start, &cond_finish);

        Node action_start, action_finish_pre, action_finish = new_node();
        create_cfg (cmd, &action_start, &action_finish_pre);
        add_edge (action_finish_pre, action_finish);

        add_edge (cond_finish, action_start);
        add_edge (action_finish, cond_start);

        Node finish = new_node();
        add_edge (cond_finish, finish);

        if (tree_node->type() == op_do) {
          *res_start = action_start;
        } else if (tree_node->type() == op_while) {
          *res_start = cond_start;
        } else {
          kphp_fail();
        }
        *res_finish = finish;

        if (tree_node->type() == op_do) {
          add_subtree (*res_start, new_subtree (cond, true));
        }

        create_cfg_exit_cycle (action_finish, finish);
        break;
      }
      case op_foreach_param: {
        VertexAdaptor <op_foreach_param> foreach_param = tree_node;
        recursive_flag = true;
        Node val_start, val_finish;
        create_cfg (foreach_param->xs(), &val_start, &val_finish);

        Node writes = new_node();
        Node finish = new_node();
        add_usage (writes, new_usage (usage_write_t, foreach_param->x()));
        if (foreach_param->has_key()) {
          add_usage (writes, new_usage (usage_write_t, foreach_param->key()));
        }

        add_edge (val_finish, writes);
        add_edge (writes, finish);

        *res_start = val_start;
        *res_finish = finish;

        break;
      }
      case op_foreach: {
        create_cfg_enter_cycle();

        VertexAdaptor <op_foreach> foreach_op = tree_node;
        Node cond_start, cond_finish;
        create_cfg (foreach_op->params(), &cond_start, &cond_finish);

        Node action_start, action_finish_pre, action_finish = new_node();
        create_cfg (foreach_op->cmd(), &action_start, &action_finish_pre);
        add_edge (action_finish_pre, action_finish);

        //TODO: cond start is visited only one
        add_edge (cond_finish, action_start);
        add_edge (action_finish, cond_start);

        Node finish = new_node();
        add_edge (action_finish, finish);
        add_edge (cond_finish, finish);

        *res_start = cond_start;
        *res_finish = finish;

        create_cfg_exit_cycle (action_finish, finish);
        break;
      }
      case op_switch: {
        create_cfg_enter_cycle();

        VertexAdaptor <op_switch> switch_op = tree_node;
        Node cond_start, cond_finish;
        create_cfg (switch_op->expr(), &cond_start, &cond_finish);

        Node prev_finish;
        Node prev_var_finish = cond_finish;

        bool was_default = false;
        Node default_start;
        FOREACH_VERTEX (switch_op->cases(), i) {
          VertexPtr expr, cmd;
          bool is_default = false;
          if ((*i)->type() == op_case) {
            VertexAdaptor <op_case> cs = *i;
            expr = cs->expr();
            cmd = cs->cmd();
          } else if ((*i)->type() == op_default) {
            is_default = true;
            VertexAdaptor <op_default> def = *i;
            cmd = def->cmd();
          } else {
            kphp_fail();
          }

          Node cur_start, cur_finish;
          create_cfg (cmd, &cur_start, &cur_finish);
          add_edge (prev_finish, cur_start);
          prev_finish = cur_finish;

          Node cur_var_start, cur_var_finish;
          if (is_default) {
            default_start = cur_start;
            was_default = true;
          } else {
            create_cfg (expr, &cur_var_start, &cur_var_finish);
            add_edge (cur_var_finish, cur_start);
            add_edge (prev_var_finish, cur_var_start);
            prev_var_finish = cur_var_finish;
          }
        }
        Node finish = new_node();
        add_edge (prev_finish, finish);
        if (!was_default) {
          add_edge (prev_var_finish, finish);
        }
        if (was_default) {
          add_edge (prev_var_finish, default_start);
        }

        *res_start = cond_start;
        *res_finish = finish;

        FOREACH_VERTEX (switch_op->cases(), i) {
          add_subtree (*res_start, new_subtree (*i, false));
        }

        create_cfg_exit_cycle (finish, finish);
        break;
      }
      case op_throw: {
        VertexAdaptor <op_throw> throw_op = tree_node;
        Node throw_start, throw_finish;
        create_cfg (throw_op->expr(), &throw_start, &throw_finish);
        create_cfg_register_exception (throw_finish);

        *res_start = throw_start;
        *res_finish = throw_finish;
        //*res_finish = Node();
        break;
      }
      case op_try: {
        VertexAdaptor <op_try> try_op = tree_node;
        Node exception_start, exception_finish;
        create_cfg (try_op->exception(), &exception_start, &exception_finish, true);

        Node try_start, try_finish;
        create_cfg_begin_try();
        create_cfg (try_op->try_cmd(), &try_start, &try_finish);
        create_cfg_end_try (exception_start);

        Node catch_start, catch_finish;
        create_cfg (try_op->catch_cmd(), &catch_start, &catch_finish);

        add_edge (exception_finish, catch_start);

        Node finish = new_node();
        add_edge (try_finish, finish);
        add_edge (catch_finish, finish);

        *res_start = try_start;
        *res_finish = finish;

        add_subtree (*res_start, new_subtree (try_op->exception(), false));
        add_subtree (*res_start, new_subtree (try_op->catch_cmd(), true));
        break;
      }
      case op_conv_bool: {
        VertexAdaptor <op_conv_bool> conv_bool = tree_node;
        create_cfg (conv_bool->expr(), res_start, res_finish);
        break;
      }
      case op_function: {
        VertexAdaptor <op_function> function = tree_node;
        Node a, b;
        create_cfg (function->params(), res_start, &a);
        create_cfg (function->cmd(), &b, res_finish);
        add_edge (a, b);
        break;
      }
      default: {
        create_full_cfg (tree_node, res_start, res_finish);
        return;
      }
    }

    add_subtree (*res_start, new_subtree (tree_node, recursive_flag));
  }

  bool cmp_by_var_id (const UsagePtr &a, const UsagePtr &b) {
    return a->v->get_var_id() < b->v->get_var_id();
  }

  bool CFG::try_uni_usages (UsagePtr usage, UsagePtr another_usage) {
    VarPtr var = usage->v->get_var_id();
    VarPtr another_var = another_usage->v->get_var_id();
    if (var == another_var) {
      VarSplitPtr var_split = get_var_split(var, false);
      kphp_assert (var_split.not_null());
      dsu_uni (&var_split->parent, usage, another_usage);
      return true;
    }
    return false;
  }

  void CFG::compress_usages (vector <UsagePtr> *usages) {
    sort (usages->begin(), usages->end(), cmp_by_var_id);
    vector <UsagePtr> res;
    for (int i = 0; i < (int)usages->size(); i++) {
      if (i == 0 || !try_uni_usages ((*usages)[i], (*usages)[i - 1])) {
        res.push_back ((*usages)[i]);
      } else {
        res.back()->weak_write_flag |= (*usages)[i]->weak_write_flag;
      }
    }
    swap (*usages, res);
  }

  void CFG::dfs (Node v, UsagePtr usage) {
    UsagePtr other_usage = node_mark[v];
    if (other_usage.not_null()) {
      try_uni_usages (usage, other_usage);
      return;
    }
    node_mark[v] = usage;

    bool return_flag = false;
    for (UsagesRange i = all (node_usages[v]); !i.empty(); i.next()) {
      UsagePtr another_usage = *i;
      if (try_uni_usages (usage, another_usage) && another_usage->type == usage_write_t) {
        return_flag = true;
      }
    }
    if (return_flag) {
      return;
    }
    for (NodesRange i = all (node_prev[v]); !i.empty(); i.next()) {
      dfs (*i, usage);
    }
  }

  UsagePtr CFG::search_uninited (Node v, VarPtr var) {
    node_was[v] = cur_dfs_mark;

    bool return_flag = false;
    for (UsagesRange i = all (node_usages[v]); !i.empty(); i.next()) {
      UsagePtr another_usage = *i;
      if (another_usage->v->get_var_id() == var) {
        if (another_usage->type == usage_write_t || another_usage->weak_write_flag) {
          return_flag = true;
        } else if (another_usage->type == usage_read_t) {
          return another_usage;
        }
      }
    }
    if (return_flag) {
      return UsagePtr();
    }

    for (NodesRange i = all (node_next[v]); !i.empty(); i.next()) {
      if (node_was[*i] != cur_dfs_mark) {
        UsagePtr res = search_uninited (*i, var);
        if (res.not_null()) {
          return res;
        }
      }
    }

    return UsagePtr();
  }

  void CFG::process_var (VarPtr var) {
    VarSplitPtr var_split = get_var_split (var, false);
    kphp_assert (var_split.not_null());

    if (var->type() != VarData::var_param_t) {
      cur_dfs_mark++;
      UsagePtr uninited = search_uninited (current_start, var);
      if (uninited.not_null()) {
        callback->uninited (uninited->v);
      }
    }

    std::fill (node_mark.begin(), node_mark.end(), UsagePtr());

    for (UsagesRange i = all (var_split->usage_gen); !i.empty(); i.next()) {
      UsagePtr u = *i;
      dfs (u->node, u);
    }

    //fprintf (stdout, "PROCESS:[%s][%d]\n", var->name.c_str(), var->id);

    int parts_cnt = 0;
    for (UsagesRange i = all (var_split->usage_gen); !i.empty(); i.next()) {
      if (node_was [(*i)->node]) {
        UsagePtr u = dsu_get (&var_split->parent, *i);
        if (u->part_id == -1) {
          u->part_id = parts_cnt++;
        }
      }
    }

    //printf ("parts_cnt = %d\n", parts_cnt);
    if (parts_cnt == 1) {
      return;
    }

    vector < vector <VertexPtr> > parts(parts_cnt);
    for (UsagesRange i = all (var_split->usage_gen); !i.empty(); i.next()) {
      if (node_was[(*i)->node]) {
        UsagePtr u = dsu_get (&var_split->parent, *i);
        parts[u->part_id].push_back ((*i)->v);
      }
    }

    callback->split_var (var, parts);
  }

  void CFG::confirm_usage (VertexPtr v, bool recursive_flag) {
    //fprintf (stdout, "%s\n", OpInfo::op_str[v->type()].c_str());
    if (!vertex_usage[v].used || (recursive_flag && !vertex_usage[v].used_rec)) {
      vertex_usage[v].used = true;
      if (recursive_flag) {
        vertex_usage[v].used_rec = true;
        for (VertexRange i = all (*v); !i.empty(); i.next()) {
          confirm_usage (*i, true);
        }
      }
    }
  }

  void CFG::calc_used (Node v) {
    node_was[v] = cur_dfs_mark;
    //fprintf (stdout, "calc_used %d\n", get_index (v));

    for (__typeof (all(node_subtrees[v])) i = all (node_subtrees[v]); !i.empty(); i.next()) {
      confirm_usage ((*i)->v, (*i)->recursive_flag);
    }
    for (NodesRange i = all (node_next[v]); !i.empty(); i.next()) {
      if (node_was[*i] != cur_dfs_mark) {
        calc_used (*i);
      }
    }
  }

  void CFG::collect_unused (VertexPtr *v, vector <VertexPtr *> *unused_vertices) {
    if (!vertex_usage[*v].used) {
      unused_vertices->push_back (v);
      return;
    }
    for (VertexRange i = all (**v); !i.empty(); i.next()) {
      collect_unused (&*i, unused_vertices);
    }
  }

  int CFG::register_vertices (VertexPtr v, int N) {
    set_index (&v, N++);
    FOREACH_VERTEX (v, i) {
      N = register_vertices (*i, N);
    }
    return N;
  }
  void CFG::process_function (FunctionPtr function) {
    //vertex_usage
    //var_split_data

    if (function->type() != FunctionData::func_local) {
      return;
    }

    vector <VarPtr> splittable_vars;
    find_splittable_vars (function, &splittable_vars);

    int var_n = (int)splittable_vars.size();
    var_split_data.update_size (var_n);
    for (int var_i = 0; var_i < var_n; var_i++) {
      VarPtr var = splittable_vars[var_i];
      set_index (&var, var_i);
      get_var_split (var, true);
    }

    int vertex_n = register_vertices (function->root, 0);
    vertex_usage.update_size (vertex_n);

    node_gen.add_id_map (&node_next);
    node_gen.add_id_map (&node_prev);
    node_gen.add_id_map (&node_was);
    node_gen.add_id_map (&node_mark);
    node_gen.add_id_map (&node_usages);
    node_gen.add_id_map (&node_subtrees);
    cur_dfs_mark = 0;

    Node start, finish;
    create_cfg (function->root, &start, &finish);
    current_start = start;
    current_finish = finish;

    cur_dfs_mark++;
    calc_used (start);
    vector <VertexPtr *> unused_vertices;
    collect_unused (&function->root.as <op_function>()->cmd(), &unused_vertices);
    callback->unused_vertices (unused_vertices);

    std::for_each (splittable_vars.begin(), splittable_vars.end(), std::bind1st(std::mem_fun(&CFG::process_var), this));
    clear(node_gen);
  }

  void CFG::run (CFGCallbackBase *new_callback) {
    callback = new_callback;
    process_function (callback->get_function());
  }
}
