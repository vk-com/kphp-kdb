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

#include "type-inferer-core.h"

#include "data.h"

namespace tinf {
  Node::Node() :
    next_(),
    rev_next_(),
    recalc_state_ (empty_st),
    holder_id_ (-1),
    type_ (TypeData::get_type (tp_Unknown)),
    recalc_cnt_ (-1),
    isset_flags (0) {
  }

  void Node::add_edge (Edge *edge) {
    AutoLocker <Node *> locker (this);
    next_.push_back (edge);
  }

  void Node::add_rev_edge (Edge *edge) {
    AutoLocker <Node *> locker (this);
    rev_next_.push_back (edge);
  }

  int Node::get_recalc_cnt() {
    return recalc_cnt_;
  }
  int Node::get_holder_id() {
    return holder_id_;
  }

  bool Node::try_start_recalc() {
    while (true) {
      int recalc_state_copy = recalc_state_;
      switch (recalc_state_copy) {
        case empty_st:
          if (__sync_bool_compare_and_swap (&recalc_state_, empty_st, own_recalc_st)) {
            recalc_cnt_++;
            holder_id_ = get_thread_id();
            return true;
          }
          break;
        case own_st:
          if (__sync_bool_compare_and_swap (&recalc_state_, own_st, own_recalc_st)) {
            return false;
          }
          break;
        case own_recalc_st:
          return false;
        default:
          assert (0);
      }
    }
    return false;
  }

  void Node::start_recalc() {
    bool err = __sync_bool_compare_and_swap (&recalc_state_, own_recalc_st, own_st);
    kphp_assert (err == true);
  }

  bool Node::try_finish_recalc() {
    recalc_cnt_++;
    while (true) {
      int recalc_state_copy = recalc_state_;
      switch (recalc_state_copy) {
        case own_st:
          if (__sync_bool_compare_and_swap (&recalc_state_, own_st, empty_st)) {
            holder_id_ = -1;
            return true;
          }
          break;
        case own_recalc_st:
          return false;
        default:
          assert (0);
      }
    }
    return false;
  }

  TypeInferer::TypeInferer() :
    finish_flag (false) {
  }

  void TypeInferer::recalc_node (Node *node) {
    //fprintf (stderr, "tinf::recalc_node %d %p %s\n", get_thread_id(), node, node->get_description().c_str());
    if (node->try_start_recalc()) {
      Q->push (node);
    } else if (is_finished()) {
      kphp_assert (node->get_holder_id() == get_thread_id());
    }
  }
  bool TypeInferer::add_node (Node *node) {
    //fprintf (stderr, "tinf::add_node %d %p %s\n", get_thread_id(), node, node->get_description().c_str());
    if (node->get_recalc_cnt() < 0) {
      recalc_node (node);
      return true;
    }
    return false;
  }
  void TypeInferer::add_edge (Edge *edge) {
    assert (edge != NULL);
    assert (edge->from != NULL);
    assert (edge->to != NULL);
    //fprintf (stderr, "add_edge %d [%p %s] -> [%p %s]\n", get_thread_id(), edge->from, edge->from->get_description().c_str(), edge->to, edge->to->get_description().c_str());
    edge->from->add_edge (edge);
    edge->to->add_rev_edge (edge);
  }

  void TypeInferer::add_restriction (RestrictionBase *restriction) {
    restrictions->push_back (restriction);
  }

  void TypeInferer::check_restrictions() {
    AUTO_PROF (tinf_check);
    for (int i = 0; i < (int)restrictions.size(); i++) {
      const vector <RestrictionBase *> &r = *restrictions.get (i);
      for (int j = 0; j < (int)r.size(); j++) {
        r[j]->check();
      }
    }
  }


  class TypeInfererTask : public Task {
    private:
      TypeInferer *inferer_;
      NodeQueue *queue_;
    public:
      TypeInfererTask (TypeInferer *inferer, NodeQueue *queue) :
        inferer_ (inferer),
        queue_ (queue) {
      }

      void execute() {
        AUTO_PROF (tinf_infer_infer);
        //double st = dl_time();
        //int cnt = (int)queue_->size();
        //int cnt2 = 
        inferer_->run_queue (queue_);
        //fprintf (stdout, "A%d: %lf %d %d\n", get_thread_id(), dl_time() - st, cnt, cnt2);
        delete queue_;
      }
  };

  vector <Task *> TypeInferer::get_tasks() {
    vector <Task *> res;
    for (int i = 0; i < Q.size(); i++) {
      NodeQueue *q = Q.get (i);
      if (q->empty()) {
        continue;
      }
      NodeQueue *new_q = new NodeQueue();
      swap (*new_q, *q);

      res.push_back (new TypeInfererTask (this, new_q));
    }
    return res;
  }

  int TypeInferer::do_run_queue() {
    NodeQueue &q = *Q;

    int cnt = 0;
    while (!q.empty()) {
      cnt++;
      Node *node = q.front();

      node->start_recalc();
      node->recalc (this);

      if (node->try_finish_recalc()) {
        q.pop();
      }
    }

    return cnt;
  }

  int TypeInferer::run_queue (NodeQueue *new_q) {
    swap (*Q, *new_q);
    return do_run_queue();
  }

  void TypeInferer::run_node (Node *node) {
    if (add_node (node)) {
      do_run_queue();
    }
    while (node->get_recalc_cnt() == 0) {
      usleep (250);
    }
  }

  const TypeData *TypeInferer::get_type (Node *node) {
    run_node (node);
    return node->type_;
  }

  void TypeInferer::finish() {
    finish_flag = true;
  }
  bool TypeInferer::is_finished() {
    return finish_flag;
  }


  static TypeInferer *CI = NULL;

  void register_inferer (TypeInferer *inferer) {
    if (!__sync_bool_compare_and_swap (&CI, NULL, inferer)) {
      kphp_fail();
    }
  }
  TypeInferer *get_inferer() {
    return CI;
  }
}

