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

#define __PASS_UB_CPP__
#include "../common.h"
#include "gentree.h"

#include "pass-ub.h"
/*** C++ undefined behaviour fixes ***/
const int UB_SIGSEGV = 3;
const int UB_ORDER = 1;
TLS <VarPtr> last_ub_error;

template <class T>
bool in_vector (const vector <T> *vec, const T &val) {
  if (vec != NULL && std::find (vec->begin(), vec->end(), val) != vec->end()) {
    *last_ub_error = val;
    return true;
  }
  return false;
}
template <class T>
bool vectors_intersect (const vector <T> *a, const vector <T> *b) {
  if (a == NULL || b == NULL) {
    return false;
  }
  FOREACH (*a, ai) {
    if (in_vector (b, *ai)) {
      return true;
    }
  }
  return false;
}

bool is_same_var (const VarPtr &a, const VarPtr &b) {
  if (a == b) {
    *last_ub_error = a;
    return true;
  }
  return in_vector (a->bad_vars, b) || in_vector (b->bad_vars, a) ||
    vectors_intersect (a->bad_vars, b->bad_vars);
}

bool is_var_written (const FunctionPtr &function, const VarPtr &var) {
  if (function->bad_vars == NULL || (var->type() != VarData::var_global_t && var->bad_vars == NULL)) {
    return false;
  }
  if (in_vector (function->bad_vars, var)) {
    return true;
  }
  return vectors_intersect (var->bad_vars, function->bad_vars);
}

bool is_var_read (const FunctionPtr &function, const VarPtr &var) {
  //TODO
  return false;
}

bool is_ub_functions (const FunctionPtr &first, const FunctionPtr &second) {
  //TODO
  vector <VarPtr> *a = first->bad_vars;
  vector <VarPtr> *b = second->bad_vars;

  if (a == NULL || b == NULL) {
    return false;
  }
  if (a->size() > b->size()) {
    swap (a, b);
  }
  vector <VarPtr>::iterator begin = b->begin(), end = b->end();
  FOREACH (*a, i) {
    begin = lower_bound (begin, end, *i);
    if (begin == end) {
      break;
    }
    if (*begin == *i) {
      return true;
    }
  }
  return false;
}

class UBMergeData {
  private:
    vector <VarPtr> writes_;
    vector <VarPtr> reads_;
    vector <VarPtr> index_refs_;
    vector <FunctionPtr> functions_;

    template <class A, class B, class F> static void check (
        const vector <A> &first,
        const vector <B> &second,
        F f,
        int error_mask, int *res_error) {

      if ((*res_error & error_mask) == error_mask) {
        return;
      }
      FOREACH (first, a) {
        FOREACH (second, b) {
          if (f (*a, *b)) {
            *res_error |= error_mask;
            return;
          }
        }
      }
    }

    static int check (const UBMergeData &first, const UBMergeData &second) {
      int err = 0;
      //sigsegv
      check (first.functions_, second.index_refs_, is_var_written, UB_SIGSEGV, &err);
      check (second.functions_, first.index_refs_, is_var_written, UB_SIGSEGV, &err);
      check (first.writes_, second.writes_, is_same_var, UB_SIGSEGV, &err);

      //just ub
      check (first.reads_, second.writes_, is_same_var, UB_SIGSEGV, &err);
      check (first.writes_, second.reads_, is_same_var, UB_SIGSEGV, &err);

      return err;
    }
    static int check (const UBMergeData &data, FunctionPtr function) {
      int err = 0;
      vector <FunctionPtr> tmp (1, function);
      check (tmp, data.index_refs_, is_var_written, UB_SIGSEGV, &err);
      return err;
    }

  public:
    UBMergeData() {
    };
    int merge_with (const UBMergeData &other, bool no_check_flag) {
      int err = 0;
      if (!no_check_flag) {
        err = check (*this, other);
      }
      reads_.insert (reads_.end(), other.reads_.begin(), other.reads_.end());
      writes_.insert (writes_.end(), other.writes_.begin(), other.writes_.end());
      index_refs_.insert (index_refs_.end(), other.index_refs_.begin(), other.index_refs_.end());
      functions_.insert (functions_.end(), other.functions_.begin(), other.functions_.end());
      return err;
    }
    int check_index_refs (const vector <VarPtr> &vars) {
      int err = 0;
      check (functions_, vars, is_var_written, UB_SIGSEGV, &err);
      check (reads_, vars, is_same_var, UB_SIGSEGV, &err);
      check (writes_, vars, is_same_var, UB_SIGSEGV, &err);
      return err;
    }
    int called_by (VertexAdaptor <op_func_call> func_call) {
      FunctionPtr function = func_call->get_func_id();
      int err = check (*this, function);
      functions_.push_back (function);
      return err;
    }

    static UBMergeData create_from_var (VertexAdaptor <op_var> var, bool index_flag) {
      UBMergeData res;
      if (var->rl_type == val_l) {
        res.writes_.push_back (var->get_var_id());
        if (index_flag) {
          res.index_refs_.push_back (var->get_var_id());
        }
      } else {
        res.reads_.push_back (var->get_var_id());
      }
      return res;
    }
    static UBMergeData create_from_func_ptr (VertexAdaptor <op_func_ptr> func_ptr) {
      UBMergeData res;
      res.functions_.push_back (func_ptr->get_func_id());
      return res;
    }
};

void fix_ub_dfs (VertexPtr v, UBMergeData *data, VertexPtr parent = VertexPtr()) {
  stage::set_location (v->get_location());

  *last_ub_error = VarPtr();
  if (v->type() == op_var) {
    *data = UBMergeData::create_from_var (v, parent.not_null() && parent->type() == op_index);
  } else if (v->type() == op_func_ptr) {
    *data = UBMergeData::create_from_func_ptr (v);
  } else {
    Location save_location = stage::get_location();

    int res = 0;
    bool do_not_check = v->type() == op_ternary || v->type() == op_log_or || v->type() == op_log_and ||
      v->type() == op_log_or_let || v->type() == op_log_and_let || v->type() == op_require ||
      v->type() == op_require_once;

    for (VertexRange i = all (*v); !i.empty(); i.next()) {
      UBMergeData node_data;
      fix_ub_dfs (*i, &node_data, v);
      res |= data->merge_with (node_data, do_not_check);
    }
    if (v->type() == op_func_call) {
      res |= data->called_by (v);
    }
    stage::set_location (save_location);

    if (res > 0) {
      bool supported = true;
      supported &= v->type() == op_set || v->type() == op_set_value || v->type() == op_push_back ||
        v->type() == op_push_back_return || v->type() == op_array || OpInfo::rl (v->type()) == rl_set ||
        v->type() == op_index;
      if (supported) {
        v->extra_type = op_ex_safe_version;
      } else {
        PROF (B).start();
        kphp_warning (dl_pstr ("Dangerous undefined behaviour %s, [var = %s]", 
            OpInfo::str (v->type()).c_str(), (*last_ub_error)->name.c_str()));
        PROF (B).finish();
      }
    }
  }
}

void fix_ub (VertexPtr v, vector <VarPtr> *foreach_vars) {
  if (v->type() == op_global || v->type() == op_static || v->type() == op_unset) {
    return;
  }
  if (OpInfo::type (v->type()) == cycle_op ||
      v->type() == op_list || v->type() == op_seq_comma ||
      v->type() == op_if || v->type() == op_else || v->type() == op_try ||
      v->type() == op_seq || v->type() == op_case || v->type() == op_default ||
      v->type() == op_noerr) {
    if (v->type() == op_foreach) {
      VertexAdaptor <op_foreach> foreach_v = v;
      VertexAdaptor <op_foreach_param> params = foreach_v->params();
      VertexPtr x = params->x();
      if (x->ref_flag) {
        VertexPtr xs = params->xs();
        while (xs->type() == op_index) {
          xs = xs.as <op_index>()->array();
        }
        kphp_assert (xs->type() == op_var);
        VarPtr var = xs.as <op_var>()->get_var_id();
        foreach_vars->push_back (var);
        fix_ub (foreach_v->cmd(), foreach_vars);
        foreach_vars->pop_back();
        return;
      }
    }
    for (VertexRange i = all (*v); !i.empty(); i.next()) {
      fix_ub (*i, foreach_vars);
    }
    return;
  }

  UBMergeData data;
  fix_ub_dfs (v, &data);
  int err = data.check_index_refs (*foreach_vars);
  if (err > 0) {
    kphp_warning (dl_pstr ("Dangerous undefined behaviour %s, [foreach var = %s]", 
        OpInfo::str (v->type()).c_str(), (*last_ub_error)->name.c_str()));
  }
}

void fix_undefined_behaviour (FunctionPtr function) {
  if (function->root->type() != op_function) {
    return;
  }
  stage::set_function (function);
  vector <VarPtr> foreach_vars;
  fix_ub (function->root.as <op_function>()->cmd(), &foreach_vars);
}


