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
#ifndef __PASS_UB_CPP__
#include "compiler-core.h"
struct DepData {
  vector <FunctionPtr> dep;
  vector <VarPtr> used_global_vars;

  vector <VarPtr> used_ref_vars;
  vector <std::pair <VarPtr, VarPtr> > ref_ref_edges;
  vector <std::pair <VarPtr, VarPtr> > global_ref_edges;
};
inline void swap (DepData &a, DepData &b) {
  swap (a.dep, b.dep);
  swap (a.used_global_vars, b.used_global_vars);
  swap (a.used_ref_vars, b.used_ref_vars);
  swap (a.ref_ref_edges, b.ref_ref_edges);
  swap (a.global_ref_edges, b.global_ref_edges);
}
class CalcFuncDepPass : public FunctionPassBase {
  private:
    AUTO_PROF (calc_func_dep);
    DepData data;
  public:
    string get_description() {
      return "Calc function depencencies";
    }

    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }
    VertexPtr on_enter_vertex (VertexPtr vertex, LocalT *local) {
      //NB: There is no user functions in default values of any kind.
      if (vertex->type() == op_func_call) {
        VertexAdaptor <op_func_call> call = vertex;
        FunctionPtr other_function = call->get_func_id();
        data.dep.push_back (other_function);
        if (other_function->type() != FunctionData::func_extern && !other_function->varg_flag) {
          int ii = 0;
          FOREACH (call->args(), arg) {
            VarPtr to_var = other_function->param_ids[ii];
            if (to_var->is_reference) { //passed as reference
              VertexPtr val = *arg;
              while (val->type() == op_index) {
                val = val.as <op_index>()->array();
              }
              kphp_assert (val->type() == op_var);
              VarPtr from_var = val.as <op_var>()->get_var_id();
              if (from_var->type() == VarData::var_global_t) {
                data.global_ref_edges.push_back (std::make_pair (from_var, to_var));
              } else if (from_var->is_reference) {
                data.ref_ref_edges.push_back (std::make_pair (from_var, to_var));
              }
            }
            ii++;
          }
        }
      } else if (vertex->type() == op_func_ptr) {
        data.dep.push_back (vertex.as <op_func_ptr>()->get_func_id());
      } else if (vertex->type() == op_var/* && vertex->rl_type == val_l*/) {
        VarPtr var = vertex.as <op_var>()->get_var_id();
        if (var->type() == VarData::var_global_t) {
          data.used_global_vars.push_back (var);
        }
      } else if (vertex->type() == op_func_param) {
        VarPtr var = vertex.as <op_func_param>()->var()->get_var_id();
        if (var->is_reference) {
          data.used_ref_vars.push_back (var);
        }
      }

      return vertex;
    }

    void on_finish (void) {
      my_unique (&data.dep);
      my_unique (&data.used_global_vars);
    }

    DepData *get_data_ptr() {
      return &data;
    }
};

/*** Common algorithm ***/
// Graph G
// Each node have Data
// Data have to be merged with all descendant's Data.
template <class VertexT>
class MergeReachalbeCallback {
  public:
    virtual void for_component (const vector <VertexT> &component, const vector <VertexT> &edges) = 0;
    virtual ~MergeReachalbeCallback(){}
};
template <class VertexT>
class MergeReachalbe {
  private:
    typedef IdMap <vector <VertexT> > GraphT;
    void dfs (const VertexT &vertex, const GraphT &graph, IdMap <int> *was, vector <VertexT> *topsorted) {
      if ((*was)[vertex]) {
        return;
      }
      (*was)[vertex] = 1;
      FOREACH (graph[vertex], next_vertex) {
        dfs (*next_vertex, graph, was, topsorted);
      }
      topsorted->push_back (vertex);
    }

    void dfs_component (VertexT vertex, const GraphT &graph, int color, IdMap <int> *was, 
               vector <int> *was_color, vector <VertexT> *component, vector <VertexT> *edges) {
      int other_color = (*was)[vertex];
      if (other_color == color) {
        return;
      }
      if (other_color != 0) {
        if ((*was_color)[other_color] != color) {
          (*was_color)[other_color] = color;
          edges->push_back (vertex);
        }
        return;
      }
      (*was)[vertex] = color;
      component->push_back (vertex);
      FOREACH (graph[vertex], next_vertex) {
        dfs_component (*next_vertex, graph, color, was, was_color, component, edges);
      }
    }

  public:
    void run(const GraphT &graph, const GraphT &rev_graph, const vector <VertexT> &vertices, 
        MergeReachalbeCallback <VertexT> *callback) {
      int vertex_n = (int)vertices.size();
      IdMap <int> was (vertex_n, 0);
      vector <VertexT> topsorted;
      FOREACH (vertices, vertex) {
        dfs (*vertex, rev_graph, &was, &topsorted);
      }

      std::fill (was.begin(), was.end(), 0);
      vector <int> was_color (vertex_n + 1, 0);
      int current_color = 0;
      FOREACH (reversed_all (topsorted), vertex) {
        if (was[*vertex]) {
          continue;
        }
        vector <VertexT> component;
        vector <VertexT> edges;
        dfs_component (*vertex, graph, ++current_color, &was, 
            &was_color, &component, &edges);

        callback->for_component (component, edges);
      }
    }
};

class CalcBadVars {
  private:
    class MergeBadVarsCallback : public MergeReachalbeCallback <FunctionPtr> {
      public:
        void for_component (const vector <FunctionPtr> &component, const vector <FunctionPtr> &edges) {
          vector <VarPtr> *bad_vars = new vector <VarPtr>();
          for (int i = (int)component.size() - 1; i >= 0; i--) {
            FunctionPtr function = component[i];
            bad_vars->insert (bad_vars->end(), function->tmp_vars.begin(), function->tmp_vars.end());
          }

          for (int i = (int)edges.size() - 1; i >= 0; i--) {
            FunctionPtr function = edges[i];
            if (function->bad_vars == NULL) {
              fprintf (stderr, "%s\n", function->name.c_str());
            }
            assert (function->bad_vars != NULL);
            bad_vars->insert (bad_vars->end(), function->bad_vars->begin(), function->bad_vars->end());
          }
          my_unique (bad_vars);
          for (int i = (int)component.size() - 1; i >= 0; i--) {
            FunctionPtr function = component[i];
            function->bad_vars = bad_vars;
          }
        }
    };

    void stupid_dfs (FunctionPtr function, set <FunctionPtr> *was, set <VarPtr> *vars) {
      if (was->count (function)) {
        return;
      }
      was->insert (function);
      vars->insert (function->global_var_ids.begin(), function->global_var_ids.end());
      for (int i = (int)function->dep.size() - 1; i >= 0; i--) {
        stupid_dfs (function->dep[i], was, vars);
      }
    }

    void generate_bad_vars (vector <FunctionPtr> &functions, vector <DepData *> &dep_datas) {
      int all_n = (int)functions.size();
      for (int cur_id = 0, i = 0; i < all_n; i++, cur_id++) {
        set_index (&functions[i], cur_id);
        swap (functions[i]->tmp_vars, dep_datas[i]->used_global_vars);
      }

      IdMap < vector <FunctionPtr> > rev_graph (all_n), graph (all_n);
      for (int i = 0; i < all_n; i++) {
        FunctionPtr to = functions[i];
        DepData *data = dep_datas[i];

        FOREACH ((data->dep), from) {
          rev_graph[*from].push_back (to);
        }
        std::swap (graph[to], data->dep);
      }
      MergeBadVarsCallback callback;
      MergeReachalbe <FunctionPtr> merge_bad_vars;
      merge_bad_vars.run (graph, rev_graph, functions, &callback);

      for (int i = 0; i < all_n; i++) {
        FunctionPtr function = functions[i];
        std::swap (function->dep, graph[function]);
      }
    }

    class MergeRefVarsCallback : public MergeReachalbeCallback <VarPtr> {
      private:
        const IdMap <vector <VarPtr> > &to_merge_;
      public:
        MergeRefVarsCallback (const IdMap <vector <VarPtr> > &to_merge) :
          to_merge_ (to_merge) {
        }
        void for_component (const vector <VarPtr> &component, const vector <VarPtr> &edges) {
          vector <VarPtr> *res = new vector <VarPtr>();
          FOREACH (component, var) {
            res->insert (res->end(), to_merge_[*var].begin(), to_merge_[*var].end());
          }
          FOREACH (edges, var) {
            if ((*var)->bad_vars != NULL) {
              res->insert (res->end(), (*var)->bad_vars->begin(), (*var)->bad_vars->end());
            }
          }
          my_unique (res);
          if (res->empty()) {
            delete res;
            return;
          }
          FOREACH (component, var) {
            (*var)->bad_vars = res;
          }
        }
    };

    void generate_ref_vars (vector <DepData *> &dep_datas) {
      vector <VarPtr> vars;
      FOREACH (dep_datas, data) {
        vars.insert (vars.end(), (*data)->used_ref_vars.begin(), 
            (*data)->used_ref_vars.end());
      }
      int vars_n = (int)vars.size();
      my_unique (&vars);
      assert ((int)vars.size() == vars_n);
      for (int cur_id = 0, i = 0; i < vars_n; i++, cur_id++) {
        set_index (&vars[i], cur_id);
      }

      IdMap < vector <VarPtr> > rev_graph (vars_n), graph (vars_n), ref_vars (vars_n);
      FOREACH (dep_datas, data) {
        FOREACH ((*data)->global_ref_edges, edge) {
          ref_vars[edge->second].push_back (edge->first);
        }
        FOREACH ((*data)->ref_ref_edges, edge) {
          graph[edge->second].push_back (edge->first);
          rev_graph[edge->first].push_back (edge->second);
        }
      }
      
      MergeRefVarsCallback callback (ref_vars);
      MergeReachalbe <VarPtr> merge_ref_vars;
      merge_ref_vars.run (graph, rev_graph, vars, &callback);

    }
  public:
    void run (const vector <pair <FunctionPtr, DepData *> > &tmp_vec) {
      int all_n = (int)tmp_vec.size();
      vector <FunctionPtr> functions (all_n);
      vector <DepData *> dep_datas (all_n);
      for (int i = 0; i < all_n; i++) {
        functions[i] = tmp_vec[i].first;
        dep_datas[i] = tmp_vec[i].second;
      }

      generate_bad_vars (functions, dep_datas);
      generate_ref_vars (dep_datas);
    }
};
#endif 
