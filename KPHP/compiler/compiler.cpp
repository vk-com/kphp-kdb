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

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftw.h>

#include "stage.h"
#include "compiler.h"
#include "data_ptr.h"
#include "io.h"
#include "name-gen.h"

#include "lexer.h"
#include "gentree.h"

#include "bicycle.h"
#include "function-pass.h"

#include "cfg.h"
#include "type-inferer.h"
#include "code-gen.h"

#include "compiler-core.h"

#include "pass-split-switch.hpp"
#include "pass-register-vars.hpp"
#include "pass-rl.h"
#include "pass-optimize.hpp"
#include "pass-ub.h"

/*** useful functions ***/
void fix_dirname (string *s) {
  if (!s->empty() && (*s)[(int)s->size() - 1] != '/') {
    *s += "/";
  }
  if (s->empty() || (*s)[0] != '/') {
    *s = "./" + *s;
  }
}

bool is_const (VertexPtr root) {
  //TODO: make correct check
  switch (root->type()) {
    case op_plus:
      return is_const (root.as <op_plus>()->expr());
    case op_minus:
      return is_const (root.as <op_minus>()->expr());
    case op_shl:
      return is_const (root.as <op_shl>()->lhs()) && is_const (root.as <op_shl>()->rhs());
    case op_int_const:
    case op_float_const:
    case op_string:
    case op_false:
    case op_true:
    case op_null:
      return true;
    default:
      return false;
  }
}

template <class T>
class SyncPipeF {
  public:
    DataStreamRaw <T> tmp_stream;
    SyncPipeF() {
      tmp_stream.set_sink (true);
    }
    template <class OutputStreamT>
    void execute (T input, OutputStreamT &os) {
      tmp_stream << input;
    }
    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {
      mem_info_t mem_info;
      get_mem_stats (getpid(), &mem_info);

      stage::die_if_global_errors();
      while (!tmp_stream.empty()) {
        os << tmp_stream.get();
      }
    }
};


/*** CompilerArgs ***/
CompilerArgs::CompilerArgs() :
  use_subdirs (false),
  use_safe_integer_arithmetic (false),
  threads_number (16) {
}
void CompilerArgs::set_use_safe_integer_arithmetic (bool new_use_safe_integer_arithmetic) {
  use_safe_integer_arithmetic = new_use_safe_integer_arithmetic;
}
void CompilerArgs::set_use_subdirs (bool new_use_subdirs) {
  use_subdirs = new_use_subdirs;
}
void CompilerArgs::add_include (const string &s) {
  include_dirs.push_back (s);
  fix_dirname (&include_dirs.back());
}
void CompilerArgs::add_main_file (const string &s) {
  main_file_names.push_back (s);
}
void CompilerArgs::set_functions_txt (const string &s) {
  kphp_error (functions_txt.empty(), "Several functions.txt arguments have been given to compiler");
  functions_txt = s;
}
void CompilerArgs::set_dest_dir (const string &s) {
  kphp_error (dest_dir.empty(), "Several dest_dir arguments have been given to compiler");
  dest_dir = s;
  fix_dirname (&dest_dir);
}
void CompilerArgs::set_index_path (const string &new_index_path) {
  kphp_error (index_path.empty(), "Several index arguments have been given to compiler");
  index_path = new_index_path;
}
void CompilerArgs::set_base_dir (const string &s) {
  kphp_error (base_dir.empty(), "Several base_dir arguments have been given to compiler");
  base_dir = s;
  fix_dirname (&base_dir);
}
void CompilerArgs::set_threads_number (int new_threads_number) {
  kphp_error (1 <= new_threads_number && new_threads_number <= 32, "Bad threads number");
  threads_number = new_threads_number;
}
void CompilerArgs::set_verbosity (int new_verbosity) {
  verbosity = new_verbosity;
}

/*** Load file ***/
class LoadFileF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (SrcFilePtr file, OutputStream &os) {
      AUTO_PROF (load_files);
      stage::set_name ("Load file");
      stage::set_file (file);

      kphp_assert (!file->loaded);
      file->load();

      if (stage::has_error()) {
        return;
      }

      os << file;
    }
};

/*** Split file into tokens ***/
struct FileAndTokens {
  SrcFilePtr file;
  vector <Token *> *tokens;
  FileAndTokens() :
    file(),
    tokens (NULL) {
  }
};
class FileToTokensF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (SrcFilePtr file, OutputStream &os) {
      AUTO_PROF (lexer);
      stage::set_name ("Split file to tokens");
      stage::set_file (file);
      kphp_assert (file.not_null());

      kphp_assert (file->loaded);
      FileAndTokens res;
      res.file = file;
      res.tokens = new vector <Token *>();
      php_text_to_tokens (
        &file->text[0], (int)file->text.length(),
        file->main_func_name, res.tokens
      );

      if (stage::has_error()) {
        return;
      }

      os << res;
    }
};

/*** Parse tokens into syntax tree ***/
template <class DataStream>
class GenTreeCallback : public GenTreeCallbackBase {
    DataStream &os;
  public:
    GenTreeCallback (DataStream &os) :
      os (os) {
    }
    void register_function (VertexPtr root) {
      G->register_function (root, os);
    }
    void register_class (const ClassInfo &info) {
      //TODO;
    }
};

class ParseF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FileAndTokens file_and_tokens, OutputStream &os) {
      AUTO_PROF (gentree);
      stage::set_name ("Parse file");
      stage::set_file (file_and_tokens.file);
      kphp_assert (file_and_tokens.file.not_null());

      GenTreeCallback <OutputStream> callback (os);
      php_gen_tree (file_and_tokens.tokens, file_and_tokens.file->main_func_name, &callback);
    }
};

/*** Split main functions by #break_file ***/
bool split_by_break_file (VertexAdaptor <op_function> root, vector <VertexPtr> *res) {
  bool need_split = false;

  VertexAdaptor <op_seq> seq = root->cmd();
  for (VertexRange i = seq->args(); !i.empty(); i.next()) {
    if ((*i)->type() == op_break_file) {
      need_split = true;
      break;
    }
  }

  if (!need_split) {
    return false;
  }

  vector <VertexPtr> splitted;
  {
    vector <VertexPtr> cur_next;
    VertexRange i = seq->args();
    while (true) {
      if (i.empty() || (*i)->type() == op_break_file) {
        CREATE_VERTEX (new_seq, op_seq, cur_next);
        splitted.push_back (new_seq);
        cur_next.clear();
      } else {
        cur_next.push_back (*i);
      }
      if (i.empty()) {
        break;
      }
      i.next();
    }
  }

  int splitted_n = (int)splitted.size();
  VertexAdaptor <op_function> next_func;
  for (int i = splitted_n - 1; i >= 0; i--) {
    string func_name_str = gen_unique_name (stage::get_file()->short_file_name);
    CREATE_VERTEX (func_name, op_func_name);
    func_name->str_val = func_name_str;
    CREATE_VERTEX (func_params, op_func_param_list);
    CREATE_VERTEX (func, op_function, func_name, func_params, splitted[i]);
    func->extra_type = op_ex_func_global;

    if (next_func.not_null()) {
      CREATE_VERTEX (call, op_func_call);
      call->str_val = next_func->name()->get_string();
      GenTree::func_force_return (func, call);
    } else {
      GenTree::func_force_return (func);
    }
    next_func = func;

    res->push_back (func);
  }

  CREATE_VERTEX (func_call , op_func_call);
  func_call->str_val = next_func->name()->get_string();

  CREATE_VERTEX (ret, op_return, func_call);
  CREATE_VERTEX (new_seq, op_seq, ret);
  root->cmd() = new_seq;

  return true;
}

class ApplyBreakFileF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      AUTO_PROF (apply_break_file);

      stage::set_name ("Apply #break_file");
      stage::set_function (function);
      kphp_assert (function.not_null());

      VertexPtr root = function->root;

      if (function->type() != FunctionData::func_global || root->type() != op_function) {
        os << function;
        return;
      }

      vector <VertexPtr> splitted;
      split_by_break_file (root, &splitted);

      if (stage::has_error()) {
        return;
      }

      for (int i = 0; i < (int)splitted.size(); i++) {
        G->register_function (splitted[i], os);
      }

      os << function;
    }
};

/*** Replace cases in big global functions with functions call ***/
class SplitSwitchF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      SplitSwitchPass split_switch;
      run_function_pass (function, &split_switch);

      const vector <VertexPtr> &new_functions = split_switch.get_new_functions();
      for (int i = 0; i < (int)new_functions.size(); i++) {
        G->register_function (new_functions[i], os);
      }

      if (stage::has_error()) {
        return;
      }

      os << function;
    }
};


/*** Collect files and functions used in function ***/
struct ReadyFunctionPtr {
  FunctionPtr function;
  ReadyFunctionPtr(){}
  ReadyFunctionPtr (FunctionPtr function) :
    function (function) {
  }
  operator FunctionPtr() const {
    return function;
  }
};

class CollectRequiredCallbackBase {
  public:
    virtual pair <SrcFilePtr, bool> require_file (const string &file_name) = 0;
    virtual void require_function_set (
        function_set_t type,
        const string &name,
        FunctionPtr by_function) = 0;
    virtual ~CollectRequiredCallbackBase() {
    }
};

class CollectRequiredPass : public FunctionPassBase {
  private:
    AUTO_PROF (collect_required);
    bool force_func_ptr;
    CollectRequiredCallbackBase *callback;
  public:
    CollectRequiredPass (CollectRequiredCallbackBase *callback) :
      force_func_ptr (false),
      callback (callback) {
    }
    struct LocalT : public FunctionPassBase::LocalT {
      bool saved_force_func_ptr;
    };
    string get_description() {
      return "Collect required";
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      if (root->type() == op_string && force_func_ptr) {
        callback->require_function_set (fs_function, root->get_string(), current_function);
      }

      bool new_force_func_ptr = false;
      if (root->type() == op_func_call || root->type() == op_func_name) {
        callback->require_function_set (fs_function, root->get_string(), current_function);
      }

      if (root->type() == op_func_call) {
        new_force_func_ptr = true;
        const string &name = root->get_string();
        if (name == "func_get_args" || name == "func_get_arg" || name == "func_num_args") {
          current_function->varg_flag = true;
        }
      }

      local->saved_force_func_ptr = force_func_ptr;
      force_func_ptr = new_force_func_ptr;

      return root;
    }

    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      force_func_ptr = local->saved_force_func_ptr;

      if (root->type() == op_require || root->type() == op_require_once) {
        VertexAdaptor <meta_op_require> require = root;
        for (VertexRange i = require->args(); !i.empty(); i.next()) {
          kphp_error_act ((*i)->type() == op_string, "Not a string in 'require' arguments", continue);
          VertexAdaptor <op_string> cur = *i;
          pair <SrcFilePtr, bool> tmp = callback->require_file (cur->str_val);
          SrcFilePtr file = tmp.first;
          bool required = tmp.second;
          if (required) {
            file->req_id = current_function;
          }

          CREATE_VERTEX (call, op_func_call);
          if (file.not_null()) {
            call->str_val = file->main_func_name;
            *i = call;
          } else {
            kphp_error (0, dl_pstr ("Cannot require [%s]\n", cur->str_val.c_str()));
          }
        }
      }

      return root;
    }
};

template <class DataStream>
class CollectRequiredCallback : public CollectRequiredCallbackBase {
  private:
    DataStream *os;
  public:
    CollectRequiredCallback (DataStream *os) :
      os (os) {
    }
    pair <SrcFilePtr, bool> require_file (const string &file_name) {
      return G->require_file (file_name, *os);
    }
    void require_function_set (
        function_set_t type,
        const string &name,
        FunctionPtr by_function) {
      G->require_function_set (type, name, by_function, *os);
    }
};

class CollectRequiredF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      CollectRequiredCallback <OutputStream> callback (&os);
      CollectRequiredPass pass (&callback);
      run_function_pass (function, &pass);

      if (stage::has_error()) {
        return;
      }

      os << ReadyFunctionPtr (function);
    }
};


/*** Calculate proper location field for each node ***/
class CalcLocationsPass : public FunctionPassBase {
  private:
    AUTO_PROF (calc_locations);
  public:
    string get_description() {
      return "Calc locations";
    }
    VertexPtr on_enter_vertex (VertexPtr v, LocalT *local) {
      stage::set_line (v->location.line);
      v->location = stage::get_location();

      return v;
    }
};

/*** Collect defines declarations ***/
class CollectDefinesPass : public FunctionPassBase {
  private:
    AUTO_PROF (collect_defines);
  public:
    string get_description() {
      return "Collect defines";
    }
    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      if (root->type() == op_define || root->type() == op_define_raw) {
        VertexAdaptor <meta_op_define> define = root;
        VertexPtr name = define->name(), val = define->value();

        kphp_error_act (
          name->type() == op_string,
          "Define: first parameter must be a string",
          return root
        );

        DefineData::DefineType def_type;
        if (!is_const (val)) {
          def_type = DefineData::def_var;

          CREATE_VERTEX (var, op_var);
          var->extra_type = op_ex_var_superglobal;
          var->str_val = name->get_string();
          set_location (var, name->get_location());
          name = var;

          define->value() = VertexPtr();
          CREATE_VERTEX (new_root, op_set, name, val);
          set_location (new_root, root->get_location());
          root = new_root;
        } else {
          def_type = DefineData::def_php;
          CREATE_VERTEX (new_root, op_empty);
          root = new_root;
        }

        DefineData *data = new DefineData (val, def_type);
        data->name = name->get_string();
        data->file_id = stage::get_file();
        DefinePtr def_id (data);

        if (def_type == DefineData::def_var) {
          name->set_string ("d$" + name->get_string());
        } else {
          current_function->define_ids.push_back (def_id);
        }

        G->register_define (def_id);
      }

      return root;
    }
};

/*** Apply function header ***/
void function_apply_header (FunctionPtr func, VertexAdaptor <meta_op_function> header) {
  VertexAdaptor <meta_op_function> root = func->root;
  func->used_in_source = true;

  kphp_assert (root.not_null() && header.not_null());
  kphp_error_return (
    func->header.is_null(),
    dl_pstr ("Function [%s]: multiple headers", func->name.c_str())
  );
  func->header = header;

  kphp_error_return (
    root->type_rule.is_null(),
    dl_pstr ("Function [%s]: type_rule is overided by header", func->name.c_str())
  );
  root->type_rule = header->type_rule;

  kphp_error_return (
    !(!header->varg_flag && func->varg_flag),
    dl_pstr ("Function [%s]: varg_flag mismatch with header", func->name.c_str())
  );
  func->varg_flag = header->varg_flag;

  if (!func->varg_flag) {
    VertexAdaptor <op_func_param_list> root_params_vertex = root->params(), header_params_vertex = header->params();
    VertexRange root_params = root_params_vertex->params();
    VertexRange header_params = header_params_vertex->params();

    kphp_error (
      root_params.size() == header_params.size(),
      dl_pstr ("Bad header for function [%s]", func->name.c_str())
    );
    int params_n = (int)root_params.size();
    for (int i = 0; i < params_n; i++) {
      kphp_error (
        root_params[i]->size() == header_params[i]->size(),
        dl_pstr (
          "Function [%s]: %dth param has problem with default value",
          func->name.c_str(), i + 1
        )
      );
      kphp_error (
        root_params[i]->type_help == tp_Unknown,
        dl_pstr ("Function [%s]: type_help is overrided by header", func->name.c_str())
      );
      root_params[i]->type_help = header_params[i]->type_help;
    }
  }
}

void prepare_function_misc (FunctionPtr func) {
  VertexAdaptor <meta_op_function> func_root = func->root;
  kphp_assert (func_root.not_null());
  VertexAdaptor <op_func_param_list> param_list = func_root->params();
  VertexRange params = param_list->args();
  int param_n = (int)params.size();
  bool was_default = false;
  func->min_argn = param_n;
  for (int i = 0; i < param_n; i++) {
    if (func->varg_flag) {
      kphp_error (params[i].as <meta_op_func_param>()->var()->ref_flag == false,
          "Reference arguments are not supported in varg functions");
    }
    if (params[i].as <meta_op_func_param>()->has_default()) {
      if (!was_default) {
        was_default = true;
        func->min_argn = i;
      }
      if (func->type() == FunctionData::func_local) {
        kphp_error (params[i].as <meta_op_func_param>()->var()->ref_flag == false,
        dl_pstr ("Default value in reference function argument [function = %s]", func->name.c_str()));
      }
    } else {
      kphp_error (!was_default,
          dl_pstr ("Default value expected [function = %s] [param_i = %d]",
            func->name.c_str(), i));
    }
  }
}

void prepare_function (FunctionPtr function) {
  prepare_function_misc (function);

  FunctionSetPtr function_set = function->function_set;
  VertexPtr header = function_set->header;
  if (header.not_null()) {
    function_apply_header (function, header);
  }
}

class PrepareFunctionF  {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      stage::set_name ("Prepare function");
      stage::set_function (function);
      kphp_assert (function.not_null());

      prepare_function (function);

      if (stage::has_error()) {
        return;
      }

      os << function;
    }
};

/*** Replace defined and defines with values ***/
class RegisterDefinesPass : public FunctionPassBase {
  private:
    AUTO_PROF (register_defines);
  public:
    string get_description() {
      return "Register defines pass";
    }
    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      if (root->type() == op_defined) {
        bool is_defined = false;

        VertexAdaptor <op_defined> defined = root;

        kphp_error_act (
          (int)root->size() == 1 && defined->expr()->type() == op_string,
          "wrong arguments in 'defined'",
          return VertexPtr()
        );

        const string name = defined->expr().as <op_string>()->str_val;
        DefinePtr def = G->get_define (name);
        is_defined = def.not_null() && def->name == name;

        if (is_defined) {
          CREATE_VERTEX (true_val, op_true);
          root = true_val;
        } else {
          CREATE_VERTEX (false_val, op_false);
          root = false_val;
        }
      }

      if (root->type() == op_func_name) {
        const string &name = root->get_string();
        DefinePtr d = G->get_define (name);

        if (d.not_null()) {
          assert (d->name == name);
          if (d->type() == DefineData::def_var) {
            CREATE_VERTEX (var, op_var);
            var->extra_type = op_ex_var_superglobal;
            var->str_val = "d$" + name;
            root = var;
          } else if (d->type() == DefineData::def_raw || d->type() == DefineData::def_php) {
            CREATE_VERTEX (def, op_define_val);
            def->set_define_id (d);
            root = def;
          } else {
            assert (0 && "unreachable branch");
          }
        }
      }
      return root;
    }
};

/*** Hack for problems with ===, null and type inference ***/
class PreprocessEq3Pass : public FunctionPassBase {
  private:
    AUTO_PROF (preprocess_eq3);
  public:
    string get_description() {
      return "Preprocess eq3";
    }
    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      if (root->type() == op_eq3 || root->type() == op_neq3) {
        VertexAdaptor <meta_op_binary_op> eq_op = root;
        VertexPtr a = eq_op->lhs();
        VertexPtr b = eq_op->rhs();

        if (b->type() == op_var || b->type() == op_index) {
          std::swap (a, b);
        }

        if (a->type() == op_var || a->type() == op_index) {
          VertexPtr ra = a;
          while (ra->type() == op_index) {
            ra = ra.as <op_index>()->array();
          }
          bool ok = ra->type() == op_var;
          if (ok) {
            ok &= //(b->type() != op_true && b->type() != op_false) ||
              (ra->get_string() != "connection" &&
               ra->get_string().find ("MC") == string::npos);
          }

          if (ok) {
            if (b->type() == op_null) {
              VertexPtr check_cmd;
              CREATE_VERTEX (isset, op_isset, a);
              if (root->type() == op_neq3) {
                check_cmd = isset;
              } else {
                CREATE_VERTEX (not_isset, op_log_not, isset);
                check_cmd = not_isset;
              }
              root = check_cmd;
            } else if (b->type() == op_false ||
                (b->type() == op_string && b->get_string() == "") ||
                (b->type() == op_int_const && b->get_string() == "0") ||
                (b->type() == op_float_const/* && b->str_val == "0.0"*/) ||
                (b->type() == op_array && b->empty())) {
              VertexPtr check_cmd;

              VertexPtr isset;
              VertexPtr a_copy = clone_vertex (a);
              if (b->type() == op_true || b->type() == op_false) {
                CREATE_VERTEX(is_bool, op_func_call, a_copy);
                is_bool->str_val = "is_bool";
                isset = is_bool;
              } else if (b->type() == op_string) {
                CREATE_VERTEX(is_string, op_func_call, a_copy);
                is_string->str_val = "is_string";
                isset = is_string;
              } else if (b->type() == op_int_const) {
                CREATE_VERTEX(is_integer, op_func_call, a_copy);
                is_integer->str_val = "is_integer";
                isset = is_integer;
              } else if (b->type() == op_float_const) {
                CREATE_VERTEX(is_float, op_func_call, a_copy);
                is_float->str_val = "is_float";
                isset = is_float;
              } else if (b->type() == op_array) {
                CREATE_VERTEX(is_array, op_func_call, a_copy);
                is_array->str_val = "is_array";
                isset = is_array;
              } else {
                kphp_fail();
              }


              if (root->type() == op_neq3) {
                CREATE_VERTEX (not_isset, op_log_not, isset);
                CREATE_VERTEX (check, op_log_or, not_isset, root);
                check_cmd = check;
              } else {
                CREATE_VERTEX (check, op_log_and, isset, root);
                check_cmd = check;
              }
              root = check_cmd;
            }
          }
        }
      }

      return root;
    }
};

/*** Replace __FUNCTION__ ***/
/*** Set function_id for all function calls ***/
class PreprocessFunctionCPass : public FunctionPassBase {
  private:
    AUTO_PROF (preprocess_function_c);
  public:
    string get_description() {
      return "Preprocess function C";
    }
    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      if (root->type() == op_function_c) {
        CREATE_VERTEX (new_root, op_string);
        new_root->str_val = stage::get_function_name();
        set_location (new_root, root->get_location());
        root = new_root;
      }

      if (root->type() == op_func_call || root->type() == op_func_ptr) {
        root = try_set_func_id (root);
      }

      return root;
    }
};

/*** Preprocess 'break 5' and similar nodes. The will be replaced with goto ***/
class PreprocessBreakPass : public FunctionPassBase {
  private:
    AUTO_PROF (preprocess_break);
    vector <VertexPtr> cycles;

    int current_label_id;
    int get_label_id (VertexAdaptor <meta_op_cycle> cycle, Operation op) {
      int *val = NULL;
      if (op == op_break) {
        val = &cycle->break_label_id;
      } else if (op == op_continue) {
        val = &cycle->continue_label_id;
      } else {
        assert (0);
      }
      if (*val == 0) {
        *val = ++current_label_id;
      }
      return *val;
    }

  public:
    struct LocalT : public FunctionPassBase::LocalT {
      bool is_cycle;
    };

    PreprocessBreakPass() :
      current_label_id (0) {
      //cycles.reserve (1024);
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      local->is_cycle = OpInfo::type (root->type()) == cycle_op;
      if (local->is_cycle) {
        cycles.push_back (root);
      }

      if (root->type() == op_break || root->type() == op_continue) {
        int val;
        VertexAdaptor <meta_op_goto> goto_op = root;
        kphp_error_act (
          goto_op->expr()->type() == op_int_const,
          "Break/continue parameter expected to be constant integer",
          return root
        );
        val = atoi (goto_op->expr()->get_string().c_str());
        kphp_error_act (
          1 <= val && val <= 10,
          "Break/continue parameter expected to be in [1;10] interval",
          return root
        );

        bool force_label = false;
        if (goto_op->type() == op_continue &&  val == 1 && !cycles.empty()
            && cycles.back()->type() == op_switch) {
          force_label = true;
        }


        int cycles_n = (int)cycles.size();
        kphp_error_act (
          val <= cycles_n,
          "Break/continue parameter is too big",
          return root
        );
        if (val != 1 || force_label) {
          goto_op->int_val = get_label_id (cycles[cycles_n - val], root->type());
        }
      }

      return root;
    }

    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      if (local->is_cycle) {
        cycles.pop_back();
      }
      return root;
    }
};

/*** Calculate const_type for all nodes ***/
class CalcConstTypePass : public FunctionPassBase {
  private:
    AUTO_PROF (calc_const_type);
  public:
    struct LocalT : public FunctionPassBase::LocalT {
      bool has_nonconst;
      LocalT() :
        has_nonconst (false) {
       }
    };
    string get_description() {
      return "Calc const types";
    }

    void on_exit_edge (VertexPtr v, LocalT *v_local, VertexPtr from, LocalT *from_local) {
      v_local->has_nonconst |= from->const_type == cnst_nonconst_val;
    }

    VertexPtr on_exit_vertex (VertexPtr v, LocalT *local) {
      switch (OpInfo::cnst (v->type())) {
        case cnst_func:
          if (v->get_func_id().not_null()) {
            VertexPtr root = v->get_func_id()->root;
            if (root.is_null() || root->type_rule.is_null() || root->type_rule->extra_type != op_ex_rule_const) {
              v->const_type = cnst_nonconst_val;
              break;
            }
          }
        case cnst_const_func:
          v->const_type = local->has_nonconst ? cnst_nonconst_val : cnst_const_val;
          break;
        case cnst_nonconst_func:
          v->const_type = cnst_nonconst_val;
          break;
        case cnst_not_func:
          v->const_type = cnst_not_val;
          break;
        default:
          kphp_error (0, dl_pstr ("Unknown cnst-type for [op = %d]", v->type()));
          kphp_fail();
          break;
      }
      return v;
    }
};

/*** Throws flags calculcation ***/
class CalcThrowEdgesPass : public FunctionPassBase {
  private:
    vector <FunctionPtr> edges;
  public:
    string get_description() {
      return "Collect throw edges";
    }

    VertexPtr on_enter_vertex (VertexPtr v, LocalT *local) {
      if (v->type() == op_throw) {
        current_function->root->throws_flag = true;
      }
      if (v->type() == op_func_call) {
        FunctionPtr from = v->get_func_id();
        kphp_assert (from.not_null());
        edges.push_back (from);
      }
      return v;
    }

    template <class VisitT>
    bool user_recursion (VertexPtr v, LocalT *local, VisitT &visit) {
      if (v->type() == op_try) {
        VertexAdaptor <op_try> try_v = v;
        visit (try_v->catch_cmd());
        return true;
      }
      return false;
    }

    const vector <FunctionPtr> &get_edges() {
      return edges;
    }
};

struct FunctionAndEdges {
  FunctionPtr function;
  vector <FunctionPtr> *edges;
  FunctionAndEdges() :
   function(),
   edges (NULL) {
  }
  FunctionAndEdges (FunctionPtr function, vector <FunctionPtr> *edges) :
    function (function),
    edges (edges) {
  }

};
class CalcThrowEdgesF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStreamT>
    void execute (FunctionPtr function, OutputStreamT &os) {
      AUTO_PROF (calc_throw_edges);
      CalcThrowEdgesPass pass;
      run_function_pass (function, &pass);

      if (stage::has_error()) {
        return;
      }

      os << FunctionAndEdges (function, new vector <FunctionPtr> (pass.get_edges()));
    }
};

static int throws_func_cnt = 0;
void calc_throws_dfs (FunctionPtr from, IdMap <vector <FunctionPtr> > &graph, vector <FunctionPtr> *bt) {
  throws_func_cnt++;
  //FIXME
  if (false && from->header.not_null()) {
    stringstream ss;
    ss << "Extern function [" << from->name << "] throws \n";
    for (int i = (int)bt->size() - 1; i >= 0; i--) {
      ss << "-->[" << bt->at (i)->name << "]";
    }
    ss << "\n";
    kphp_warning (ss.str().c_str());
  }
  bt->push_back (from);
  for (FunctionRange i = all (graph[from]); !i.empty(); i.next()) {
    FunctionPtr to = *i;
    if (!to->root->throws_flag) {
      to->root->throws_flag = true;
      calc_throws_dfs (to, graph, bt);

    }
  }
  bt->pop_back();
}

class CalcThrowsF {
  private:
    DataStreamRaw <FunctionAndEdges> tmp_stream;
  public:
    CalcThrowsF() {
      tmp_stream.set_sink (true);
    }

    template <class OutputStreamT>
    void execute (FunctionAndEdges input, OutputStreamT &os) {
      tmp_stream << input;
    }

    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {

      mem_info_t mem_info;
      get_mem_stats (getpid(), &mem_info);

      stage::set_name ("Calc throw");
      stage::set_file (SrcFilePtr());

      stage::die_if_global_errors();

      AUTO_PROF (calc_throws);

      vector <FunctionPtr> from;

      vector <FunctionAndEdges> all = tmp_stream.get_as_vector();
      int cur_id = 0;
      for (int i = 0; i < (int)all.size(); i++) {
        set_index (&all[i].function, cur_id++);
        if (all[i].function->root->throws_flag) {
          from.push_back (all[i].function);
        }
      }

      IdMap < vector <FunctionPtr> > graph;
      graph.update_size (all.size());
      for (int i = 0; i < (int)all.size(); i++) {
        for (int j = 0; j < (int)all[i].edges->size(); j++) {
          graph[(*all[i].edges)[j]].push_back (all[i].function);
        }
      }

      for (int i = 0; i < (int)from.size(); i++) {
        vector <FunctionPtr> bt;
        calc_throws_dfs (from[i], graph, &bt);
      }


      if (stage::has_error()) {
        return;
      }

      for (int i = 0; i < (int)all.size(); i++) {
        os << all[i].function;
        delete all[i].edges;
      }
    }
};

/*** Check function calls ***/
class CheckFunctionCallsPass : public FunctionPassBase {
  private:
    AUTO_PROF (check_function_calls);
  public:
    string get_description() {
      return "Check function calls";
    }

    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->root->type() == op_function;
    }

    bool on_start (FunctionPtr function) {
      if (!FunctionPassBase::on_start (function)) {
        return false;
      }
      return true;
    }

    void check_func_call (VertexPtr call) {
      kphp_assert (call->type() == op_func_call || call->type() == op_func_ptr);

      FunctionPtr f = call->get_func_id();
      kphp_assert (f.not_null());
      kphp_error_return (f->root.not_null(), dl_pstr ("Function [%s] undeclared", f->name.c_str()));

      if (call->type() == op_func_ptr) {
        return;
      }

      VertexRange func_params = f->root.as <meta_op_function>()->params().
        as <op_func_param_list>()->params();

      if (f->varg_flag) {
        return;
      }

      VertexRange call_params = call.as <op_func_call>()->args();
      int func_params_n = (int)func_params.size(), call_params_n = (int)call_params.size();

      kphp_error_return (
        call_params_n >= f->min_argn,
        dl_pstr ("Not enough arguments in function [%s:%s] [found %d] [expected at least %d]",
          f->file_id->file_name.c_str(), f->name.c_str(), call_params_n, f->min_argn)
      );

      kphp_error (
        call_params.empty() || call_params[0]->type() != op_varg,
        dl_pstr (
          "call_user_func_array is used for function [%s:%s]",
          f->file_id->file_name.c_str(), f->name.c_str()
        )
      );

      kphp_error_return (
        func_params_n >= call_params_n,
        dl_pstr (
          "Too much arguments in function [%s:%s] [found %d] [expected %d]",
          f->file_id->file_name.c_str(), f->name.c_str(), call_params_n, func_params_n
        )
      );
      for (int i = 0; i < call_params_n; i++) {
        if (func_params[i]->type() == op_func_param_callback) {
          kphp_error_act (
            call_params[i]->type() != op_string,
            "Can't use a string as callback function's name",
            continue
          );
          kphp_error_act (
            call_params[i]->type() == op_func_ptr,
            "Function pointer expected",
            continue
          );

          FunctionPtr func_ptr = call_params[i]->get_func_id();

          kphp_error_act (
            func_ptr->root.not_null(),
            dl_pstr ("Unknown callback function [%s]", func_ptr->name.c_str()),
            continue
          );
          VertexRange cur_params = func_ptr->root.as <meta_op_function>()->params().
            as <op_func_param_list>()->params();
          kphp_error (
            (int)cur_params.size() == func_params[i].as <op_func_param_callback>()->param_cnt,
            "Wrong callback arguments count"
          );
          for (int j = 0; j < (int)cur_params.size(); j++) {
            kphp_error (cur_params[j]->type() == op_func_param,
                       "Callback function with callback parameter");
            kphp_error (cur_params[j].as <op_func_param>()->var()->ref_flag == 0,
                       "Callback function with reference parameter");
          }
        } else {
          kphp_error (call_params[i]->type() != op_func_ptr, "Unexpected function pointer");
        }
      }
    }

    VertexPtr on_enter_vertex (VertexPtr v, LocalT *local) {
      if (v->type() == op_func_ptr || v->type() == op_func_call) {
        check_func_call (v);
      }
      return v;
    }
};

/*** RL ***/
void rl_calc (VertexPtr root, RLValueType expected_rl_type);

class CalcRLF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      AUTO_PROF (calc_rl);
      stage::set_name ("Calc RL");
      stage::set_function (function);

      rl_calc (function->root, val_none);

      if (stage::has_error()) {
        return;
      }

      os << function;
    }
};

/*** Control Flow Graph ***/
class CFGCallback : public cfg::CFGCallbackBase {
  private:
    FunctionPtr function;

    vector <VertexPtr> uninited_vars;
    vector <VarPtr> todo_var;
    vector <vector <vector <VertexPtr> > > todo_parts;
  public:
    void set_function (FunctionPtr new_function) {
      function = new_function;
    }

    void split_var (VarPtr var, vector < vector <VertexPtr> > &parts) {
      assert (var->type() == VarData::var_local_t || var->type() == VarData::var_param_t);
      int parts_size = (int)parts.size();
      if (parts_size == 0) {
        if (var->type() == VarData::var_local_t) {
          function->local_var_ids.erase (
              std::find (
                function->local_var_ids.begin(),
                function->local_var_ids.end(),
                var));
        }
        return;
      }
      assert (parts_size > 1);

      for (int i = 0; i < parts_size; i++) {
        string new_name = var->name + "$v_" + int_to_str (i);
        VarPtr new_var = G->create_var (new_name, var->type());

        for (int j = 0; j < (int)parts[i].size(); j++) {
          VertexPtr v = parts[i][j];
          v->set_var_id (new_var);
        }

        VertexRange params = function->root.
          as <meta_op_function>()->params().
          as <op_func_param_list>()->args();
        if (var->type() == VarData::var_local_t) {
          new_var->type() = VarData::var_local_t;
          function->local_var_ids.push_back (new_var);
        } else if (var->type() == VarData::var_param_t) {
          bool was_var = std::find (
              parts[i].begin(),
              parts[i].end(),
              params[var->param_i].as <op_func_param>()->var()
              ) != parts[i].end();

          if (was_var) { //union of part that contains function argument
            new_var->type() = VarData::var_param_t;
            new_var->param_i = var->param_i;
            new_var->init_val = var->init_val;
            function->param_ids[var->param_i] = new_var;
          } else {
            new_var->type() = VarData::var_local_t;
            function->local_var_ids.push_back (new_var);
          }
        } else {
          kphp_fail();
        }

      }

      if (var->type() == VarData::var_local_t) {
        vector <VarPtr>::iterator tmp = std::find (function->local_var_ids.begin(), function->local_var_ids.end(), var);
        if (function->local_var_ids.end() != tmp) {
          function->local_var_ids.erase (tmp);
        } else {
          kphp_fail();
        }
      }

      todo_var.push_back (var);

      //it could be simple std::move
      todo_parts.push_back (vector <vector <VertexPtr> > ());
      std::swap (todo_parts.back(), parts);
    }
    void unused_vertices (vector <VertexPtr *> &v) {
      for (__typeof (all (v)) i = all (v); !i.empty(); i.next()) {
        CREATE_VERTEX (empty, op_empty);
        **i = empty;
      }
    }
    FunctionPtr get_function() {
      return function;
    }
    void uninited (VertexPtr v) {
      if (v.not_null() && v->type() == op_var) {
        uninited_vars.push_back (v);
        v->get_var_id()->set_uninited_flag (true);
      }
    }

    void check_uninited() {
      for (int i = 0; i < (int)uninited_vars.size(); i++) {
        VertexPtr v = uninited_vars[i];
        VarPtr var = v->get_var_id();
        if (tinf::get_type (v)->ptype() == tp_var) {
          continue;
        }

        stage::set_location (v->get_location());
        kphp_warning (dl_pstr ("Variable [%s] may be used uninitialized", var->name.c_str()));
      }
    }

    VarPtr merge_vars (vector <VarPtr> vars, const string &new_name) {
      VarPtr new_var = G->create_var (new_name, VarData::var_unknown_t);;
      //new_var->tinf = vars[0]->tinf; //hack, TODO: fix it
      new_var->tinf_node.copy_type_from (tinf::get_type (vars[0]));

      int param_i = -1;
      for (__typeof (all (vars)) i = all (vars); !i.empty(); i.next()) {
        if ((*i)->type() == VarData::var_param_t) {
          param_i = (*i)->param_i;
        } else if ((*i)->type() == VarData::var_local_t) {
          //FIXME: remember to remove all unused variables
          //func->local_var_ids.erase (*i);
          vector <VarPtr>::iterator tmp = std::find (function->local_var_ids.begin(), function->local_var_ids.end(), *i);
          if (function->local_var_ids.end() != tmp) {
            function->local_var_ids.erase (tmp);
          } else {
            kphp_fail();
          }

        } else {
          assert (0 && "unreachable");
        }
      }
      if (param_i != -1) {
        new_var->type() = VarData::var_param_t;
        function->param_ids[param_i] = new_var;
      } else {
        new_var->type() = VarData::var_local_t;
        function->local_var_ids.push_back (new_var);
      }

      return new_var;
    }


    struct MergeData {
      int id;
      VarPtr var;
      MergeData (int id, VarPtr var) :
        id (id),
        var (var) {
      }
    };

    static bool cmp_merge_data (const MergeData &a, const MergeData &b) {
      return type_out (tinf::get_type (a.var)) <
        type_out (tinf::get_type (b.var));
    }
    static bool eq_merge_data (const MergeData &a, const MergeData &b) {
      return type_out (tinf::get_type (a.var)) ==
        type_out (tinf::get_type (b.var));
    }

    void merge_same_type() {
      int todo_n = (int)todo_parts.size();
      for (int todo_i = 0; todo_i < todo_n; todo_i++) {
        vector <vector <VertexPtr> > &parts = todo_parts[todo_i];

        int n = (int)parts.size();
        vector <MergeData> to_merge;
        for (int i = 0; i < n; i++) {
          to_merge.push_back (MergeData (i, parts[i][0]->get_var_id()));
        }
        sort (to_merge.begin(), to_merge.end(), cmp_merge_data);

        vector <int> ids;
        int merge_id = 0;
        for (int i = 0; i <= n; i++) {
          if (i == n || (i > 0 && !eq_merge_data (to_merge[i - 1], to_merge[i]))) {
            vector <VarPtr> vars;
            for (int j = 0; j < (int)ids.size(); j++) {
              vars.push_back (parts[ids[j]][0]->get_var_id());
            }
            string new_name = vars[0]->name;
            int name_i = (int)new_name.size() - 1;
            while (new_name[name_i] != '$') {
              name_i--;
            }
            new_name.erase (name_i);
            new_name += "$v";
            new_name += int_to_str (merge_id++);

            VarPtr new_var = merge_vars (vars, new_name);
            for (int j = 0; j < (int)ids.size(); j++) {
              for (__typeof (all (parts[ids[j]])) v = all (parts[ids[j]]); !v.empty(); v.next()) {
                (*v)->set_var_id (new_var);
              }
            }

            ids.clear();
          }
          if (i == n) {
            break;
          }
          ids.push_back (to_merge[i].id);
        }
      }
    }
};

struct FunctionAndCFG {
  FunctionPtr function;
  CFGCallback *callback;
  FunctionAndCFG() :
    function(),
    callback (NULL) {
  }
  FunctionAndCFG (FunctionPtr function, CFGCallback *callback) :
    function (function),
    callback (callback) {
  }
};
class CFGBeginF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      AUTO_PROF (CFG);
      stage::set_name ("Calc control flow graph");
      stage::set_function (function);

      cfg::CFG cfg;
      CFGCallback *callback = new CFGCallback();
      callback->set_function (function);
      cfg.run (callback);

      if (stage::has_error()) {
        return;
      }

      os << FunctionAndCFG (function, callback);
    }
};

/*** Type inference ***/
class CollectMainEdgesCallback : public CollectMainEdgesCallbackBase {
  private:
    tinf::TypeInferer *inferer_;

  public:
    CollectMainEdgesCallback (tinf::TypeInferer *inferer) :
      inferer_ (inferer) {
    }

    tinf::Node *node_from_rvalue (const RValue &rvalue) {
      if (rvalue.node == NULL) {
        kphp_assert (rvalue.type != NULL);
        return new tinf::TypeNode (rvalue.type);
      } else {
        return rvalue.node;
      }
    }

    virtual void require_node (const RValue &rvalue) {
      if (rvalue.node != NULL) {
        inferer_->add_node (rvalue.node);
      }
    }
    virtual void create_set (const LValue &lvalue, const RValue &rvalue) {
      tinf::Edge *edge = new tinf::Edge();
      edge->from = lvalue.value;
      edge->from_at = lvalue.key;
      edge->to = node_from_rvalue (rvalue);
      inferer_->add_edge (edge);
      inferer_->add_node (edge->from);
    }
    virtual void create_less (const RValue &lhs, const RValue &rhs) {
      tinf::Node *a = node_from_rvalue (lhs);
      tinf::Node *b = node_from_rvalue (rhs);
      inferer_->add_node (a);
      inferer_->add_node (b);
      inferer_->add_restriction (new RestrictionLess (a, b));
    }
};

class TypeInfererF {
  //TODO: extract pattern
  private:
  public:
    TypeInfererF() {
      tinf::register_inferer (new tinf::TypeInferer());
    }

    template <class OutputStreamT>
    void execute (FunctionAndCFG input, OutputStreamT &os) {
      AUTO_PROF (tinf_infer_gen_dep);
      CollectMainEdgesCallback callback (tinf::get_inferer());
      CollectMainEdgesPass pass (&callback);
      run_function_pass (input.function, &pass);
      os << input;
    }

    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {
      //FIXME: rebalance Queues
      vector <Task *> tasks = tinf::get_inferer()->get_tasks();
      for (int i = 0; i < (int)tasks.size(); i++) {
        register_async_task (tasks[i]);
      }
    }
};

class TypeInfererEndF {
  private:
    DataStreamRaw <FunctionAndCFG> tmp_stream;
  public:
    TypeInfererEndF() {
      tmp_stream.set_sink (true);
    }
    template <class OutputStreamT>
    void execute (FunctionAndCFG input, OutputStreamT &os) {
      tmp_stream << input;
    }

    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {
      tinf::get_inferer()->check_restrictions();
      tinf::get_inferer()->finish();

      vector <FunctionAndCFG> all = tmp_stream.get_as_vector();
      for (int i = 0; i < (int)all.size(); i++) {
        os << all[i];
      }
    }
};

/*** Control flow graph. End ***/
class CFGEndF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionAndCFG data, OutputStream &os) {
      AUTO_PROF (CFG_End);
      stage::set_name ("Control flow graph. End");
      stage::set_function (data.function);

      //data.callback->check_uninited();
      data.callback->merge_same_type();
      delete data.callback;

      if (stage::has_error()) {
        return;
      }

      os << data.function;
    }
};

/*** Calc val_ref_flag ***/
class CalcValRefPass : public FunctionPassBase {
  private:
    AUTO_PROF (calc_val_ref);
  public:
    string get_description() {
      return "Calc val ref";
    }
    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }

    struct LocalT : public FunctionPassBase::LocalT {
      bool forbidden;
      bool child_forbidden;
    };

    void on_enter_edge (VertexPtr vertex, LocalT *local, VertexPtr dest_vertex, LocalT *dest_local) {
      if (!local->child_forbidden && dest_vertex->rl_type != val_none && dest_vertex->rl_type != val_error) {
        const TypeData *tp = tinf::get_type (dest_vertex);

        if (tp->ptype() != tp_Unknown && tp->use_or_false()) {
          dest_vertex->val_ref_flag = dest_vertex->rl_type;
        }
      }
    }

    VertexPtr on_enter_vertex (VertexPtr v, LocalT *local) {
      local->forbidden = v->type() == op_eq3 || v->type() == op_neq3 || v->type() == op_eq2 ||
        v->type() == op_neq2 || v->type() == op_set || v->type() == op_return || v->type() == op_conv_bool ||
        (v->type() == op_func_call && !v->get_func_id()->is_extern) || v->type() == op_array ||
        v->type() == op_double_arrow || v->type() == op_var_dump;
      return v;
    }

    template <class VisitT>
    bool user_recursion (VertexPtr v, LocalT *local, VisitT &visit) {
      int n = v->size();
      int ii = 0;
      FOREACH_VERTEX (v, i) {
        bool forbidden_i =
          ((v->type() == op_push_back || v->type() == op_push_back_return || v->type() == op_set_value)
           && ii + 1 == n) ||
          (v->type() == op_list && ii + 1 != n) || (v->type() == op_foreach_param /*&& ii != 0*/);
        local->child_forbidden = local->forbidden || forbidden_i;
        visit (*i);
        ii++;
      }
      return true;
    }
};

class CalcBadVarsF {
  private:
    DataStreamRaw <pair <FunctionPtr, DepData *> > tmp_stream;
  public:
    CalcBadVarsF() {
      tmp_stream.set_sink (true);
    }

    template <class OutputStreamT>
    void execute (FunctionPtr function, OutputStreamT &os) {
      CalcFuncDepPass pass;
      run_function_pass (function, &pass);
      DepData *data = new DepData();
      swap (*data, *pass.get_data_ptr());
      tmp_stream << std::make_pair (function, data);
    }


    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {
      AUTO_PROF (calc_bad_vars);
      stage::set_name ("Calc bad vars (for UB check)");
      vector <pair <FunctionPtr, DepData *> > tmp_vec = tmp_stream.get_as_vector();
      CalcBadVars calc_bad_vars;
      calc_bad_vars.run (tmp_vec);
      FOREACH (tmp_vec, i) {
        delete (*i).second;
        os << (*i).first;
      }
    }
};

/*** C++ undefined behaviour fixes ***/
class CheckUBF {
  public:
    DUMMY_ON_FINISH;
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      AUTO_PROF (check_ub);
      stage::set_name ("Check for undefined behaviour");
      stage::set_function (function);

      if (function->root->type() == op_function) {
        fix_undefined_behaviour (function);
      }

      if (stage::has_error()) {
        return;
      }

      os << function;
    }
};

class FinalCheckPass : public FunctionPassBase {
  private:
    AUTO_PROF (final_check);
  public:
    string get_description() {
      return "Final check";
    }

    bool on_start (FunctionPtr function) {
      if (!FunctionPassBase::on_start (function)) {
        return false;
      }
      return function->type() != FunctionData::func_extern;
    }
    VertexPtr on_enter_vertex (VertexPtr vertex, LocalT *local) {
      if (vertex->type() == op_func_name) {
        kphp_error (0, "Unexpected function name");
      }
      //TODO: may be this should be moved to tinf_check 
      return vertex;
    }
    template <class VisitT>
    bool user_recursion (VertexPtr v, LocalT *local, VisitT &visit) {
      if (v->type() == op_function) {
        visit (v.as <op_function>()->cmd());
        return true;
      }

      if (v->type() == op_func_call || v->type() == op_var ||
          v->type() == op_index) {
        if (v->rl_type == val_r) {
          const TypeData *type = tinf::get_type (v);
          if (type->ptype() == tp_Unknown && !type->use_or_false()) {
            while (v->type() == op_index) {
              v = v.as <op_index>()->array();
            }
            string desc;
            if (v->type() == op_var) {
              desc = "variable [$" + v.as <op_var>()->get_var_id()->name + "]";
            } else if (v->type() == op_func_call) {
              desc = "function [" + v.as <op_func_call>()->get_func_id()->name + "]";
            } else {
              desc = "...";
            }
            kphp_error (0, dl_pstr ("Using Unknown type : %s", desc.c_str()));
            return true;
          }
        }
      }

      return false;
    }
};

/*** CODE GENERATION ***/
//FIXME
map <string, long long> subdir_hash;
string get_subdir (const string &file, const string &base) {
  kphp_assert (G->args().use_subdirs);

  int func_hash = hash (base);
  int bucket = func_hash % 100;

  string subdir = string ("o_") + int_to_str (bucket);

  {
    long long &cur_hash = subdir_hash[subdir];
    cur_hash = cur_hash * 987654321 + hash (file);
  }

  return subdir;
}

void prepare_generate_function (FunctionPtr func) {
  string file_name = func->name;
  for (int i = 0; i < (int)file_name.size(); i++) {
    if (file_name[i] == '$') {
      file_name[i] = '@';
    }
  }

  string file_subdir = func->file_id->short_file_name;

  if (G->args().use_subdirs) {
    func->src_name = file_name + ".cpp";
    func->header_name = file_name + ".h";

    func->subdir = get_subdir (file_name, file_subdir);

    func->src_full_name = func->subdir + "/" + func->src_name;
    func->header_full_name = func->subdir + "/" + func->header_name;
  } else {
    string full_name = file_subdir + "." + file_name;
    func->src_name = full_name + ".cpp";
    func->header_name = full_name + ".h";
    func->subdir = "";
    func->src_full_name = func->src_name;
    func->header_full_name = func->header_name;
  }

  my_unique (&func->static_var_ids);
  my_unique (&func->global_var_ids);
  my_unique (&func->header_global_var_ids);
  my_unique (&func->local_var_ids);
}

set <string> new_subdirs;
template <class OutputStream>
class WriterCallback : public WriterCallbackBase {
  private:
    OutputStream &os;
  public:
    WriterCallback (OutputStream &os, const string dir = "./") :
      os (os) {
    }

    void on_end_write (WriterData *data) {
      if (stage::has_error()) {
        return;
      }

      WriterData *data_copy = new WriterData();
      data_copy->swap (*data);
      data_copy->calc_crc();
      os << data_copy;
    }
};

class CodeGenF {
  //TODO: extract pattern
  private:
    DataStreamRaw <FunctionPtr> tmp_stream;
  public:
    CodeGenF() {
      tmp_stream.set_sink (true);
    }

    template <class OutputStreamT>
    void execute (FunctionPtr input, OutputStreamT &os) {
      tmp_stream << input;
    }

    template <class OutputStreamT>
    void on_finish (OutputStreamT &os) {
      AUTO_PROF (code_gen);

      stage::set_name ("GenerateCode");
      stage::set_file (SrcFilePtr());
      stage::die_if_global_errors();

      vector <FunctionPtr> xall = tmp_stream.get_as_vector();
      sort (xall.begin(), xall.end());

      //TODO: delete W_ptr
      CodeGenerator *W_ptr = new CodeGenerator();
      CodeGenerator &W = *W_ptr;

      if (G->args().use_safe_integer_arithmetic) {
        W.use_safe_integer_arithmetic();
      }

      G->init_dest_dir();

      W.init (new WriterCallback <OutputStreamT> (os));

      for (int i = 0; i < (int)xall.size(); i++) {
        //TODO: parallelize;
        prepare_generate_function (xall[i]);
      }

      vector <SrcFilePtr> main_files = G->get_main_files();
      vector <FunctionPtr> all_functions;
      vector <FunctionPtr> source_functions;
      for (int i = 0; i < (int)xall.size(); i++) {
        FunctionPtr function = xall[i];
        if (function->used_in_source) {
          source_functions.push_back (function);
        }
        if (function->type() == FunctionData::func_extern) {
          continue;
        }
        all_functions.push_back (function);
        W << Async (FunctionH (function));
        W << Async (FunctionCpp (function));
      }

      W << Async (XmainCpp());
      W << Async (InitScriptsH());
      FOREACH (main_files, j) {
        W << Async (DfsInit (*j));
      }
      W << Async (InitScriptsCpp (/*std::move*/main_files, source_functions, all_functions));

      vector <VarPtr> vars = G->get_global_vars();
      int parts_cnt = (int)vars.size() > 1000 ? 64 : 1;
      W << Async (VarsCpp (vars, parts_cnt));
    }
};

class WriteFilesF {
  public:
    DUMMY_ON_FINISH;
    template <class OutputStreamT>
      void execute (WriterData *data, OutputStreamT &os) {
        AUTO_PROF (end_write);
        stage::set_name ("Write files");
        string dir = G->args().dest_dir;

        string cur_file_name = data->file_name;
        string cur_subdir = data->subdir;

        //FIXME:
        if (!cur_subdir.empty() && new_subdirs.insert (cur_subdir).second) {
          string full_path = dir + cur_subdir;
          int ret = mkdir (full_path.c_str(), 0777);
          dl_passert (ret != -1 || errno == EEXIST, full_path.c_str());
          //printf ("create dir %s\n", full_path.c_str());
        }

        string full_file_name = dir;
        if (!cur_subdir.empty()) {
          full_file_name += cur_subdir;
          full_file_name += "/";
        }
        full_file_name += cur_file_name;

        FileInfo *file =  G->get_file_info (full_file_name);
        file->needed = true;

        if (file->on_disk) {
          if (file->crc64 == (unsigned long long)-1) {
            FILE *old_file = fopen (full_file_name.c_str(), "r");
            dl_passert (old_file != NULL, 
                dl_pstr ("Failed to open [%s]", full_file_name.c_str()));
            unsigned long long old_crc;

            if (fscanf (old_file, "//crc64:%Lx", &old_crc) != 1) {
              kphp_warning (dl_pstr ("can't read crc64 from [%s]\n", full_file_name.c_str()));
              old_crc = -1;
            }
            fclose (old_file);

            file->crc64 = old_crc;
          }
        }

        bool need_del = false;
        bool need_fix = false;
        unsigned long long crc = data->calc_crc();
        if (file->on_disk) {
          if (file->crc64 != crc) {
            need_fix = true;
            need_del = true;
          }
        } else {
          need_fix = true;
        }

        if (need_fix) {
          fprintf (stdout, "File [%s] changed\n", full_file_name.c_str());
          string dest_file_name = full_file_name;
          if (need_del) {
            int err = unlink (dest_file_name.c_str());
            dl_passert (err == 0, dl_pstr ("Failed to unlink [%s]", dest_file_name.c_str()));
          }	
          FILE *dest_file = fopen (dest_file_name.c_str(), "w");
          dl_passert (dest_file != NULL, 
              dl_pstr ("Failed to open [%s] for write\n", dest_file_name.c_str()));

          dl_pcheck (fprintf (dest_file, "//crc64:%016Lx\n", ~crc));
          data->dump (dest_file);
          dl_pcheck (fflush (dest_file));
          dl_pcheck (fseek (dest_file, 0, SEEK_SET));
          dl_pcheck (fprintf (dest_file, "//crc64:%016Lx\n", crc));

          dl_pcheck (fflush (dest_file));
          dl_pcheck (fclose (dest_file));

          file->crc64 =  crc;
          file->on_disk = true;

          struct stat st;
          dl_pcheck (stat (dest_file_name.c_str(), &st));
          file->mtime = st.st_mtime;
        }
        delete data;
      }
};

void compiler_execute (CompilerArgs *args) {
  double st = dl_time();
  G = new CompilerCore();
  G->register_args (args);
  G->start();

  //TODO: call it with pthread_once on need
  lexer_init();
  gen_tree_init();
  OpInfo::init_static();
  MultiKey::init_static();
  TypeData::init_static();

  DataStreamRaw <SrcFilePtr> file_stream;

  for (int i = 0; i < (int)args->main_file_names.size(); i++) {
    G->register_main_file (args->main_file_names[i], file_stream);
  }

  {
    SchedulerBase *scheduler;
    if (G->args().threads_number == 1) {
      scheduler = new OneThreadScheduler();
    } else {
      Scheduler *s = new Scheduler();
      s->set_threads_count (G->args().threads_number);
      scheduler = s;
    }

    Pipe <LoadFileF,
         DataStream <SrcFilePtr>,
         DataStream <SrcFilePtr> > load_file_pipe (true);
    Pipe <FileToTokensF,
         DataStream <SrcFilePtr>,
         DataStream <FileAndTokens> > file_to_tokens_pipe (true);
    Pipe <ParseF,
         DataStream <FileAndTokens>,
         DataStream <FunctionPtr> > parse_pipe (true);
    Pipe <ApplyBreakFileF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > apply_break_file_pipe (true);
    Pipe <SplitSwitchF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > split_switch_pipe (true);
    Pipe <CollectRequiredF,
         DataStream <FunctionPtr>,
         DataStreamTriple <ReadyFunctionPtr, SrcFilePtr, FunctionPtr> > collect_required_pipe (true);
    Pipe <FunctionPassF <CalcLocationsPass>,
         DataStream <ReadyFunctionPtr>,
         DataStream <FunctionPtr> > calc_locations_pipe (true);
    FunctionPassPipe <CollectDefinesPass>::Self collect_defines_pipe (true);
    Pipe <SyncPipeF <FunctionPtr>,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > first_sync_pipe (true, true);
    Pipe <PrepareFunctionF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > prepare_function_pipe (true);
    Pipe <SyncPipeF <FunctionPtr>,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > second_sync_pipe (true, true);
    FunctionPassPipe <RegisterDefinesPass>::Self register_defines_pipe (true);
    FunctionPassPipe <PreprocessEq3Pass>::Self preprocess_eq3_pipe (true);
    FunctionPassPipe <PreprocessFunctionCPass>::Self preprocess_function_c_pipe (true);
    FunctionPassPipe <PreprocessBreakPass>::Self preprocess_break_pipe (true);
    FunctionPassPipe <RegisterVariables>::Self register_variables_pipe (true);
    FunctionPassPipe <CalcConstTypePass>::Self calc_const_type_pipe (true);
    FunctionPassPipe <CollectConstVarsPass>::Self collect_const_vars_pipe (true);
    Pipe <CalcThrowEdgesF,
         DataStream <FunctionPtr>,
         DataStream <FunctionAndEdges> > calc_throw_edges_pipe (true);
    Pipe <CalcThrowsF,
         DataStream <FunctionAndEdges>,
         DataStream <FunctionPtr> > calc_throws_pipe (true, true);
    FunctionPassPipe <CheckFunctionCallsPass>::Self check_func_calls_pipe (true);
    Pipe <CalcRLF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > calc_rl_pipe (true);
    Pipe <CFGBeginF,
         DataStream <FunctionPtr>,
         DataStream <FunctionAndCFG> > cfg_begin_pipe (true);
    //tmp
    Pipe <SyncPipeF <FunctionAndCFG>,
         DataStream <FunctionAndCFG>,
         DataStream <FunctionAndCFG> > tmp_sync_pipe (true, true);

    Pipe <TypeInfererF,
         DataStream <FunctionAndCFG>,
         DataStream <FunctionAndCFG> > type_inferer_pipe (true);
    Pipe <TypeInfererEndF,
         DataStream <FunctionAndCFG>,
         DataStream <FunctionAndCFG> > type_inferer_end_pipe (true);
    Pipe <CFGEndF,
         DataStream <FunctionAndCFG>,
         DataStream <FunctionPtr> > cfg_end_pipe (true);
    FunctionPassPipe <OptimizationPass>::Self optimization_pipe (true);
    FunctionPassPipe <CalcValRefPass>::Self calc_val_ref_pipe (true);
    Pipe <CalcBadVarsF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > calc_bad_vars_pipe (true);
    Pipe <CheckUBF,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > check_ub_pipe (true);
    Pipe <SyncPipeF <FunctionPtr>,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > third_sync_pipe (true, true);
    Pipe <SyncPipeF <FunctionPtr>,
         DataStream <FunctionPtr>,
         DataStream <FunctionPtr> > forth_sync_pipe (true, true);
    FunctionPassPipe <FinalCheckPass>::Self final_check_pass (true);
    Pipe <CodeGenF,
         DataStream <FunctionPtr>,
         DataStream <WriterData *> > code_gen_pipe;
    Pipe <WriteFilesF,
         DataStream <WriterData *>,
         EmptyStream> write_files_pipe (false);


    pipe_input (load_file_pipe).set_stream (&file_stream);

    scheduler_constructor (*scheduler, load_file_pipe) >>
      file_to_tokens_pipe >>
      parse_pipe >>
      apply_break_file_pipe >>
      split_switch_pipe >>
      collect_required_pipe >> use_first_output() >>
      calc_locations_pipe >>
      collect_defines_pipe >>
      first_sync_pipe >> sync_node() >>
      prepare_function_pipe >>
      second_sync_pipe >> sync_node() >>
      register_defines_pipe >>
      preprocess_eq3_pipe >>
      preprocess_function_c_pipe >>
      preprocess_break_pipe >>
      calc_const_type_pipe >>
      collect_const_vars_pipe >>
      register_variables_pipe >>
      calc_throw_edges_pipe >>
      calc_throws_pipe >> sync_node() >>
      check_func_calls_pipe >>
      calc_rl_pipe >>
      cfg_begin_pipe >>
      tmp_sync_pipe >> sync_node() >>
      type_inferer_pipe >> sync_node() >>
      type_inferer_end_pipe >> sync_node() >>
      cfg_end_pipe >>
      optimization_pipe >>
      calc_val_ref_pipe >>
      calc_bad_vars_pipe >> sync_node() >>
      check_ub_pipe >>
      final_check_pass >>
      code_gen_pipe >> sync_node() >>
      write_files_pipe;

    scheduler_constructor (*scheduler, collect_required_pipe) >> use_second_output() >>
      load_file_pipe;
    scheduler_constructor (*scheduler, collect_required_pipe) >> use_third_output() >>
      apply_break_file_pipe;

    get_scheduler()->execute();
  }

  int verbosity = G->args().verbosity;
  G->finish();
  if (verbosity > 1) {
    profiler_print_all();
    double en = dl_time();
    double passed = en - st;
    fprintf (stderr, "PASSED: %lf\n", passed);
    mem_info_t mem_info;
    get_mem_stats (getpid(), &mem_info);
    fprintf (stderr, "RSS: %lluKb\n", mem_info.rss);
  }

}
