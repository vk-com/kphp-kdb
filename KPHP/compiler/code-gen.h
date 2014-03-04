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
#include "data.h"
#include "io.h"
#include "vertex.h"

#include "bicycle.h"

struct CGContext {
  vector <string> catch_labels;
  vector <int> catch_label_used;
  FunctionPtr parent_func;
  bool use_safe_integer_arithmetic;
  CGContext() :
    use_safe_integer_arithmetic (false) {
  }
};

struct PlainCode;

class CodeGenerator {
  private:
    TLS <Writer> *master_writer;
    Writer *writer;
    WriterCallbackBase *callback_;
    CGContext context;
    bool own_flag;
  public:
    bool debug_flag;
    CodeGenerator() :
      master_writer (NULL),
      writer (NULL),
      callback_ (NULL),
      context(),
      own_flag (false),
      debug_flag (false) {
      }
    explicit CodeGenerator (const CodeGenerator &from) :
      master_writer (from.master_writer),
      writer (NULL),
      callback_ (from.callback_),
      context (from.context),
      own_flag (false),
      debug_flag (false) {
      }

    void init (WriterCallbackBase *new_callback) {
      master_writer = new TLS <Writer>();
      callback_ = new_callback;
      own_flag = true;
    }
    void clear() {
      if (own_flag) {
        delete master_writer;
        delete callback_;
        own_flag = false;
      }
      master_writer = NULL;
      callback_ = NULL;
    }

    ~CodeGenerator() {
      clear();
    }

    void use_safe_integer_arithmetic (bool flag = true) {
      context.use_safe_integer_arithmetic = flag;
    }

    WriterCallbackBase *callback() {
      return callback_;
    }

    inline void lock_writer() {
      assert (writer == NULL);
      writer = master_writer->lock_get();
    }

    inline void unlock_writer() {
      assert (writer != NULL);
      master_writer->unlock_get (writer);
      writer = NULL;
    }


    inline CodeGenerator &operator << (const char *s);
    inline CodeGenerator &operator << (const string &s);
    inline CodeGenerator &operator << (const string_ref &s);

    template <Operation Op>
      inline CodeGenerator &operator << (VertexAdaptor <Op> vertex);
    template <class T> CodeGenerator &operator << (const T &value);

    inline Writer &get_writer();
    inline CGContext &get_context();
};

class ScopedDebug {
  private:
    CodeGenerator *cg_;
    ScopedDebug (const ScopedDebug &);
    ScopedDebug &operator = (const ScopedDebug &);
  public:
    ScopedDebug (CodeGenerator &cg) :
      cg_ (&cg) {
      cg_->debug_flag = true;
    }
    ~ScopedDebug() {
      cg_->debug_flag = false;
    }
};

inline void compile_vertex (VertexPtr, CodeGenerator &W);

#define NL NewLine()
#define BEGIN OpenBlock()
#define END CloseBlock()

template <class T>
struct AsyncImpl {
  const T &cmd;
  AsyncImpl (const T &cmd);
  void compile (CodeGenerator &W) const;
};
template <class T>
inline AsyncImpl <T> Async (const T &cmd);

struct LockComments {
  inline void compile (CodeGenerator &W) const;
};
struct UnlockComments {
  inline void compile (CodeGenerator &W) const;
};

struct OpenFile {
  string file_name;
  string subdir;
  inline OpenFile (const string &file_name, const string &subdir = "");
  inline void compile (CodeGenerator &W) const;
};

struct CloseFile {
  inline void compile (CodeGenerator &W) const;
};

struct ClearLocation {
  inline void compile (CodeGenerator &W) const;
};

struct UpdateLocation {
  const Location &location;
  inline UpdateLocation (const Location &location);
  inline void compile (CodeGenerator &W) const;
};

struct NewLine {
  inline void compile (CodeGenerator &W) const;
};

struct Indent {
  int val;
  Indent (int val);
  inline void compile (CodeGenerator &W) const;
};

struct PlainCode {
  string_ref str;
  inline PlainCode (const char *s);
  inline PlainCode (const string &s);
  inline PlainCode (const string_ref &s);
  inline void compile (CodeGenerator &W) const;
};

struct OpenBlock {
  inline void compile (CodeGenerator &W) const;
};

struct CloseBlock {
  inline void compile (CodeGenerator &W) const;
};

struct Include {
  const PlainCode &plain_code;
  Include (const PlainCode &plain_code);
  inline void compile (CodeGenerator &W) const;
private:
  DISALLOW_COPY_AND_ASSIGN (Include);
};

struct FunctionName {
  FunctionPtr function;
  inline FunctionName (FunctionPtr function);
  inline void compile (CodeGenerator &W) const;
};

struct VarName {
  VarPtr var;
  inline VarName (VarPtr var);
  inline void compile (CodeGenerator &W) const;
};

struct TypeName {
  const TypeData *type;
  inline TypeName (const TypeData *type);
  inline void compile (CodeGenerator &W) const;
};

struct FunctionCallFlag {
  FunctionPtr function;
  inline FunctionCallFlag (FunctionPtr function);
  inline void compile (CodeGenerator &W) const;
};

struct FunctionDeclaration {
  FunctionPtr function;
  bool in_header;
  inline FunctionDeclaration (FunctionPtr function, bool in_header = false);
  inline void compile (CodeGenerator &W) const;
};

struct VarDeclaration {
  VarPtr var;
  bool extern_flag;
  inline VarDeclaration (VarPtr var, bool extern_flag = false);
  inline void compile (CodeGenerator &W) const;
};

struct Function {
  FunctionPtr function;
  bool in_header;
  inline Function (FunctionPtr function, bool in_header = false);
  inline void compile (CodeGenerator &W) const;
};

VarDeclaration VarExternDeclaration (VarPtr var);

struct Operand {
  VertexPtr root;
  Operation parent_type;
  bool is_left;
  inline Operand (VertexPtr root, Operation parent_type, bool is_left);
  inline void compile (CodeGenerator &W) const;
};

struct LabelName {
  int label_id;
  inline LabelName (int label_id);
  inline void compile (CodeGenerator &W) const;
};

struct Label {
  int label_id;
  inline Label (int label_id);
  inline void compile (CodeGenerator &W) const;
};

struct AsList {
  VertexPtr root;
  string delim;
  inline AsList (VertexPtr root, string delim);
  inline void compile (CodeGenerator &W) const;
};

struct AsSeq {
  VertexPtr root;
  inline AsSeq (VertexPtr root);
  inline void compile (CodeGenerator &W) const;
};

struct CycleBody {
  VertexPtr body;
  int continue_label_id;
  int break_label_id;
  inline CycleBody (VertexPtr body, int continue_label_id, int break_label_id);
  inline void compile (CodeGenerator &W) const;
};
struct VertexCompiler {
  VertexPtr vertex;
  inline VertexCompiler (VertexPtr vertex);
  inline void compile (CodeGenerator &W) const;
};

struct InitVar {
  VarPtr var;
  inline InitVar (VarPtr var);
  inline void compile (CodeGenerator &W) const;
};

struct FunctionStaticInit {
  FunctionPtr function;
  bool in_header;
  inline FunctionStaticInit (FunctionPtr function, bool in_header = false);
  inline void compile (CodeGenerator &W) const;
};

struct StaticInit {
  const vector <FunctionPtr> &all_functions;
  inline StaticInit (const vector <FunctionPtr> &all_functions);
  inline void compile (CodeGenerator &W) const;
};

struct InitScriptsH {
  inline void compile (CodeGenerator &W) const;
};

struct InitScriptsCpp {
  vector <SrcFilePtr> main_file_ids;
  vector <FunctionPtr> source_functions;
  vector <FunctionPtr> all_functions;
  inline InitScriptsCpp (
    vector <SrcFilePtr> &main_file_ids,
    vector <FunctionPtr> &source_functions,
    vector <FunctionPtr> &all_functions);
  inline void compile (CodeGenerator &W) const;
};

struct InitFuncPtrs {
  const vector <FunctionPtr> &ids;
  inline InitFuncPtrs (const vector <FunctionPtr> &ids);
  inline void compile (CodeGenerator &W) const;
};

struct RunFunction {
  FunctionPtr function;
  bool in_header;
  inline RunFunction (FunctionPtr function, bool in_header = false);
  inline void compile (CodeGenerator &W) const;
};

struct XmainCpp {
  inline void compile (CodeGenerator &W) const;
};

struct VarsCppPart {
  int file_num;
  const vector <VarPtr> &vars;
  inline VarsCppPart (int file_num, const vector <VarPtr> &vars);
  inline void compile (CodeGenerator &W) const;
private:
  DISALLOW_COPY_AND_ASSIGN (VarsCppPart);
};

struct VarsCpp {
  vector <VarPtr> vars;
  int parts_cnt;
  inline VarsCpp (vector <VarPtr> &vars, int parts_cnt);
  inline void compile (CodeGenerator &W) const;
//private:
  //DISALLOW_COPY_AND_ASSIGN (VarsCpp);
};
struct DfsInit {
  SrcFilePtr main_file;
  inline DfsInit (SrcFilePtr main_file);
  static inline void compile_dfs_init_part (
      FunctionPtr func,
      const set <VarPtr> &used_vars, bool full_flag,
      int part_i, CodeGenerator &W);
  static inline void compile_dfs_init_func (
      FunctionPtr func, const set <FunctionPtr> &used_functions,
      bool full_flag, const vector <string> &header_names, CodeGenerator &W);
  static inline void collect_used_funcs_and_vars (
      FunctionPtr func,
      set <FunctionPtr> *visited_functions,
      set <VarPtr> *used_vars,
      int used_vars_cnt);
  inline void compile (CodeGenerator &W) const;
};


struct FunctionH {
  FunctionPtr function;
  inline FunctionH (FunctionPtr function);
  inline void compile (CodeGenerator &W) const;
};
struct FunctionCpp {
  FunctionPtr function;
  inline FunctionCpp (FunctionPtr function);
  inline void compile (CodeGenerator &W) const;
};

inline void compile_prefix_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W);
inline void compile_postfix_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W);
inline void compile_conv_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W);
inline void compile_noerr (VertexAdaptor <op_noerr> root, CodeGenerator &W);
inline void compile_binary_func_op (VertexAdaptor <meta_op_binary_op> root, CodeGenerator &W);
inline void compile_binary_op (VertexAdaptor <meta_op_binary_op> root, CodeGenerator &W);
inline void compile_ternary_op (VertexAdaptor <op_ternary> root, CodeGenerator &W);
inline void compile_if (VertexAdaptor <op_if> root, CodeGenerator &W);
inline void compile_while (VertexAdaptor <op_while> root, CodeGenerator &W);
inline void compile_do (VertexAdaptor <op_do> root, CodeGenerator &W);
inline void compile_require (VertexPtr root, CodeGenerator &W);
inline void compile_return (VertexAdaptor <op_return> root, CodeGenerator &W);
inline void compile_for (VertexAdaptor <op_for> root, CodeGenerator &W);
inline void compile_throw_fast_action (CodeGenerator &W);
inline void compile_throw (VertexAdaptor <op_throw> root, CodeGenerator &W);
inline void compile_throw_fast (VertexAdaptor <op_throw> root, CodeGenerator &W);
inline void compile_try (VertexAdaptor <op_try> root, CodeGenerator &W);
inline void compile_try_fast (VertexAdaptor <op_try> root, CodeGenerator &W);
inline void compile_foreach (VertexAdaptor <op_foreach> root, CodeGenerator &W);
struct CaseInfo;
inline void compile_switch_str (VertexAdaptor <op_switch> root, CodeGenerator &W);
inline void compile_switch_int (VertexAdaptor <op_switch> root, CodeGenerator &W);
inline void compile_switch_var (VertexAdaptor <op_switch> root, CodeGenerator &W);
inline void compile_switch (VertexAdaptor <op_switch> root, CodeGenerator &W);
inline void compile_function (VertexPtr root, CodeGenerator &W);
inline void compile_string_build_raw (VertexAdaptor <op_string_build> root, CodeGenerator &W);
inline void compile_index (VertexAdaptor <op_index> root, CodeGenerator &W);
inline void compile_as_printable (VertexPtr root, CodeGenerator &W);
inline void compile_echo (VertexPtr root, CodeGenerator &W);
inline void compile_var_dump (VertexPtr root, CodeGenerator &W);
inline void compile_print (VertexAdaptor <op_print> root, CodeGenerator &W);
inline void compile_store_many (VertexAdaptor <op_store_many> root, CodeGenerator &W);
inline void compile_pack (VertexAdaptor <op_pack> root, CodeGenerator &W);
inline void compile_xprintf (VertexAdaptor <meta_op_xprintf> root, CodeGenerator &W);
inline void compile_xset (VertexAdaptor <meta_op_xset> root, CodeGenerator &W);
inline void compile_list (VertexAdaptor <op_list> root, CodeGenerator &W);
inline void compile_array (VertexAdaptor <op_array> root, CodeGenerator &W);
inline void compile_func_call_fast (VertexAdaptor <op_func_call> root, CodeGenerator &W);
inline void compile_func_call (VertexAdaptor <op_func_call> root, CodeGenerator &W, int fix = 0);
inline void compile_func_ptr (VertexAdaptor <op_func_ptr> root, CodeGenerator &W);
inline void compile_define (VertexPtr root, CodeGenerator &W);
inline void compile_defined (VertexPtr root, CodeGenerator &W);
inline void compile_safe_version (VertexPtr root, CodeGenerator &W);
inline void compile_set_value (VertexAdaptor <op_set_value> root, CodeGenerator &W);
inline void compile_push_back (VertexAdaptor <op_push_back> root, CodeGenerator &W);
inline void compile_push_back_return (VertexAdaptor <op_push_back_return> root, CodeGenerator &W);
void compile_string_raw (const string &str, CodeGenerator &W);
inline void compile_string_raw (VertexAdaptor <op_string> root, CodeGenerator &W);
inline void compile_string (VertexAdaptor <op_string> root, CodeGenerator &W);
inline void compile_string_build (VertexPtr root, CodeGenerator &W);
inline void compile_new (VertexPtr root, CodeGenerator &W);
inline void compile_break_continue (VertexAdaptor <meta_op_goto> root, CodeGenerator &W);
inline void compile_conv_array_l (VertexAdaptor <op_conv_array_l> root, CodeGenerator &W);
inline void compile_conv_int_l (VertexAdaptor <op_conv_int_l> root, CodeGenerator &W);
inline void compile_cycle_op (VertexPtr root, CodeGenerator &W);
inline void compile_min_max (VertexPtr root, CodeGenerator &W);
inline void compile_common_op (VertexPtr root, CodeGenerator &W);
inline void compile (VertexPtr root, CodeGenerator &W);

/*** Implementation ***/
inline CodeGenerator &CodeGenerator::operator << (const char *s) {
  return (*this) << PlainCode (s);
}
inline CodeGenerator &CodeGenerator::operator << (const string &s) {
  return (*this) << PlainCode (s);
}
inline CodeGenerator &CodeGenerator::operator << (const string_ref &s) {
  return (*this) << PlainCode (s);
}
template <Operation Op>
inline CodeGenerator &CodeGenerator::operator << (VertexAdaptor <Op> vertex) {
  return (*this) << VertexCompiler (vertex);
}
template <class T> CodeGenerator &CodeGenerator::operator << (const T &value) {
  value.compile (*this);
  return *this;
}

inline Writer &CodeGenerator::get_writer() {
  assert (writer != NULL);
  return *writer;
}
inline CGContext &CodeGenerator::get_context() {
  return context;
}

template <class T>
inline AsyncImpl <T>::AsyncImpl (const T &cmd) :
  cmd (cmd) {
}

template <class T>
class CodeGenTask : public Task {
  private:
    CodeGenerator W;
    T cmd;
  public:
    CodeGenTask (CodeGenerator &W, const T& cmd) :
      W (W),
      cmd (cmd) {
    }
    void execute() {
      PROF(writer).start();
      stage::set_name ("Async code generation");
      W << cmd;
      PROF(writer).finish();
    }
};

template <class T>
inline CodeGenTask <T> *create_async_task (CodeGenerator &W, const T &cmd) {
  return new CodeGenTask <T> (W, cmd);
}

template <class T>
inline void AsyncImpl <T>::compile (CodeGenerator &W) const {
  Task *task = create_async_task (W, cmd);
  register_async_task (task);
}

template <class T>
inline AsyncImpl <T> Async (const T &cmd) {
  return AsyncImpl <T> (cmd);
}

inline void LockComments::compile (CodeGenerator &W) const {
  W.get_writer().lock_comments();
}
inline void UnlockComments::compile (CodeGenerator &W) const {
  W.get_writer().unlock_comments();
}

inline OpenFile::OpenFile (const string &file_name, const string &subdir) :
  file_name (file_name),
  subdir (subdir) {
}
inline void OpenFile::compile (CodeGenerator &W) const {
  W.lock_writer();
  W.get_writer().set_callback (W.callback());
  W.get_writer().begin_write();
  W.get_writer().set_file_name (file_name, subdir);
}

inline void CloseFile::compile (CodeGenerator &W) const {
  W.get_writer().end_write();
  W.unlock_writer();
}

inline void ClearLocation::compile (CodeGenerator &W) const {
  stage::set_location (Location());
  W << UpdateLocation (Location());
}

inline UpdateLocation::UpdateLocation (const Location &location) :
  location (location) {
}
inline void UpdateLocation::compile (CodeGenerator &W) const {
  if (!W.get_writer().is_comments_locked()) {
    stage::set_location (location);
    W.get_writer() (stage::get_file(), stage::get_line());
  }
}

inline void NewLine::compile (CodeGenerator &W) const {
  if (W.debug_flag) {
    fprintf (stderr, "\n");
    return;
  }
  W.get_writer().new_line();
}

inline Indent::Indent (int val) :
  val (val) {
}
inline void Indent::compile (CodeGenerator &W) const {
  W.get_writer().indent (val);
}

inline PlainCode::PlainCode (const char *s) :
  str (s, s + strlen (s)) {
}
inline PlainCode::PlainCode (const string &s) :
  str (&s[0], &s[0] + s.size()) {
}
inline PlainCode::PlainCode (const string_ref &s) :
  str (s) {
}
inline void PlainCode::compile (CodeGenerator &W) const {
  if (W.debug_flag) {
    fprintf (stderr, "%s", str.c_str());
    return;
  }
  W.get_writer() (str);
}


inline void OpenBlock::compile (CodeGenerator &W) const {
  W << "{" << NL << Indent (+2);
}

inline void CloseBlock::compile (CodeGenerator &W) const {
  W << Indent (-2) << "}";
}

inline Include::Include (const PlainCode &plain_code) :
  plain_code (plain_code) {
}
inline void Include::compile (CodeGenerator &W) const {
  W << "#include \"" << plain_code << "\"" << NL;
}

inline FunctionName::FunctionName (FunctionPtr function) :
  function (function) {
}
void FunctionName::compile (CodeGenerator &W) const {
  W << "f$";
  if (W.get_context().use_safe_integer_arithmetic && function->name == "intval") {
    W << "safe_intval";
  } else {
    W << function->name;
  }
}

inline VarName::VarName (VarPtr var) :
  var (var) {
}
void VarName::compile (CodeGenerator &W) const {
  if (var->static_id.not_null()) {
    W << FunctionName (var->static_id) << "$";
  }

  W << "v$" << var->name;
}

inline TypeName::TypeName (const TypeData *type) :
  type (type) {
}
inline void TypeName::compile (CodeGenerator &W) const {
  W << type_out (type);
}


inline FunctionCallFlag::FunctionCallFlag (FunctionPtr function) :
  function (function) {
}
inline void FunctionCallFlag::compile (CodeGenerator &W) const {
  W << FunctionName (function) << "$called";
}

inline FunctionDeclaration::FunctionDeclaration (FunctionPtr function, bool in_header) :
  function (function),
  in_header (in_header) {
}

inline void FunctionDeclaration::compile (CodeGenerator &W) const {
  VertexAdaptor <meta_op_function> root = function->root;
  assert (root->type() == op_function);

  W << TypeName (tinf::get_type (function, -1)) << " " << FunctionName (function) << "(";
  if (function->varg_flag) {
    W << "array <var> VA_LIST";
  } else {
    bool first = true;
    int ii = 0;
    VertexAdaptor <op_func_param_list> params = root->params();
    FOREACH (params->params(), i) {
      if ((*i)->type() == op_func_param) {
        assert ("functions with callback are not supported");
      }

      VertexAdaptor <op_func_param> param = *i;
      VertexPtr var = param->var();
      VertexPtr def_val;
      if (param->has_default()) {
        def_val = param->default_value();
      }

      if (first) {
        first = false;
      } else {
        W << ", ";
      }
      W << TypeName (tinf::get_type (function, ii)) << " ";
      if (var->ref_flag) {
        W << "&";
      }
      W << VarName (var->get_var_id());

      if (def_val.not_null() && in_header) {
        W << " = " << def_val;
      }

      ii++;
    }
  }
  W << ")";
}


inline VarDeclaration::VarDeclaration (VarPtr var, bool extern_flag) :
  var (var),
  extern_flag (extern_flag) {
}
void VarDeclaration::compile (CodeGenerator &W) const {
  const TypeData *type = tinf::get_type (var);

  W << (extern_flag ? "extern " : "") <<
       TypeName (type) << " " <<
       VarName (var);

  if (!extern_flag) {
    if (type->ptype() == tp_float || type->ptype() == tp_int) {
      W << " = 0";
    } else if (type->ptype() == tp_bool ||
               (type->ptype() == tp_Unknown && type->use_or_false())) {
      W << " = false";
    }
  }
  W << ";" << NL;
}
inline VarDeclaration VarExternDeclaration (VarPtr var) {
  return VarDeclaration (var, true);
}

inline Function::Function (FunctionPtr function, bool in_header) :
  function (function),
  in_header (in_header) {
}

inline void Function::compile (CodeGenerator &W) const {
  if (in_header) {
    W << FunctionDeclaration (function, in_header) << ";";
  } else {
    W << function->root;
  }
  W << NL;
}

inline VertexCompiler::VertexCompiler (VertexPtr vertex) :
  vertex (vertex) {
}

inline void VertexCompiler::compile (CodeGenerator &W) const {
  compile_vertex (vertex, W);
}


inline InitFuncPtrs::InitFuncPtrs (const vector <FunctionPtr> &ids) :
  ids (ids) {
}
inline void InitFuncPtrs::compile (CodeGenerator &W) const {
  vector <string> names;

  int n = (int)ids.size();

  for (int i = 0; i < n; i++) {
    FunctionPtr f = ids[i];
    if (ids[i].not_null() && (ids[i]->root.is_null() || !ids[i]->is_required)) {
      //skip
    } else {
      kphp_assert (f->root->type() == op_function);
      W << Include (f->header_full_name);
    }
  }

  W << "void init_func_ptrs ()" <<
       BEGIN;

  for (int i = 0; i < n; i++) {
    FunctionPtr f = ids[i];
    //FIXME: copypast %(
    if (ids[i].not_null() && (ids[i]->root.is_null() || !ids[i]->is_required)) {
      //skip
    } else {
      W << f->name << "_pointer = " << FunctionName (f) << ";" << NL;
    }
  }
  W << END << NL;
}

inline RunFunction::RunFunction (FunctionPtr function, bool in_header) :
  function (function),
  in_header (in_header) {
}

inline void RunFunction::compile (CodeGenerator &W) const {
  W << "void " << FunctionName (function) << "$run (php_query_data *data, void *mem, size_t mem_size)";
  if (!in_header) {
    W << " " <<
         BEGIN <<
           "dl::allocator_init (mem, mem_size);" << NL <<
           "init_static();" << NL <<
           "drivers_init_static();" << NL <<
           FunctionName (function) << "$dfs_init();" << NL <<
           "init_superglobals (data);" << NL <<
           "TRY_CALL_VOID (void, " << FunctionName (function) << "());" << NL <<
           "finish (0);" << NL <<
         END;
  } else {
    W << ";";
  }
  W << NL;
}

inline void InitScriptsH::compile (CodeGenerator &W) const {
  W << OpenFile ("init_scripts.h");

  W << "#ifdef  __cplusplus" << NL <<
       "  extern \"C\" {" << NL <<
       "#endif" << NL;

  W << "void static_init_scripts (void);" << NL;
  W << "void init_scripts (void);" << NL;

  W << "#ifdef  __cplusplus" << NL <<
       "  }" << NL <<
       "#endif" << NL;

  W << CloseFile();
}

inline InitScriptsCpp::InitScriptsCpp (
    vector <SrcFilePtr> &new_main_file_ids,
    vector <FunctionPtr> &new_source_functions,
    vector <FunctionPtr> &new_all_functions) :
  main_file_ids(),
  source_functions(),
  all_functions() {
  std::swap (main_file_ids, new_main_file_ids);
  std::swap (source_functions, new_source_functions);
  std::swap (all_functions, new_all_functions);
}

inline void InitScriptsCpp::compile (CodeGenerator &W) const {
  W << OpenFile ("init_scripts.cpp");

  W << Include ("php_functions.h") <<
    Include ("php_script.h") <<
    Include ("init_scripts.h");

  FOREACH (main_file_ids, i) {
    FunctionPtr main_function = (*i)->main_function;
    W << Include (main_function->header_full_name) <<
         Include ("dfs." + main_function->header_name);
  }

  W << "extern string_buffer SB;" << NL;

  W << InitFuncPtrs (source_functions);
  W << StaticInit (all_functions);

  FOREACH (main_file_ids, i) {
    W << RunFunction ((*i)->main_function);
  }

  W << "void init_scripts (void)" <<
       BEGIN <<
         "init_func_ptrs();" << NL;

  FOREACH (main_file_ids, i) {
    W << "set_script (" <<
            "\"@" << (*i)->short_file_name << "\", " <<
            FunctionName ((*i)->main_function) << "$run, " <<
            FunctionName ((*i)->main_function) << "$dfs_clear);" << NL;
  }

  W << END;

  W << CloseFile();
}


inline void XmainCpp::compile (CodeGenerator &W) const {
  W << OpenFile ("xmain.cpp") <<
       "#include \"php_functions.h\"" << NL <<
       "#include \"php_script.h\"" << NL <<
       "#include \"init_scripts.h\"" << NL <<
       "#include \"dl-utils-lite.h\"" << NL <<
       "static char memory_buffer[1 << 29];" << NL <<
       "int main (void) " <<
       BEGIN <<
         "dl_set_default_handlers();" << NL <<
         "static_init_scripts();" << NL <<
         "init_scripts();" << NL <<
         "script_t *script = get_script (\"#0\");" << NL <<
#ifdef FAST_EXCEPTIONS
         "script->run (NULL, memory_buffer, 1 << 29);" <<  NL <<
         "if (CurException) " <<
         BEGIN <<
          "Exception e = *CurException;" << NL <<
#else
         "try " <<
         BEGIN <<
           "script->run (NULL, memory_buffer, 1 << 29);" << NL <<
         END <<
         "catch (Exception &e) " <<
         BEGIN <<
#endif
          "fprintf ("
            "stderr, "
            "\"Unhandled Exception caught in file %s at line %d. Error %d: %s.\\n\", "
            "e.file.c_str(), e.line, e.code, e.message.c_str()"
          ");" << NL <<
          "fprintf (stderr, \"Backtrace:\\n%s\", f$exception_getTraceAsString (e).c_str());" << NL <<
         END << NL <<
         "script->clear();" << NL <<
       END << NL <<
       CloseFile();
}

inline VarsCppPart::VarsCppPart (int file_num, const vector <VarPtr> &vars) :
  file_num (file_num),
  vars (vars) {
}
inline void VarsCppPart::compile (CodeGenerator &W) const {
  string file_name = string ("vars") + int_to_str (file_num) + ".cpp";
  W << OpenFile (file_name);

  W << Include ("php_functions.h");

  vector <VarPtr> const_string_vars;
  vector <VarPtr> const_regexp_vars;
  FOREACH (vars, var) {
    W << VarDeclaration (*var);
    if ((*var)->type ()== VarData::var_const_t &&
        (*var)->global_init_flag) {
      if ((*var)->init_val->type() == op_string) {
        const_string_vars.push_back (*var);
      } else if ((*var)->init_val->type() == op_conv_regexp) {
        const_regexp_vars.push_back (*var);
      } else {
        kphp_fail();
      }
    }
  }

  if (file_num == 0) {
    W << "string_buffer SB;" << NL;
  }

  string raw_data;
  vector <int> const_string_shifts (const_string_vars.size());
  vector <int> const_string_length (const_string_vars.size());
  int ii = 0;
  FOREACH (const_string_vars, var) {
    int shift_to_align = (((int)raw_data.size() + 7) & -8) - (int)raw_data.size();
    if (shift_to_align != 0) {
      raw_data.append (shift_to_align, 0);
    }
    const string &s = (*var)->init_val.as <op_string>()->get_string();
    int raw_len = string_raw_len (s.size());
    kphp_assert (raw_len != -1);
    const_string_shifts[ii] = (int)raw_data.size();
    raw_data.append (raw_len, 0);
    int err = string_raw (&raw_data[const_string_shifts[ii]], raw_len, s.c_str(), (int)s.size());
    kphp_assert (err == raw_len);
    const_string_length[ii] = (int)raw_data.size() - const_string_shifts[ii];
    ii++;
  }

  W << "static const char *raw = ";
  compile_string_raw (raw_data, W);
  W << ";" << NL;
  W << "void const_vars_init" << int_to_str (file_num) << "()";
  W << BEGIN;
  ii = 0;
  FOREACH (const_string_vars, var) {
    W << VarName (*var) << ".assign_raw (&raw[" << int_to_str (const_string_shifts[ii]) << "]);" << NL;
    ii++;
  }
  FOREACH (const_regexp_vars, var) {
    W << InitVar (*var);
  }
  W << END;

  W << CloseFile();
}

inline VarsCpp::VarsCpp (vector <VarPtr> &new_vars, int parts_cnt) :
  vars(),
  parts_cnt (parts_cnt) {
  std::swap (vars, new_vars);
}

inline void VarsCpp::compile (CodeGenerator &W) const {
  kphp_assert (1 <= parts_cnt && parts_cnt <= 128);
  vector <vector <VarPtr> > vcpp (parts_cnt);

  FOREACH (vars, i) {
    int vi = (unsigned)hash ((*i)->name) % parts_cnt;
    vcpp[vi].push_back (*i);
  }

  for (int i = 0; i < parts_cnt; i++) {
    sort (vcpp[i].begin(), vcpp[i].end());
    W << VarsCppPart (i, vcpp[i]);
  }

  W << OpenFile ("vars.cpp");
  for (int i = 0; i < parts_cnt; i++) {
    W << "void const_vars_init" << int_to_str (i) << "();" << NL;
  }

  W << "void const_vars_init()" << BEGIN;
  for (int i = 0; i < parts_cnt; i++) {
    W << "const_vars_init" << int_to_str (i) << "();" << NL;
  }
  W << END;
  W << CloseFile();
  
}

inline InitVar::InitVar (VarPtr var) :
  var (var) {
}
inline void InitVar::compile (CodeGenerator &W) const {
  Location save_location = stage::get_location();
  W << UnlockComments();

  VertexPtr init_val = var->init_val;
  if (init_val->type() == op_conv_regexp) {
    W << VarName (var) << ".init (" << var->init_val << ");" << NL;
  } else {
    W << VarName (var) << " = " << var->init_val << ";" << NL;
  }

  W << LockComments();
  stage::set_location (save_location);
}

inline FunctionStaticInit::FunctionStaticInit (FunctionPtr function, bool in_header) :
  function (function),
  in_header (in_header) {
}
void FunctionStaticInit::compile (CodeGenerator &W) const {
  W << "void " << FunctionName (function) << "$static_init (void)";
  if (in_header) {
    W << ";" << NL;
    return;
  }

  W << " " << BEGIN;
  FOREACH (function->const_var_ids, i) {
    VarPtr var = *i;
    if (var->global_init_flag) {
      continue;
    }
    W << InitVar (var);
  }
  FOREACH (function->header_const_var_ids, i) {
    VarPtr var = *i;
    if (var->global_init_flag) {
      continue;
    }
    W << InitVar (var);
  }
  W << END << NL;
}

inline StaticInit::StaticInit (const vector <FunctionPtr> &all_functions) :
  all_functions (all_functions) {
}
inline void StaticInit::compile (CodeGenerator &W) const {
  FOREACH (all_functions, i) {
    FunctionPtr to = *i;
    W << Include (to->header_full_name);
  }
  W << "void const_vars_init();" << NL;

  W << "void static_init_scripts (void)";
  W << " " << BEGIN;
  W << "regexp::init_static();" << NL;
  FOREACH (all_functions, i) {
    FunctionPtr to = *i;
    W << FunctionName (to) << "$static_init();" << NL;
  }
  W << "const_vars_init();" << NL;
  W << "dl::allocator_init (NULL, 0);" << NL;
  W << END << NL;
}

DfsInit::DfsInit (SrcFilePtr main_file) :
  main_file (main_file) {
}

void DfsInit::compile_dfs_init_part (
    FunctionPtr func,
    const set <VarPtr> &used_vars, bool full_flag,
    int part_i, CodeGenerator &W) {

  if (full_flag) {
    FOREACH (used_vars, var) {
      W << VarExternDeclaration (*var);
    }
  }
  W << "void " << FunctionName (func) << "$dfs_init" << int_to_str (part_i) << " (void)";

  if (full_flag) {
    W << " " << BEGIN;

    FOREACH (used_vars, i) {
      VarPtr var = *i;
      if (var->is_constant) {
        continue;
      }

      const TypeData *tp = tinf::get_type (var);

      W << "INIT_VAR (" << TypeName (tp) << ", " << VarName (var) << ");" << NL;
      //FIXME: brk and comments
      if (var->init_val.not_null()) {
        W << UnlockComments();
        W << VarName (var) << " = " << var->init_val << ";" << NL;
        W << LockComments();
      }
    }

    W << END;
  } else {
    W << ";";
  }
  W << NL;


  W << "void " << FunctionName (func) << "$dfs_clear" << int_to_str (part_i) << "(void)";
  if (full_flag) {
    W << " " << BEGIN;

    FOREACH (used_vars, i) {
      VarPtr var = *i;
      if (var->is_constant) {
        continue;
      }

      const TypeData *type = tinf::get_type (var);

      W << "CLEAR_VAR (" << TypeName (type) << ", " << VarName (var) << ");" << NL;
    }

    W << END;
  } else {
    W << ";";
  }
  W << NL;
}

void DfsInit::compile_dfs_init_func (
    FunctionPtr func, const set <FunctionPtr> &used_functions,
    bool full_flag, const vector <string> &header_names, CodeGenerator &W) {

  int parts_n = (int)header_names.size();
  if (full_flag) {
    for (int i = 0; i < parts_n; i++) {
      W << Include (header_names[i]);
    }
    FOREACH (used_functions, i) {
      FunctionPtr func = *i;
      if (func->type() == FunctionData::func_global) {
        W << Include (func->header_full_name);
      }
    }
  }

  W << "void " << FunctionName (func) << "$dfs_init (void)";
  if (full_flag) {
    W << " " << BEGIN;

    FOREACH (used_functions, i) {
      FunctionPtr func = *i;
      if (func->type() == FunctionData::func_global) {
        W << FunctionCallFlag (func) << " = false;" << NL;
      }
    }

    for (int i = 0; i < parts_n; i++) {
      W << FunctionName (func) << "$dfs_init" << int_to_str (i) << "();" << NL;
    }

    W << END;
  } else {
    W << ";";
  }
  W << NL;

  W << "void " << FunctionName (func) << "$dfs_clear (void)";
  if (full_flag) {
    W << " " << BEGIN;

    for (int i = 0; i < parts_n; i++) {
      W << FunctionName (func) << "$dfs_clear" << int_to_str (i) << "();" << NL;
    }

    W << END;
  } else {
    W << ";";
  }
  W << NL;
}

void DfsInit::collect_used_funcs_and_vars (
    FunctionPtr func,
    set <FunctionPtr> *visited_functions,
    set <VarPtr> *used_vars,
    int used_vars_cnt) {
  for (int i = 0, ni = (int)func->dep.size(); i < ni; i++) {
    FunctionPtr to = func->dep[i];
    if (visited_functions->insert (to).second) {
      collect_used_funcs_and_vars (to, visited_functions, used_vars, used_vars_cnt);
    }
  }

  int func_hash = hash (func->name);
  int bucket = func_hash % used_vars_cnt;

  used_vars[bucket].insert (func->global_var_ids.begin(), func->global_var_ids.end());
  used_vars[bucket].insert (func->header_global_var_ids.begin(), func->header_global_var_ids.end());
  used_vars[bucket].insert (func->static_var_ids.begin(), func->static_var_ids.end());
  used_vars[bucket].insert (func->const_var_ids.begin(), func->const_var_ids.end());
  used_vars[bucket].insert (func->header_const_var_ids.begin(), func->header_const_var_ids.end());
}

inline void DfsInit::compile (CodeGenerator &W) const {
  FunctionPtr main_func = main_file->main_function;

  set <FunctionPtr> used_functions;

  const int parts_n = 32;
  set <VarPtr> used_vars[parts_n];
  collect_used_funcs_and_vars (main_func, &used_functions, used_vars, parts_n);

  vector <string> header_names (parts_n);
  vector <string> src_names (parts_n);
  for (int i = 0; i < parts_n; i++) {
    string prefix = string ("dfs") + int_to_str (i) + ".";
    string header_name = prefix + main_func->header_name;
    string src_name = prefix + main_func->src_name;
    header_names[i] = header_name;
    src_names[i] = src_name;
  }

  for (int i = 0; i < parts_n; i++) {
    W << OpenFile (header_names[i]);
    compile_dfs_init_part (main_func, used_vars[i], false, i, W);
    W << CloseFile();

    W << OpenFile (src_names[i]);
    W << Include ("php_functions.h");
    compile_dfs_init_part (main_func, used_vars[i], true, i, W);
    W << CloseFile();
  }

  W << OpenFile ("dfs." + main_func->header_name);
  compile_dfs_init_func (main_func, used_functions, false, header_names, W);
  W << CloseFile();

  W << OpenFile ("dfs." + main_func->src_name);
  compile_dfs_init_func (main_func, used_functions, true, header_names, W);
  W << CloseFile();
}

inline FunctionH::FunctionH (FunctionPtr function) :
  function (function) {
}
void FunctionH::compile (CodeGenerator &W) const {
  W << OpenFile (function->header_name, function->subdir);
  W << "#pragma once" << NL <<
       Include ("php_functions.h");

  FOREACH (function->header_global_var_ids, global_var) {
    W << VarExternDeclaration (*global_var) << NL;
  }

  FOREACH (function->header_const_var_ids, const_var) {
    W << VarExternDeclaration (*const_var) << NL;
  }

  if (function->type() == FunctionData::func_global) {
    W << "extern bool " << FunctionCallFlag (function) << ";" << NL;
  }

  W << Function (function, true);

  W << FunctionStaticInit (function, true);

  W << CloseFile();
}

inline FunctionCpp::FunctionCpp (FunctionPtr function) :
  function (function) {
}

void FunctionCpp::compile (CodeGenerator &W) const {
  W << OpenFile (function->src_name, function->subdir);
  W << Include (function->header_full_name);

  stage::set_function (function);

  FOREACH (function->dep, it) {
    FunctionPtr to_include = *it;
    if (to_include == function ||
        to_include->type() == FunctionData::func_extern) {
      continue;
    }

    W << Include (to_include->header_full_name);
  }

  FOREACH (function->global_var_ids, global_var) {
    W << VarExternDeclaration (*global_var) << NL;
  }
  FOREACH (function->const_var_ids, const_var) {
    W << VarExternDeclaration (*const_var) << NL;
  }
  FOREACH (function->static_var_ids, static_var) {
    W << VarDeclaration (*static_var) << NL;
  }

  W << "extern string_buffer SB;" << NL;

  if (function->type() == FunctionData::func_global) {
    W << "bool " << FunctionCallFlag (function) << ";" << NL;
  }

  W << UnlockComments();
  W << Function (function);
  W << LockComments();

  W << FunctionStaticInit (function);

  W << CloseFile();
}


Operand::Operand (VertexPtr root, Operation parent_type, bool is_left) :
  root (root),
  parent_type (parent_type),
  is_left (is_left) {
}
inline void Operand::compile (CodeGenerator &W) const {
  int priority = OpInfo::priority (parent_type);
  bool left_to_right = OpInfo::fixity (parent_type) == left_opp;

  int root_priority = OpInfo::priority (root->type());

  bool need_par = (root_priority < priority || (root_priority == priority && (left_to_right ^ is_left))) && root_priority > 0;
  need_par |= parent_type == op_log_and_let || parent_type == op_log_or_let || parent_type == op_log_xor_let;

  if (need_par) {
    W << "(";
  }

  W << root;

  if (need_par) {
    W << ")";
  }
}

inline LabelName::LabelName (int label_id) :
  label_id (label_id) {
}
inline void LabelName::compile (CodeGenerator &W) const {
  W << "label" << int_to_str (label_id);
}

inline Label::Label (int label_id) :
  label_id (label_id) {
}
inline void Label::compile (CodeGenerator &W) const {
  if (label_id != 0) {
    W << NL << LabelName (label_id) << ":;" << NL;
  }
}

inline AsList::AsList (VertexPtr root, string delim) :
  root (root),
  delim (delim) {
}
inline void AsList::compile (CodeGenerator &W) const {
  bool first = true;
  FOREACH_VERTEX (root, i) {
    if (first) {
      first = false;
    } else {
      W << delim;
    }
    W << *i;
  }
}
inline AsSeq::AsSeq (VertexPtr root) :
  root (root) {
}
inline void AsSeq::compile (CodeGenerator &W) const {
  FOREACH_VERTEX (root, i) {
    if ((*i)->type() != op_var) {
      W << *i << ";" << NL;
    }
  }
}

void compile_prefix_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W) {
  W << OpInfo::str (root->type()) << Operand (root->expr(), root->type(), true);
}

void compile_postfix_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W) {
  W << Operand (root->expr(), root->type(), true) << OpInfo::str (root->type());
}


void compile_conv_op (VertexAdaptor <meta_op_unary_op> root, CodeGenerator &W) {
  if (root->type() == op_conv_regexp) {
    W << root->expr();
  } else {
    W << OpInfo::str (root->type()) << " (" << root->expr() << ")";
  }
}

void compile_noerr (VertexAdaptor <op_noerr> root, CodeGenerator &W) {
  if (root->rl_type == val_none) {
    W << "NOERR_VOID (" << Operand (root->expr(), root->type(), true) <<  ")";
  } else {
    const TypeData *res_tp = tinf::get_type (root);
    W << "NOERR (" << Operand (root->expr(), root->type(), true) << ", " << TypeName (res_tp) << ")";
  }
}


void compile_binary_func_op (VertexAdaptor <meta_op_binary_op> root, CodeGenerator &W) {
  W << OpInfo::str (root->type()) << " (" <<
       Operand (root->lhs(), root->type(), true) <<
       ", " <<
       Operand (root->rhs(), root->type(), false) <<
       ")";
}


void compile_binary_op (VertexAdaptor <meta_op_binary_op> root, CodeGenerator &W) {
  kphp_error_return (OpInfo::str (root->type())[0] != '@', dl_pstr ("Unexpected %s\n", OpInfo::str (root->type()).c_str() + 1));

  VertexPtr lhs = root->lhs();
  VertexPtr rhs = root->rhs();

  if (root->type() == op_add) {
    const TypeData *lhs_tp, *rhs_tp;
    lhs_tp = tinf::get_type (lhs);
    rhs_tp = tinf::get_type (rhs);

    if (lhs_tp->ptype() == tp_array && rhs_tp->ptype() == tp_array && type_out (lhs_tp) != type_out (rhs_tp)) {
      const TypeData *res_tp = tinf::get_type (root)->const_read_at (Key::any_key());
      W << "array_add < " << TypeName (res_tp) << " > (" << lhs << ", " << rhs << ")";
      return;
    }
  }

  OperationType tp = OpInfo::type (root->type());
  if (tp == binary_func_op) {
    compile_binary_func_op (root, W);
    return;
  }

  if (root->type() == op_lt || root->type() == op_gt || root->type() == op_le || root->type() == op_ge ||
      root->type() == op_eq2 || root->type() == op_neq2) {
    const TypeData *lhs_tp = tinf::get_type (lhs);
    const TypeData *rhs_tp = tinf::get_type (rhs);
    bool lhs_is_bool = lhs_tp->ptype() == tp_bool || (lhs_tp->ptype() == tp_Unknown && lhs_tp->use_or_false());
    bool rhs_is_bool = rhs_tp->ptype() == tp_bool || (rhs_tp->ptype() == tp_Unknown && rhs_tp->use_or_false());

    if (rhs_is_bool ^ lhs_is_bool) {
      if (root->type() == op_lt) {
        W << "lt";
      } else if (root->type() == op_gt) {
        W << "gt";
      } else if (root->type() == op_le) {
        W << "leq";
      } else if (root->type() == op_ge) {
        W << "geq";
      } else if (root->type() == op_eq2) {
        W << "eq2";
      } else if (root->type() == op_neq2) {
        W << "neq2";
      } else {
        kphp_fail();
      }
      W << " (" << lhs << ", " << rhs << ")";
      return;
    }
  }


  W << Operand (lhs, root->type(), true) <<
       " " << OpInfo::str (root->type()) << " " <<
       Operand (rhs, root->type(), false);
}


void compile_ternary_op (VertexAdaptor <op_ternary> root, CodeGenerator &W) {
  VertexPtr cond = root->cond();
  VertexPtr true_expr = root->true_expr();
  VertexPtr false_expr = root->false_expr();

  const TypeData *true_expr_tp, *false_expr_tp, *res_tp = NULL;
  true_expr_tp = tinf::get_type (true_expr);
  false_expr_tp = tinf::get_type (false_expr);

  //TODO: optimize type_out
  if (type_out (true_expr_tp) != type_out (false_expr_tp)) {
    res_tp = tinf::get_type (root);
  }

  W << Operand (cond, root->type(), true) << " ? ";

  if (res_tp != NULL) {
    W << TypeName (res_tp) << "(";
  }
  W << Operand (true_expr, root->type(), true);
  if (res_tp != NULL) {
    W << ")";
  }

  W << " : ";

  if (res_tp != NULL) {
    W << TypeName (res_tp) << "(";
  }
  W << Operand (false_expr, root->type(), true);
  if (res_tp != NULL) {
    W << ")";
  }
}


void compile_if (VertexAdaptor <op_if> root, CodeGenerator &W) {
  W << "if (" << root->cond() << ") " <<
        root->true_cmd();

  if (root->has_false_cmd()) {
    W << " else " << root->false_cmd();
  }
}


inline CycleBody::CycleBody (VertexPtr body, int continue_label_id, int break_label_id) :
  body (body),
  continue_label_id (continue_label_id),
  break_label_id (break_label_id) {
}
inline void CycleBody::compile (CodeGenerator &W) const {
  W << BEGIN <<
         AsSeq (body) <<
         Label (continue_label_id) <<
       END <<
       Label (break_label_id);
}


void compile_while (VertexAdaptor <op_while> root, CodeGenerator &W) {
  W << "while (" << root->cond() << ") " <<
        CycleBody (root->cmd(), root->continue_label_id, root->break_label_id);
}


void compile_do (VertexAdaptor <op_do> root, CodeGenerator &W) {
  W << "do " <<
       BEGIN <<
         AsSeq (root->cmd()) <<
         Label (root->continue_label_id) <<
       END << " while (" << root->cond() << ");" << NL <<
       Label (root->break_label_id);
}


void compile_require (VertexPtr root, CodeGenerator &W) {
  bool first = true;
  for (VertexRange i = all (*root); !i.empty(); i.next()) {
    if (first) {
      first = false;
    } else {
      W << "," << NL;
    }

    if (root->type() == op_require) {
      W << "require (";
    } else if (root->type() == op_require_once) {
      W << "require_once (";
    } else {
      kphp_fail();
    }

    VertexAdaptor <op_func_call> func = *i;
    W << FunctionCallFlag (func->get_func_id()) <<
         ", " <<
         func <<
         ")";
  }
}


void compile_return (VertexAdaptor <op_return> root, CodeGenerator &W) {
  W << "return ";
  VertexPtr val = root->expr();
  if (val->type() == op_empty) {
    W << "var()";
  } else {
    W << val;
  }
}



void compile_for (VertexAdaptor <op_for> root, CodeGenerator &W) {
  W << "for (" <<
          AsList (root->pre_cond(), ", ") << "; " <<
          AsList (root->cond(), ", ") << "; " <<
          AsList (root->post_cond(), ", ") << ") " <<
        CycleBody (root->cmd(), root->continue_label_id, root->break_label_id);
}

//TODO: some interface for context?
void compile_throw_fast_action (CodeGenerator &W) {
  CGContext &context = W.get_context();
  if (context.catch_labels.empty() || context.catch_labels.back().empty()) {
    const TypeData *tp = tinf::get_type (context.parent_func, -1);
    W << "return ";
    if (tp->ptype() != tp_void) {
      W << "(" << TypeName (tp) << "())";
    }
  } else {
    W << "goto " << context.catch_labels.back();
    context.catch_label_used.back() = 1;
  }
}


void compile_throw (VertexAdaptor <op_throw> root, CodeGenerator &W) {
  W << "throw (" << root->expr() << ")";
}


void compile_throw_fast (VertexAdaptor <op_throw> root, CodeGenerator &W) {
  W << BEGIN <<
         "THROW_EXCEPTION (" << root->expr() << ");" << NL;
         compile_throw_fast_action (W);
  W <<   ";" << NL <<
       END << NL;
}


void compile_try (VertexAdaptor <op_try> root, CodeGenerator &W) {
  W << "try " << root->try_cmd() <<
       " catch (Exception " << root->exception() << ")" <<
       root->catch_cmd();
}


void compile_try_fast (VertexAdaptor <op_try> root, CodeGenerator &W) {
  CGContext &context = W.get_context();

  string catch_label = gen_unique_name ("catch_label");
  W << "/""*** TRY ***""/" << NL;
  context.catch_labels.push_back (catch_label);
  context.catch_label_used.push_back (0);
  W << root->try_cmd() << NL;
  context.catch_labels.pop_back();
  bool used = context.catch_label_used.back();
  context.catch_label_used.pop_back();

  if (used) {
    W << "/""*** CATCH ***""/" << NL <<
         "if (0) " <<
         BEGIN <<
           catch_label << ":;" << NL << //TODO: Label (lable_id) ?
           root->exception() << " = *CurException;" << NL <<
           "FREE_EXCEPTION;" << NL <<
           root->catch_cmd() << NL <<
         END << NL;
  }
}

void compile_foreach (VertexAdaptor <op_foreach> root, CodeGenerator &W) {
  VertexAdaptor <op_foreach_param> params = root->params();
  VertexPtr cmd = root->cmd();

  //foreach (xs as [key =>] x)
  VertexPtr xs = params->xs();
  VertexPtr x = params->x();
  VertexPtr key;
  if (params->has_key()) {
    key = params->key();
  }


  string xs_copy_str;
  xs_copy_str = gen_unique_name ("tmp_expr");
  const TypeData *xs_type = tinf::get_type (xs);

  W << BEGIN;
  //save array to 'xs_copy_str'
  if (!x->ref_flag) {
    W << "const ";
  }
  W << TypeName (xs_type) << " ";
  if (x->ref_flag) {
    W << ("&");
  }
  W << xs_copy_str << " = " << xs << ";" << NL;

  string it = gen_unique_name ("it");
  W << "for (" <<
          "typeof (begin (" << xs_copy_str << ")) " << it << " = begin (" << xs_copy_str << "); " <<
          it << " != end (" << xs_copy_str << "); " <<
          "++" << it << ")" <<
        BEGIN;


  //save value
  if (x->ref_flag) {
    W << TypeName (tinf::get_type (x)) << " &";
  }
  W << x << " = " << it << ".get_value();" << NL;

  //save key
  if (key.not_null()) {
    W << key << " = " << it << ".get_key();" << NL;
  }

  W <<     AsSeq (cmd) << NL <<
           Label (root->continue_label_id) <<
         END <<
         Label (root->break_label_id) << NL <<
       END;
}

struct CaseInfo {
  unsigned int hash;
  bool is_default;
  string goto_name;
  CaseInfo *next;
  VertexPtr v;
  VertexPtr expr;
  VertexPtr cmd;

  inline CaseInfo() {
  }
  inline CaseInfo (VertexPtr root) {
    next = NULL;
    v = root;
    if (v->type() == op_default) {
      is_default = true;
      cmd = VertexAdaptor <op_default> (v)->cmd();
    } else {
      is_default = false;
      VertexAdaptor <op_case> cs = v;

      expr = cs->expr();
      cmd = cs->cmd();

      VertexPtr val = GenTree::get_actual_value (expr);
      kphp_assert (val->type() == op_string);
      const string &s = val.as <op_string>()->str_val;
      hash = string_hash (s.c_str(), (int)s.size());
    }
  }
};


void compile_switch_str (VertexAdaptor <op_switch> root, CodeGenerator &W) {
  vector <CaseInfo> cases;

  for (VertexRange i = root->cases(); !i.empty(); i.next()) {
    cases.push_back (CaseInfo (*i));
  }
  int n = (int)cases.size();

  CaseInfo *default_case = NULL;
  for (int i = 0; i < n; i++) {
    if (cases[i].is_default) {
      kphp_error_return (default_case == NULL, "Several default cases in switch");
      default_case = &cases[i];
    }
  }

  map <unsigned int, int> prev;
  for (int i = n - 1; i >= 0; i--) {
    if (cases[i].is_default) {
      continue;
    }
    pair <unsigned int, int> new_val (cases[i].hash, i);
    pair <map <unsigned int, int>::iterator, bool> insert_res = prev.insert (new_val);
    if (insert_res.second == false) {
      int next_i = insert_res.first->second;
      insert_res.first->second = i;
      cases[i].next = &cases[next_i];
    } else {
      cases[i].next = default_case;
    }
    CaseInfo *next = cases[i].next;
    if (next != NULL && next->goto_name.empty()) {
      next->goto_name = gen_unique_name ("switch_goto");
    }
  }

  W << BEGIN;

  string string_name = gen_unique_name ("ss");
  W << "string " << string_name << " = f$strval (" << root->expr() << ");" << NL;

  string hash_name = gen_unique_name ("ss_hash");
  W << "unsigned int " << hash_name << " = " << string_name << ".hash();" << NL;

  string flag_name = gen_unique_name ("switch_flag");
  W << "bool " << flag_name << " = false;" << NL;

  W << "switch (" << hash_name << ") " <<
       BEGIN;
  for (int i = 0; i < n; i++) {
    CaseInfo *cur = &cases[i];
    if (cur->is_default) {
      W << "default:" << NL;
    } else if (cur->goto_name.empty()) {
      char buf[100];
      sprintf (buf, "0x%x", cur->hash);

      W << "case " << (const char *)buf << ":" << NL;
    }
    if (!cur->goto_name.empty()) {
      W << cur->goto_name << ":;" << NL;
    }

    if (!cur->is_default) {
      W << "if (!" << flag_name << ") " <<
           BEGIN <<
             "if (!equals (" << string_name << ", " << cur->expr << ")) " <<
             BEGIN;
      string next_goto;
      if (cur->next != NULL) {
        next_goto = cur->next->goto_name;
        W <<   "goto " << next_goto << ";" << NL;
      } else {
        W <<   "break;" << NL;
      }
      W <<   END <<
             " else " <<
             BEGIN <<
               flag_name << " = true;" << NL <<
             END << NL <<
           END << NL;
    } else {
      W << flag_name << " = true;" << NL;
    }
    W << cur->cmd << NL;
  }
  W << END << NL;

  W <<   Label (root->continue_label_id) <<
       END <<
       Label (root->break_label_id);
}

void compile_switch_int (VertexAdaptor <op_switch> root, CodeGenerator &W) {
  W << "switch (f$intval (" << root->expr() << "))" <<
        BEGIN;

  set <string> used;
  for (VertexRange i = root->cases(); !i.empty(); i.next()) {
    Operation tp = (*i)->type();
    VertexPtr cmd;
    if (tp == op_case) {
      VertexAdaptor <op_case> cs = *i;
      cmd = cs->cmd();

      VertexPtr val = GenTree::get_actual_value (cs->expr());
      kphp_assert (val->type() == op_int_const);
      string str = val.as <op_int_const>()->str_val;
      W << "case " << str;

      kphp_error (!used.count (str),
          dl_pstr ("Switch: repeated cases found [%s]", str.c_str()));
      used.insert (str);
    } else if (tp == op_default) {
      W << "default";
      cmd = VertexAdaptor <op_default> (*i)->cmd();
    } else {
      kphp_fail();
    }
    W << ": " << cmd << NL;
  }
  W <<   Label (root->continue_label_id) <<
       END <<
       Label (root->break_label_id);
}


void compile_switch_var (VertexAdaptor <op_switch> root, CodeGenerator &W) {
  string var_name = gen_unique_name ("switch_var");
  string flag_name = gen_unique_name ("switch_flag"), goto_name;

  W << "do " <<
       BEGIN <<
         "var " << var_name << " = " << root->expr() << ";" << NL <<
         "bool " << flag_name << " = false;" << NL;

  FOREACH_VERTEX (root->cases(), i) {
    Operation tp = (*i)->type();
    VertexPtr expr;
    VertexPtr cmd;
    if (tp == op_case) {
      VertexAdaptor <op_case> cs (*i);
      expr = cs->expr();
      cmd = cs->cmd();
    } else if (tp == op_default) {
      cmd = VertexAdaptor <op_default> (*i)->cmd();
    } else {
      kphp_fail();
    }

    W << "if (" << flag_name;

    if (tp == op_case) {
      W << " || eq2 (" << var_name << ", " << expr << ")";
    }
    W << ") " <<
         BEGIN;
    if (tp == op_default) {
      goto_name = gen_unique_name ("switch_goto");
      W << goto_name + ": ";
    }

    W <<   flag_name << " = true;" <<
           AsSeq (cmd) <<
         END << NL;
  }


  if (!goto_name.empty()) {
    W << "if (" << flag_name << ") " <<
         BEGIN <<
           "break;" << NL <<
         END << NL <<
         flag_name << " = true;" << NL <<
         "goto " << goto_name << ";" << NL;
  }


  W <<   Label (root->continue_label_id) <<
       END << " while (0)" <<
       Label (root->break_label_id);
}



void compile_switch (VertexAdaptor <op_switch> root, CodeGenerator &W) {
  int cnt_int = 0, cnt_str = 0, cnt_default = 0;

  FOREACH_VERTEX (root->cases(), i) {
    if ((*i)->type() == op_default) {
      cnt_default++;
    } else {
      VertexAdaptor <op_case> cs = *i;
      VertexPtr val = GenTree::get_actual_value (cs->expr());
      if (val->type() == op_int_const) {
        cnt_int++;
      } else if (val->type() == op_string) {
        cnt_str++;
      } else {
        cnt_str++;
        cnt_int++;
      }
    }
  }
  kphp_error_return (cnt_default <= 1, "Switch: several default cases found");

  if (!cnt_int) {
    compile_switch_str (root, W);
  } else if (!cnt_str) {
    compile_switch_int (root, W);
  } else {
    compile_switch_var (root, W);
  }
}

void compile_function (VertexPtr root, CodeGenerator &W) {

  VertexAdaptor <op_function> func_root = root;
  FunctionPtr func = func_root->get_func_id();

  W.get_context().parent_func = func;

  W << FunctionDeclaration (func, false) << " " <<
       BEGIN;

  FOREACH (func->local_var_ids, var) {
    W << VarDeclaration (*var);
  }

  if (func->varg_flag) {
    VertexAdaptor <op_func_param_list> params = func_root->params();
    for (int i = 0; i < (int)func->param_ids.size(); i++) {
      VarPtr var = func->param_ids[i];
      W << "var " << VarName (var) << " = VA_LIST.isset (" << int_to_str (i) << ") ? " <<
              "VA_LIST.get_value (" << int_to_str (i) << ")" << " : ";

      VertexAdaptor <op_func_param> param = params->ith_param (i);
      if (param->has_default()) {
        VertexPtr default_val = param->default_value();
        W << default_val;
      } else {
        W << "var()";
      }

      W << ";" << NL;
    }
  }

  W <<  AsSeq (func_root->cmd()) << NL <<
      END << NL;
}


void compile_string_build_raw (VertexAdaptor <op_string_build> root, CodeGenerator &W) {
  W << "(SB.clean()";
  FOREACH (root->args(), i) {
    VertexPtr v = *i;

    W << "+";

    if (v->type() != op_string) {
      W << "(";
    }

    W << v;

    if (v->type() != op_string) {
      W << ")";
    }
  }
  W << ")";
}

struct StrlenInfo {
  VertexPtr v;
  int len;
  bool str_flag;
  bool var_flag;
  string str;

  StrlenInfo() :
    len (0),
    str_flag (false),
    var_flag (false),
    str() {
    }
};

static bool can_save_ref (VertexPtr v) {
  if (v->type() == op_var) {
    return true;
  }
  if (v->type() == op_func_call) {
    FunctionPtr func = v.as <op_func_call>()->get_func_id();
    if (func->type() == FunctionData::func_extern) {
      //todo
      return false;
    }
    return true;
  }
  return false;
}
void compile_string_build_as_string (VertexAdaptor <op_string_build> root, CodeGenerator &W) {
  vector <StrlenInfo> info (root->size());
  bool ok = true;
  bool was_dynamic = false;
  bool was_object = false;
  int static_length = 0;
  int ii = 0;
  FOREACH (root->args(), i) {
    info[ii].v = *i;
    VertexPtr value = GenTree::get_actual_value (*i);
    const TypeData *type = tinf::get_type (value);

    int value_length = type_strlen (type);
    if (value_length == STRLEN_ERROR) {
      kphp_error (0, dl_pstr ("Cannot convert type [%s] to string", type_out (type).c_str()));
      ok = false;
      ii++;
      continue;
    }

    if (value->type() == op_string || value->type() == op_int_const) {
      info[ii].str_flag = true;
      info[ii].str = value->get_string();
      info[ii].len = (int)info[ii].str.size();
      static_length += info[ii].len;
    } else {
      if (value_length == STRLEN_DYNAMIC) {
        was_dynamic = true;
      } else if (value_length == STRLEN_OBJECT) {
        was_object = true;
      } else {
        if (value_length & STRLEN_WARNING_FLAG) {
          value_length &= ~STRLEN_WARNING_FLAG;
          kphp_warning (dl_pstr ("Suspicious convertion of type [%s] to string", type_out (type).c_str()));
        }

        kphp_assert (value_length >= 0);
        static_length += value_length;
      }

      info[ii].len = value_length;
    }

    ii++;
  }
  if (!ok) {
    return;
  }

  bool complex_flag = was_dynamic || was_object;
  string len_name;

  if (complex_flag) {
    W << "(" << BEGIN;
    vector <string> to_add;
    for (int i = 0; i < (int)info.size(); i++) {
      if (info[i].str_flag) {
        continue;
      }
      if (info[i].len == STRLEN_DYNAMIC || info[i].len == STRLEN_OBJECT) {
        string var_name = gen_unique_name ("var");

        if (info[i].len == STRLEN_DYNAMIC) {
          bool can_save_ref_flag = can_save_ref (info[i].v);
          W << "const " << TypeName (tinf::get_type (info[i].v)) << " " <<
            (can_save_ref_flag ? "&" : "") <<
            var_name << "=" << info[i].v << ";" << NL;
        } else if (info[i].len == STRLEN_OBJECT) {
          W << "const string " << var_name << " = f$strval" <<
            "(" << info[i].v << ");" << NL;
        }

        to_add.push_back (var_name);
        info[i].var_flag = true;
        info[i].str = var_name;
      }
    }

    len_name = gen_unique_name ("strlen");
    W << "dl::size_type " << len_name << " = " << int_to_str (static_length);
    for (int i = 0; i < (int)to_add.size(); i++) {
      W << " + max_string_size (" << to_add[i] << ")";
    }
    W << ";" << NL;
  }

  W << "string (";
  if (complex_flag) {
    W << len_name;
  } else {
    W << int_to_str (static_length);
  }
  W << ", true)";
  for (int i = 0; i < (int)info.size(); i++) {
    W << ".append_unsafe (";
    if (info[i].str_flag) {
      compile_string_raw (info[i].str, W);
      W << ", " << int_to_str ((int)info[i].len);
    } else if (info[i].var_flag) {
      W << info[i].str;
    } else {
      W << info[i].v;
    }
    W << ")";
  }
  W << ".finish_append()";

  if (complex_flag) {
    W <<   ";" << NL <<
         END << ")";
  }
}

void compile_index (VertexAdaptor <op_index> root, CodeGenerator &W) {
  bool has_key = root->has_key();
  if (root->extra_type == op_ex_none) {
      W << root->array() << "[";
      if (has_key) {
        W << root->key();
      }
      W << "]";
  } else if (root->extra_type == op_ex_index_rval) {
    W << root->array() << ".get_value (";
    if (has_key) {
      W << root->key();
    }
    W << ")";
  } else {
    kphp_fail();
  }
}

void compile_as_printable (VertexPtr root, CodeGenerator &W) {
  if (root->type() == op_conv_string) {
    VertexAdaptor <op_conv_string> conv = root;
    if (conv->expr()->type() == op_string) {
      root = conv->expr();
    }
  }

  if (root->type() == op_string) {
    compile_string (root, W);
    return;
  }

  if (root->type() == op_string_build) {
    //compile_string_build_raw (root, W);
    compile_string_build_as_string (root, W);
    return;
  }

  if (root->type() == op_conv_string) {
    VertexAdaptor <op_conv_string> conv = root;
    if (conv->expr()->type() == op_string_build) {
      compile_as_printable (conv->expr(), W);
      return;
    }
  }

  if (root->type() != op_conv_string) {
    W << "(";
  }
  W << root;
  if (root->type() != op_conv_string) {
    W << ")";
  }
}


void compile_echo (VertexPtr root, CodeGenerator &W) {
  bool first = true;

  for (VertexRange i = all (*root); !i.empty(); i.next()) {
    if (first) {
      first = false;
    } else {
      W << ";" << NL;
    }

    if (root->type() == op_dbg_echo) {
      W << "dbg_echo (";
    } else {
      W << "print (";
    }
    compile_as_printable (*i, W);
    W << ")";
  }
}


void compile_var_dump (VertexPtr root, CodeGenerator &W) {
  bool first = true;
  FOREACH_VERTEX (root, i) {
    if (first) {
      first = false;
    } else {
      W << ";" << NL;
    }

    W << "f$var_dump (" << *i << ")";
  }
}


void compile_print (VertexAdaptor <op_print> root, CodeGenerator &W) {
  W << "print (";
  compile_as_printable (root->expr(), W);
  W << ")";
}


void compile_store_many (VertexAdaptor <op_store_many> root, CodeGenerator &W) {
  W << "f$store_many (" << root->expr() << ")";
}


void compile_pack (VertexAdaptor <op_pack> root, CodeGenerator &W) {
  W << "f$pack (" << root->expr() << ")";
}


void compile_xprintf (VertexAdaptor <meta_op_xprintf> root, CodeGenerator &W) {
  if (root->type() == op_printf) {
    W << "f$printf (";
  } else if (root->type() == op_sprintf) {
    W << "f$sprintf (";
  } else {
    assert (0);
  }

  W << root->expr() << ")";
}



void compile_xset (VertexAdaptor <meta_op_xset> root, CodeGenerator &W) {
  assert ((int)root->size() == 1 || root->type() == op_unset);

  bool first = true;
  for (VertexRange i = root->args(); !i.empty(); i.next()) {
    if (first) {
      first = false;
    } else {
      W << ";" << NL;
    }

    VertexPtr arg = *i;
    if (root->type() == op_unset && arg->type() == op_var) {
      W << "unset (" << arg << ")";
      continue;
    }
    if (root->type() == op_isset && arg->type() == op_var) {
      W << "(!f$is_null(" << arg << "))";
      continue;
    }
    if (arg->type() == op_index) {
      VertexAdaptor <op_index> index = arg;
      kphp_assert (index->has_key());
      VertexPtr arr = index->array(), id = index->key();
      W << "(" << arr;
      if (root->type() == op_isset) {
        W << ").isset (";
      } else if (root->type() == op_unset) {
        W << ").unset (";
      } else {
        assert (0);
      }
      W << id << ")";
      continue;
    }
    kphp_error (0, "Some problems with isset/unset");
  }
}


void compile_list (VertexAdaptor <op_list> root, CodeGenerator &W) {
  W << "(" << BEGIN;

  VertexPtr arr = root->array();
  VertexRange list = root->list();

  string arr_name;
  if (arr->type() == op_var) {
  } else {
    arr_name = gen_unique_name ("tmp_arr");
    W << "const " << TypeName (tinf::get_type (arr)) << " " << arr_name << " = " << arr << ";" << NL;
  }


  int n = (int)list.size();
  for (int i = n - 1; i >= 0; i--) {
    VertexPtr cur = list[i];
    if (cur->type() != op_lvalue_null) {
      W << "assign (" << cur << ", get_value (";
      if (arr_name.empty()) {
        W << VarName (arr->get_var_id());
      } else {
        W << arr_name;
      }
      W << ", " << int_to_str (i) << "));" << NL;
    }
  }

  W << arr_name << ";" <<
       END << ")";
}


void compile_array (VertexAdaptor <op_array> root, CodeGenerator &W) {
  int n = (int)root->args().size();
  const TypeData *type = tinf::get_type (root);

  if (n == 0) {
    W << TypeName (type) << " ()";
    return;
  }

  bool has_double_arrow = false;
  int int_cnt = 0, string_cnt = 0, xx_cnt = 0;
  for (VertexRange i = root->args(); !i.empty(); i.next()) {
    if ((*i)->type() == op_double_arrow) {
      VertexAdaptor <op_double_arrow> arrow = *i;
      has_double_arrow = true;
      VertexPtr key = arrow->key();
      PrimitiveType tp = tinf::get_type (key)->ptype();
      if (tp == tp_int) {
        int_cnt++;
      } else {
        VertexPtr key_val = GenTree::get_actual_value (key);
        if (tp == tp_string && key_val->type() == op_string) {
          string key = key_val.as <op_string>()->str_val;
          if (php_is_int (key.c_str(), (int)key.size())) {
            int_cnt++;
          } else {
            string_cnt++;
          }
        } else {
          xx_cnt++;
        }
      }
    } else {
      int_cnt++;
    }
  }
  if (2 <= n && n <= 10 && !has_double_arrow && type->ptype() == tp_array && root->extra_type != op_ex_safe_version) {
    W << TypeName (type) << " (" << AsList (root, ", ") << ")";
    return;
  }

  W << "(" << BEGIN;

  string arr_name = gen_unique_name ("tmp_array");
  W << TypeName (type) << " " << arr_name << " = ";

  //TODO: check
  if (type->ptype() == tp_array) {
    W << TypeName (type);
  } else {
    W << "array <var>";
  }
  char tmp[70];
  sprintf (tmp, " (array_size (%d, %d, %s));", int_cnt + xx_cnt, string_cnt + xx_cnt, has_double_arrow ? "false" : "true");
  W << (const char *)tmp << NL;

  FOREACH (root->args(), i) {
    W << arr_name;
    VertexPtr cur = *i;
    if (cur->type() == op_double_arrow) {
      VertexAdaptor <op_double_arrow> arrow = cur;
      W << ".set_value (" << arrow->key() << ", " << arrow->value() << ")";
    } else {
      W << ".push_back (" << cur << ")";
    }

    W << ";" << NL;
  }

  W << arr_name << ";" << NL <<
       END << ")";
}


void compile_func_call_fast (VertexAdaptor <op_func_call> root, CodeGenerator &W) {
  if (!root->get_func_id()->root->throws_flag) {
   compile_func_call (root, W);
   return;
  }
  bool is_void = root->rl_type == val_none;

  if (is_void) {
    W << "TRY_CALL_VOID_ (";
  } else {
    const TypeData *type = tinf::get_type (root);
    W << "TRY_CALL_ (" << TypeName (type) << ", ";
  }

  W.get_context().catch_labels.push_back ("");
  compile_func_call (root, W);
  W.get_context().catch_labels.pop_back();

  W << ", ";
  compile_throw_fast_action (W);
  W << ")";
}

//FIXME: remove int fix
void compile_func_call (VertexAdaptor <op_func_call> root, CodeGenerator &W, int fix) {
  FunctionPtr func;
  if (root->extra_type == op_ex_internal_func) {
    W << root->str_val;
  } else {
    func = root->get_func_id();
    W << FunctionName (func);
    if (0)
    if (func->name == "preg_match" || func->name == "preg_match_all" || func->name == "preg_replace_callback" ||
        func->name == "preg_split" || func->name == "preg_replace") {
      VertexPtr first = root->args()[0];
      VertexPtr val = GenTree::get_actual_value (first);
      if (val->type() == op_string) {
        ScopedDebug scoped_debug (W);
        W << "const:";
        compile_string (val, W);
        W << NL;
      } else {
        ScopedDebug scoped_debug (W);
        stage::print_file (stderr);
        stage::print_comment (stderr);
        W << "nonconst:" << first << NL;
      }
    }
  }
  if (!func.is_null() && func->root->auto_flag) {
    const TypeData *tp = tinf::get_type (root);
    W << "< " << TypeName (tp) << " >";
  }
  W << "(";
  VertexRange i = root->args();
  if (fix && !i.empty()) {
    i.next();
  }
  bool first = true;
  for (;!i.empty(); i.next()) {
    if (first) {
      first = false;
    } else {
      W << ", ";
    }
    W << *i;
  }
  W << ")";
}


void compile_func_ptr (VertexAdaptor <op_func_ptr> root, CodeGenerator &W) {
  if (root->str_val == "boolval") {
    W << "(bool (*) (const var &))";
  }
  if (root->str_val == "intval") {
    W << "(int (*) (const var &))";
  }
  if (root->str_val == "floatval") {
    W << "(double (*) (const var &))";
  }
  if (root->str_val == "strval") {
    W << "(string (*) (const var &))";
  }
  if (root->str_val == "is_numeric" ||
      root->str_val == "is_null" ||
      root->str_val == "is_bool" ||
      root->str_val == "is_int" ||
      root->str_val == "is_float" ||
      root->str_val == "is_scalar" ||
      root->str_val == "is_string" ||
      root->str_val == "is_array" ||
      root->str_val == "is_object" ||
      root->str_val == "is_integer" ||
      root->str_val == "is_long" ||
      root->str_val == "is_double" ||
      root->str_val == "is_real") {
    W << "(bool (*) (const var &))";
  }
  W << FunctionName (root->get_func_id());
}


//TODO: write proper define_raw

void compile_define (VertexPtr root, CodeGenerator &W) {
  DefinePtr d = root->get_define_id();

  W << LockComments() <<
       "(" << d->val << ")" <<
       UnlockComments();
}


void compile_defined (VertexPtr root, CodeGenerator &W) {
  W << "false";
  //TODO: it is not CodeGen part
}


void compile_safe_version (VertexPtr root, CodeGenerator &W) {
  if (root->type() == op_set_value) {
    VertexAdaptor <op_set_value> set_value = root;
    W << "SAFE_SET_VALUE ("  <<
            set_value->array() << ", " <<
            set_value->key() << ", " <<
            TypeName (tinf::get_type (set_value->key())) << ", " <<
            set_value->value() << ", " <<
            TypeName (tinf::get_type (set_value->value())) <<
          ")";
  } else if (OpInfo::rl (root->type()) == rl_set) {
    VertexAdaptor <meta_op_binary_op> op = root;
    if (OpInfo::type (root->type()) == binary_func_op) {
      W << "SAFE_SET_FUNC_OP (";
    } else if (OpInfo::type (root->type()) == binary_op) {
      W << "SAFE_SET_OP (";
    } else {
      kphp_fail();
    }
    W <<     op->lhs() << ", " <<
             OpInfo::str (root->type()) << ", " <<
             op->rhs() << ", " <<
             TypeName (tinf::get_type (op->rhs())) <<
           ")";
  } else if (root->type() == op_push_back) {
    VertexAdaptor <op_push_back> pb = root;
    W << "SAFE_PUSH_BACK (" <<
            pb->array() << ", " <<
            pb->value() << ", " <<
            TypeName (tinf::get_type (pb->value())) <<
          ")";
  } else if (root->type() == op_push_back_return) {
    VertexAdaptor <op_push_back_return> pb = root;
    W << "SAFE_PUSH_BACK_RETURN (" <<
            pb->array() << ", " <<
            pb->value() << ", " <<
            TypeName (tinf::get_type (pb->value())) <<
          ")";
  } else if (root->type() == op_array) {
    compile_array (root, W);
    return;
  } else if (root->type() == op_index) {
    VertexAdaptor <op_index> index = root;
    kphp_assert (index->has_key());
    W << "SAFE_INDEX (" <<
           index->array() << ", " <<
           index->key() << ", " <<
           TypeName (tinf::get_type (index->key())) <<
         ")";
  } else {
    kphp_error (0, dl_pstr ("Safe version of [%s] is not supported", OpInfo::str (root->type()).c_str()));
    kphp_fail();
  }

}


void compile_set_value (VertexAdaptor <op_set_value> root, CodeGenerator &W) {
  W << "(" << root->array() << ").set_value (" << root->key() << ", " << root->value() << ")";
}

void compile_push_back (VertexAdaptor <op_push_back> root, CodeGenerator &W) {
  W << "(" << root->array() << ").push_back (" << root->value() << ")";
}

void compile_push_back_return (VertexAdaptor <op_push_back_return> root, CodeGenerator &W) {
  W << "(" << root->array() << ").push_back_return (" << root->value() << ")";
}


void compile_string_raw (const string &str, CodeGenerator &W) {
  W << "\"";
  for (int i = 0; i < (int)str.size(); i++) {
    switch (str[i]) {
      case '\r':
        W << "\\r";
        break;
      case '\n':
        W << "\\n";
        break;
      case '"':
        W << "\\\"";
        break;
      case '\\':
        W << "\\\\";
        break;
      case '\'':
        W << "\\\'";
        break;
      case 0: {
        if (str[i + 1] < '0' || str[i + 1] > '9') {
          W << "\\0";
        } else {
          W << "\\000";
        }
        break;
      }
      case '\a':
        W << "\\a";
        break;
      case '\b':
        W << "\\b";
        break;
      case '\f':
        W << "\\f";
        break;
      case '\t':
        W << "\\t";
        break;
      case '\v':
        W << "\\v";
        break;
      default:
        if ((unsigned char)str[i] < 32) {
          string tmp = "\\0";
          tmp += (char)('0' + (str[i] / 8));
          tmp += (char)('0' + (str[i] % 8));
          W << tmp;
        } else {
          //TODO:fixme
          W << str.substr (i, 1);
        }
        break;
    }
  }
  W << "\"";
}
void compile_string_raw (VertexAdaptor <op_string> root, CodeGenerator &W) {
  const string &str = root->str_val;
  compile_string_raw (str, W);
}


void compile_string (VertexAdaptor <op_string> root, CodeGenerator &W) {
  W << "string (";
  compile_string_raw (root, W);
  W << ", " << int_to_str ((int)root->str_val.size()) << ")";
}


void compile_string_build (VertexPtr root, CodeGenerator &W) {
  //W << "(";
  //compile_string_build_raw (root, W);
  //W << ".str())";
  compile_string_build_as_string (root, W);
}



void compile_new (VertexPtr root, CodeGenerator &W) {
  //compile_type_name (tinf::get_type (root), w);
  //(*w) ("()");
}



void compile_break_continue (VertexAdaptor <meta_op_goto> root, CodeGenerator &W) {
  if (root->int_val != 0) {
    W << "goto " << LabelName (root->int_val);
  } else {
    if (root->type() == op_break) {
      W << "break";
    } else if (root->type() == op_continue) {
      W << "continue";
    } else {
      assert (0);
    }
  }
}


void compile_conv_array_l (VertexAdaptor <op_conv_array_l> root, CodeGenerator &W) {
  VertexPtr val = root->expr();
  PrimitiveType tp = tinf::get_type (val)->ptype();
  if (tp == tp_array || tp == tp_var) {
    W << "arrayval_ref (" << val << ", \"unknown\", -1)";
  } else {
    kphp_error (0, "Trying to pass non-array as reference to array");
  }
}


void compile_conv_int_l (VertexAdaptor <op_conv_int_l> root, CodeGenerator &W) {
  VertexPtr val = root->expr();
  PrimitiveType tp = tinf::get_type (val)->ptype();
  if (tp == tp_int || tp == tp_var) {
    W << "intval_ref (" << val << ")";
  } else {
    kphp_error (0, "Trying to pass non-int as reference to int");
  }
}


void compile_cycle_op (VertexPtr root, CodeGenerator &W) {
  Operation tp = root->type();
  switch (tp) {
    case op_while:
      compile_while (root, W);
      break;
    case op_do:
      compile_do (root, W);
      break;
    case op_for:
      compile_for (root, W);
      break;
    case op_foreach:
      compile_foreach (root, W);
      break;
    case op_switch:
      compile_switch (root, W);
      break;
    default:
      assert (0);
      break;
  }
}


void compile_min_max (VertexPtr root, CodeGenerator &W) {
  W << OpInfo::str (root->type()) << "< " << TypeName (tinf::get_type (root)) << " > (" <<
         AsList (root, ", ") <<
       ")";
}


void compile_common_op (VertexPtr root, CodeGenerator &W) {
  Operation tp = root->type();
  string str;
  switch (tp) {
    case op_seq:
      W << BEGIN << AsSeq (root) << END;
      break;

    case op_int_const:
      str = root.as <op_int_const>()->str_val;
      if (str.size() > 9) {
        W << "(int)";
      }
      W << str;
      break;
    case op_uint_const:
      str = root.as <op_uint_const>()->str_val;
      if (str.size() > 9) {
        W << "(unsigned int)";
      }
      W << str << "u";
      break;
    case op_long_const:
      str = root.as <op_long_const>()->str_val;
      if (str.size() > 18) {
        W << "(long long)";
      }
      W << str << "ll";
      break;
    case op_ulong_const:
      str = root.as <op_ulong_const>()->str_val;
      if (str.size() > 18) {
        W << "(unsigned long long)";
      }
      W << str << "ull";
      break;
    case op_float_const:
      str = root.as <op_float_const>()->str_val;
      W << "(double)" << str;
      break;
    case op_false:
      W << "false";
      break;
    case op_true:
      W << "true";
      break;
    case op_null:
      W << "var()";
      break;
    case op_var:
      W << VarName (root->get_var_id());
      break;
    case op_string:
      compile_string (root, W);
      break;
    case op_new:
      compile_new (root, W);
      break;

    case op_if:
      compile_if (root, W);
      break;
    case op_require:
    case op_require_once:
      compile_require (root, W);
      break;
    case op_return:
      compile_return (root, W);
      break;
    case op_global:
    case op_static:
      //already processed
      break;
    case op_echo:
    case op_dbg_echo:
      compile_echo (root, W);
      break;
    case op_throw:
#ifdef FAST_EXCEPTIONS
      compile_throw_fast (root, W);
#else
      compile_throw (root, W);
#endif
      break;
    case op_var_dump:
      compile_var_dump (root, W);
      break;
    case op_print:
      compile_print (root, W);
      break;
    case op_pack:
      compile_pack (root, W);
      break;
    case op_printf:
    case op_sprintf:
      compile_xprintf (root, W);
      break;
    case op_store_many:
      compile_store_many (root, W);
      break;
    case op_min:
    case op_max:
      compile_min_max (root, W);
      break;
    case op_continue:
    case op_break:
      compile_break_continue (root, W);
      break;
    case op_try:
#ifdef FAST_EXCEPTIONS
      compile_try_fast (root, W);
#else
      compile_try (root, W);
#endif
      break;
    case op_function:
      compile_function (root, W);
      break;
    case op_func_call:
#ifdef FAST_EXCEPTIONS
      compile_func_call_fast (root, W);
#else
      compile_func_call (root, W);
#endif
      break;
    case op_func_ptr:
      compile_func_ptr (root, W);
      break;
    case op_string_build:
      compile_string_build (root, W);
      break;
    case op_index:
      compile_index(root, W);
      break;
    case op_isset:
      compile_xset (root, W);
      break;
    case op_list:
      compile_list (root, W);
      break;
    case op_array:
      compile_array (root, W);
      break;
    case op_unset:
      compile_xset (root, W);
      break;
    case op_empty:
      break;
    case op_define_val:
      compile_define (root, W);
      break;
    case op_defined:
      compile_defined (root, W);
      break;
    case op_extern_func:
      break;
    case op_conv_array_l:
      compile_conv_array_l (root, W);
      break;
    case op_conv_int_l:
      compile_conv_int_l (root, W);
      break;
    case op_set_value:
      compile_set_value (root, W);
      break;
    case op_push_back:
      compile_push_back (root, W);
      break;
    case op_push_back_return:
      compile_push_back_return (root, W);
      break;
    case op_noerr:
      compile_noerr (root, W);
      break;
    default:
      printf ("wtf??? %d\n", tp);
      assert (0);
      break;
  }
}


void compile_vertex (VertexPtr root, CodeGenerator &W) {
  OperationType tp = OpInfo::type (root->type());

  W << UpdateLocation (root->location);

  bool close_par = false;
  if (root->val_ref_flag != val_none) {
    close_par = true;
    if (root->val_ref_flag == val_r) {
      W << "val (";
    } else {
      W << "ref(";
    }
  }

  if (root->extra_type == op_ex_safe_version) {
    compile_safe_version (root, W);
  } else {
    switch (tp) {
      case prefix_op:
        compile_prefix_op (root, W);
        break;
      case postfix_op:
        compile_postfix_op (root, W);
        break;
      case binary_op:
      case binary_func_op:
        compile_binary_op (root, W);
        break;
      case ternary_op:
        compile_ternary_op (root, W);
        break;
      case common_op:
        compile_common_op (root, W);
        break;
      case cycle_op:
        compile_cycle_op (root, W);
        break;
      case conv_op:
        compile_conv_op (root, W);
        break;
      default:
        printf ("%d: %d\n", tp, root->type());
        assert (0);
        break;
    }
  }

  if (close_par) {
    W << ")";
  }
}
