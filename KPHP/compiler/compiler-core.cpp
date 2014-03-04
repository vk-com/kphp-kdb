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

#include "compiler-core.h"

#include "gentree.h"

static vector <FileInfo> *scan_dir_dest;
int scan_dir_callback (const char *fpath, const struct stat *sb, int typeflag) {
  if (typeflag == FTW_D) {
    //skip
  } else if (typeflag == FTW_F) {
    scan_dir_dest->push_back (FileInfo (fpath, sb->st_mtime, -1, true));
  } else {
    kphp_error (0, dl_pstr ("Failed to scan directory [fpath=%s]\n", fpath));
    kphp_fail();
  }
  return 0;
}

vector <FileInfo> scan_dir (const string &dir) {
  vector <FileInfo> res;
  scan_dir_dest = &res;
  int err = ftw (dir.c_str(), scan_dir_callback, 10);
  dl_passert (err == 0, dl_pstr ("ftw [%s] failed", dir.c_str()));
  return res;
}

inline void mkpath (const string &s) {
  string path = s;
  int n = (int)s.size();
  for (int i = 1; i <= n; i++) {
    if (i == n || path[i] == '/') {
      if (i != n) {
        path[i] = 0;
      }

      int f = mkdir (path.c_str(), 0777);
      dl_passert (f == 0 || errno == EEXIST, 
          dl_pstr ("Failed to mkdir [%s]", path.c_str()));

      if (i != n) {
        path[i] = '/';
      }
    }
  }
}
//TODO: review this code. It is just some scratch of small part of future incremental compilation
FileInfo::FileInfo (const string &path/* = ""*/, time_t mtime/* = -1*/, unsigned long long crc/* = -1*/, bool on_disk/* = false*/) :
  path (path),
  mtime (mtime),
  crc64 (crc),
  on_disk (on_disk),
  needed (false) {
  }
FileInfo::FileInfo (const FileInfo &from) :
  path (from.path),
  mtime (from.mtime),
  crc64 (from.crc64),
  on_disk (from.on_disk),
  needed (from.needed) {
  }
FileInfo &FileInfo::operator = (const FileInfo &from) {
  path = from.path;
  mtime = from.mtime;
  crc64 = from.crc64;
  on_disk = from.on_disk;
  needed = from.needed;
  return *this;
}

int scan_dir_callback (const char *fpath, const struct stat *sb, int typeflag);
vector <FileInfo> scan_dir (const string &dir);

void Index::sync_with_dir (string dir) {
  vector <FileInfo> dir_files = scan_dir (dir);
  sort (dir_files.begin(), dir_files.end());

  int i = 0;
  for (int dir_files_i = 0; dir_files_i < (int)dir_files.size(); dir_files_i++) {
    while (i < (int)files.size() && files[i] < dir_files[dir_files_i]) {
      i++;
    }
    if (i < (int)files.size() && files[i] == dir_files[dir_files_i]) {
      if (dir_files[dir_files_i].mtime == files[i].mtime) {
        dir_files[dir_files_i].crc64 = files[i].crc64;
      }
    }
  }

  swap (files, dir_files);
}

FileInfo *Index::get_file_static (const string &name) {
  FileInfo needle (name);
  vector <FileInfo>::iterator it = std::lower_bound (files.begin(), files.end(), needle);

  if (it == files.end() || *it != needle) {
    return NULL;
  }
  return &*it;
}

FileInfo *Index::get_file_dyn (const string &name, bool force/* = false*/) {
  FileInfo needle (name);
  vector <FileInfo>::iterator it = std::lower_bound (files.begin(), files.end(), needle);

  if (it == files.end() || *it != needle) {
    if (!force) {
      return NULL;
    }
    it = files.insert (it, needle);
  }
  return &*it;
}

FileInfo &Index::get_file (const string &file_name) {
  FileInfo *file;
  file = get_file_static (file_name);
  if (file != NULL) {
    return *file;
  }

  file = get_file_dyn (file_name, true);
  return *file;
}

void Index::flush_changes() {
  if (new_files.empty()) {
    return;
  }
  vector <FileInfo> res;
  std::merge (files.begin(), files.end(), new_files.begin(), new_files.end(), back_inserter (res));
  std::swap (files, res);
  new_files.clear();
}

void Index::del_extra_files() {
  flush_changes();
  vector <FileInfo> res;
  for (int i = 0; i < (int)files.size(); i++) {
    if (!files[i].needed) {
      string path = files[i].path;
      int n = (int)path.size();
      if ((n >= 2 && path[n - 2] == '.' && path[n - 1] == 'h') ||
          (n >= 4 && path[n - 4] == '.' && path[n - 3] == 'c' && path[n - 2] == 'p' && path[n - 1] == 'p')) {
      } else {
        kphp_error (0, dl_pstr ("Unexpected file [%s] in destination folder is found", path.c_str()));
        return;
      }
    }
  }
  for (int i = 0; i < (int)files.size(); i++) {
    if (files[i].needed) {
      res.push_back (files[i]);
    } else {
      fprintf (stderr, "unlink %s\n", files[i].path.c_str());
      int err = unlink (files[i].path.c_str());
      if (err != 0) {
        kphp_error (0, "Failed to unlink file");
        kphp_fail();
      }
    }
  }
}

//stupid text version. to be improved
void Index::save (FILE *f) {
  dl_pcheck (fprintf (f, "%d\n", (int)files.size()));
  for (int i = 0; i < (int)files.size(); i++) {
    dl_pcheck (fprintf (f, "%s %llu %llu\n", files[i].path.c_str(), (unsigned long long)files[i].mtime, files[i].crc64));
  }
}
void Index::load (FILE *f) {
  int n;
  int err = fscanf (f, "%d\n", &n);
  dl_passert (err == 1, "Failed to load index");
  files = vector <FileInfo>(n);
  for (int i = 0; i < n; i++) {
    char tmp[500];
    unsigned long long mtime;
    unsigned long long crc64;
    int err = fscanf (f, "%s %llu %llu", tmp, &mtime, &crc64);
    dl_passert (err == 3, "Failed to load index");
    files[i].path = tmp;
    files[i].mtime = mtime;
    files[i].crc64 = crc64;
  }
}

CompilerCore::CompilerCore() :
  args_ (NULL) {
}
void CompilerCore::start() {
  PROF (total).start();
  stage::die_if_global_errors();
  load_index();
}

void CompilerCore::finish() {
  stage::die_if_global_errors();
  del_extra_files();
  save_index();
  stage::die_if_global_errors();

  delete args_;
  args_ = NULL;

  PROF (total).finish();
}
void CompilerCore::register_args (CompilerArgs *args) {
  kphp_assert (args_ == NULL);
  args_ = args;
}
const CompilerArgs &CompilerCore::args() {
  kphp_assert (args_ != NULL);
  return *args_;
}


bool CompilerCore::add_to_function_set (FunctionSetPtr function_set, FunctionPtr function,
    bool req) {
  AutoLocker <FunctionSetPtr> locker (function_set);
  if (req) {
    kphp_assert (function_set->size() == 0);
    function_set->is_required = true;
  }
  function->function_set = function_set;
  function_set->add_function (function);
  return function_set->is_required;
}

FunctionSetPtr CompilerCore::get_function_set (function_set_t type, const string &name, bool force) {
  HT <FunctionSetPtr> *ht = &function_set_ht;

  HT <FunctionSetPtr>::HTNode *node = ht->at (hash_ll (name));
  if (node->data.is_null()) {
    if (!force) {
      return FunctionSetPtr();
    }
    AutoLocker <Lockable *> locker (node);
    if (node->data.is_null()) {
      FunctionSetPtr new_func_set = FunctionSetPtr (new FunctionSet());
      new_func_set->name = name;
      node->data = new_func_set;
    }
  }
  FunctionSetPtr function_set = node->data;
  kphp_assert (function_set->name == name/*, "Bug in compiler: hash collision"*/);
  return function_set;
}

FunctionPtr CompilerCore::create_function (VertexAdaptor <meta_op_function>  function_root) {
  AUTO_PROF (create_function);
  string function_name = function_root->name().as <op_func_name>()->str_val;
  FunctionPtr function = FunctionPtr (new FunctionData());

  function->name = function_name;
  function->root = function_root;
  function_root->set_func_id (function);
  function->file_id = stage::get_file();

  if (function_root->type() == op_func_decl) {
    function->is_extern = true;
    function->type() = FunctionData::func_extern;
  } else {
    switch (function_root->extra_type) {
      case op_ex_func_switch:
        function->type() = FunctionData::func_switch;
        break;
      case op_ex_func_global:
        function->type() = FunctionData::func_global;
        break;
      default:
        function->type() = FunctionData::func_local;
        break;
    }
  }

  if (function->type() == FunctionData::func_global) {
    if (stage::get_file()->main_func_name == function->name) {
      stage::get_file()->main_function = function;
    }
  }

  return function;
}

string CompilerCore::unify_file_name (const string &file_name) {
  if (args().base_dir.empty()) { //hack: directory of first file will be used ad base_dir
    size_t i = file_name.find_last_of ("/");
    kphp_assert (i != string::npos);
    args_->set_base_dir (file_name.substr (0, i + 1));
  }
  const string &base_dir = args().base_dir;
  if (strncmp (file_name.c_str(), base_dir.c_str(), base_dir.size())) {
    return file_name;
  }
  return file_name.substr (base_dir.size());
}

SrcFilePtr CompilerCore::register_file (const string &file_name) {
  if (file_name.empty()) {
    return SrcFilePtr();
  }

  //search file
  string full_file_name;
  if (file_name[0] != '/' && file_name[0] != '.') {
    int n = (int)args().include_dirs.size();
    for (int i = 0; i < n && full_file_name.empty(); i++) {
      full_file_name = get_full_path (args().include_dirs[i] + file_name);
    }
  }
  if (file_name[0] == '/') {
    full_file_name = get_full_path (file_name);
  } else if (full_file_name.empty()) {
    vector <string> cur_include_dirs;
    SrcFilePtr from_file = stage::get_file();
    if (from_file.not_null()) {
      string from_file_name = from_file->file_name;
      size_t en = from_file_name.find_last_of ('/');
      assert (en != string::npos);
      string cur_dir = from_file_name.substr (0, en + 1);
      cur_include_dirs.push_back (cur_dir);
    }
    if (from_file.is_null() || file_name[0] != '.') {
      cur_include_dirs.push_back ("");
    }
    int n = (int)cur_include_dirs.size();
    for (int i = 0; i < n && full_file_name.empty(); i++) {
      full_file_name = get_full_path (cur_include_dirs[i] + file_name);
    }
  }

  kphp_error_act (
    !full_file_name.empty(),
    dl_pstr ("Cannot load file [%s]", file_name.c_str()),
    return SrcFilePtr()
  );


  //find short_file_name
  int st = -1;
  int en = (int)full_file_name.size();
  for (int i = en - 1; i > st; i--) {
    if (full_file_name[i] == '/') {
      st = i;
      break;
    }
  }
  st++;

  int dot_pos = en;
  for (int i = st; i < en; i++) {
    if (full_file_name[i] == '.') {
      dot_pos = i;
    }
  }
  //TODO: en == full_file_name.size()
  string short_file_name = full_file_name.substr (st, dot_pos - st);
  for (int i = 0; i < (int)short_file_name.size(); i++) {
    if (short_file_name[i] == '.') {
    }
  }
  string extension = full_file_name.substr (dot_pos + 1);
  if (extension != "php") {
    short_file_name += "_";
    short_file_name += extension;
  }

  //register file if needed
  HT <SrcFilePtr>::HTNode *node = file_ht.at (hash_ll (full_file_name));
  if (node->data.is_null()) {
    AutoLocker <Lockable *> locker (node);
    if (node->data.is_null()) {
      SrcFilePtr new_file = SrcFilePtr (new SrcFile (full_file_name, short_file_name));
      char tmp[50];
      sprintf (tmp, "%x", hash (full_file_name));
      string func_name = gen_unique_name ("src$" + new_file->short_file_name + tmp, true);
      new_file->main_func_name = func_name;
      new_file->unified_file_name = unify_file_name (new_file->file_name);
      node->data = new_file;
    }
  }
  SrcFilePtr file = node->data;
  return file;
}

bool CompilerCore::register_define (DefinePtr def_id) {
  HT <DefinePtr>::HTNode *node = defines_ht.at (hash_ll (def_id->name));
  AutoLocker <Lockable *> locker (node);

  kphp_error_act (
    node->data.is_null(),
    dl_pstr ("Redeclaration of define [%s], the previous declaration was in [%s]",
        def_id->name.c_str(),
        node->data->file_id->file_name.c_str()),
    return false
  );

  node->data = def_id;
  return true;
}

DefinePtr CompilerCore::get_define (const string &name) {
  return defines_ht.at (hash_ll (name))->data;
}
VarPtr CompilerCore::create_var (const string &name, VarData::Type type) {
  VarPtr var = VarPtr (new VarData (type));
  var->name = name;
  return var;
}

VarPtr CompilerCore::get_global_var (const string &name, VarData::Type type,
    VertexPtr init_val) {
  HT <VarPtr>::HTNode *node = global_vars_ht.at (hash_ll (name));
  VarPtr new_var;
  if (node->data.is_null()) {
    AutoLocker <Lockable *> locker (node);
    if (node->data.is_null()) {
      new_var = create_var (name, type);
      new_var->init_val = init_val;
      new_var->is_constant = type == VarData::var_const_t;
      node->data = new_var;
    }
  }
  VarPtr var = node->data;
  if (new_var.is_null()) {
    kphp_assert (var->name == name/*, "bug in compiler (hash collision)"*/);
    if (init_val.not_null() && init_val->type() == op_string) {
      kphp_assert (var->init_val->type() == op_string);
      kphp_assert (var->init_val->get_string() == init_val->get_string());
    }
  }
  return var;
}

VarPtr CompilerCore::create_local_var (FunctionPtr function, const string &name,
    VarData::Type type) {
  VarData::Type real_type = type;
  if (type == VarData::var_static_t) {
    real_type = VarData::var_global_t;
  }
  VarPtr var = create_var (name, real_type);
  var->holder_func = function;
  switch (type) {
    case VarData::var_local_t:
      function->local_var_ids.push_back (var);
      break;
    case VarData::var_static_t:
      function->static_var_ids.push_back (var);
      break;
    case VarData::var_param_t:
      var->param_i = (int)function->param_ids.size();
      function->param_ids.push_back (var);
      break;
    default:
      kphp_fail();
  }
  return var;
}

const vector <SrcFilePtr> &CompilerCore::get_main_files() {
  return main_files;
}
vector <VarPtr> CompilerCore::get_global_vars() {
  return global_vars_ht.get_all();
}
void CompilerCore::load_index() {
  string index_path = args().index_path;
  if (index_path.empty()) {
    return;
  }
  FILE *f = fopen (index_path.c_str(), "r");
  if (f == NULL) {
    return;
  }
  cpp_index.load (f);
  fclose (f);
}

void CompilerCore::save_index() {
  string index_path = args().index_path;
  if (index_path.empty()) {
    return;
  }
  string tmp_index_path = index_path + ".tmp";
  FILE *f = fopen (tmp_index_path.c_str(), "w");
  if (f == NULL) {
    return;
  }
  cpp_index.save (f);
  fclose (f);
  int err = system (("mv " + tmp_index_path + " " + index_path).c_str());
  kphp_error (err == 0, "Failed to rewrite index");
  kphp_fail();
}

FileInfo *CompilerCore::get_file_info (const string &file_name) {
  return &cpp_index.get_file (file_name);
}

void CompilerCore::del_extra_files() {
  cpp_index.del_extra_files();
}

void CompilerCore::init_dest_dir() {
  args_->dest_dir += "kphp/";
  mkpath (args().dest_dir);
  cpp_index.sync_with_dir (args().dest_dir);
}

CompilerCore *G;

bool try_optimize_var (VarPtr var) {
  return __sync_bool_compare_and_swap (&var->optimize_flag, false, true);
}

VertexPtr conv_to_func_ptr (VertexPtr call) {
  if (call->type() != op_func_ptr) {
    VertexPtr name_v = GenTree::get_actual_value (call);
    string name;
    if (name_v->type() == op_string) {
      name = name_v.as <op_string>()->str_val;
    } else if (name_v->type() == op_func_name) {
      name = name_v.as <op_func_name>()->str_val;
    }
    if (!name.empty()) {
      CREATE_VERTEX (new_call, op_func_ptr);
      new_call->str_val = name;
      set_location (new_call, name_v->get_location());
      call = new_call;
    }
  }

  return call;
}

VertexPtr set_func_id (VertexPtr call, FunctionPtr func) {
  kphp_assert (call->type() == op_func_ptr || call->type() == op_func_call);
  kphp_assert (func.not_null());
  kphp_assert (call->get_func_id().is_null() || call->get_func_id() == func);
  if (call->get_func_id() == func) {
    return call;
  }
  //fprintf (stderr, "%s\n", func->name.c_str());

  call->set_func_id (func);
  if (call->type() == op_func_ptr) {
    func->is_callback = true;
    return call;
  }

  if (func->root.is_null()) {
    kphp_fail();
    return call;
  }

  VertexAdaptor <meta_op_function> func_root = func->root;
  VertexAdaptor <op_func_param_list> param_list = func_root->params();
  VertexRange call_args = call.as <op_func_call>()->args();
  VertexRange func_args = param_list->params();
  int call_args_n = (int)call_args.size();
  int func_args_n = (int)func_args.size();

  if (func->varg_flag) {
    for (int i = 0; i < call_args_n; i++) {
      kphp_error_act (
        call_args[i]->type() != op_func_name,
        "Unexpected function pointer",
        return VertexPtr()
      );
    }
    VertexPtr args;
    if (call_args_n == 1 && call_args[0]->type() == op_varg) {
      args = VertexAdaptor <op_varg> (call_args[0])->expr();
    } else {
      CREATE_VERTEX (new_args, op_array, call->get_next());
      args = new_args;
    }
    vector <VertexPtr> tmp (1, GenTree::conv_to <tp_array> (args));
    COPY_CREATE_VERTEX (new_call, call, op_func_call, tmp);
    return new_call;
  }

  for (int i = 0; i < call_args_n; i++) {
    if (i < func_args_n) {
      if (func_args[i]->type() == op_func_param) {
        kphp_error_act (
          call_args[i]->type() != op_func_name,
          "Unexpected function pointer",
          continue
        );
        VertexAdaptor <op_func_param> param = func_args[i];
        if (param->type_help != tp_Unknown) {
          call_args[i] = GenTree::conv_to (call_args[i], param->type_help, param->var()->ref_flag);
        }
      } else if (func_args[i]->type() == op_func_param_callback) {
        call_args[i] = conv_to_func_ptr (call_args[i]);
        kphp_error (call_args[i]->type() == op_func_ptr, "Function pointer expected");
      } else {
        kphp_fail();
      }
    }
  }
  return call;
}

VertexPtr try_set_func_id (VertexPtr call) {
  if (call->type() != op_func_ptr && call->type() != op_func_call) {
    return call;
  }

  if (call->get_func_id().not_null()) {
    return call;
  }

  const string &name = call->get_string();
  FunctionSetPtr function_set = G->get_function_set (fs_function, name, true);
  FunctionPtr function;
  int functions_cnt = (int)function_set->size();

  kphp_error_act (
    functions_cnt != 0,
    dl_pstr ("Unknown function [%s]\n%s\n", name.c_str(), 
      stage::get_function_history().c_str()),
    return call
  );

  kphp_assert (function_set->is_required);

  if (functions_cnt == 1) {
    function = function_set[0];
  }

  kphp_error_act (
    function.not_null(),
    dl_pstr ("Function overloading is not supported properly [%s]", name.c_str()),
    return call
  );

  call = set_func_id (call, function);
  return call;
}
