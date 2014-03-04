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
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftw.h>

#include "stage.h"
#include "data_ptr.h"
#include "io.h"
#include "name-gen.h"
#include "data.h"
#include "compiler.h"
#include "function-pass.h"

#include "bicycle.h"

/*** Index ***/
void mkpath (const string &s);
//TODO: review this code. It is just some scratch of small part of future incremental compilation
class FileInfo {
  public:
    string path;
    time_t mtime;
    unsigned long long crc64;
    bool on_disk;
    bool needed;
    FileInfo (const string &path = "", time_t mtime = -1, unsigned long long crc = -1, bool on_disk = false);
    FileInfo (const FileInfo &from);
    FileInfo &operator = (const FileInfo &from);
};
inline bool operator < (const FileInfo &a, const FileInfo &b);
inline bool operator == (const FileInfo &a, const FileInfo &b);
inline bool operator != (const FileInfo &a, const FileInfo &b);

int scan_dir_callback (const char *fpath, const struct stat *sb, int typeflag);
vector <FileInfo> scan_dir (const string &dir);

class Index {
  private:
    vector <FileInfo> files;
    vector <FileInfo> new_files;

  public:
    void sync_with_dir (string dir);
    FileInfo *get_file_static (const string &name);
    FileInfo *get_file_dyn (const string &name, bool force = false);
    FileInfo &get_file (const string &file_name);
    void flush_changes();
    void del_extra_files();

    //stupid text version. to be improved
    void save (FILE *f);
    void load (FILE *f);
};

/*** Core ***/
//Consists mostly of functions that require synchronization
typedef enum {fs_function, fs_member_function} function_set_t;
class CompilerCore {
  private:
    Index cpp_index;
    HT <SrcFilePtr> file_ht;
    HT <FunctionSetPtr> function_set_ht;
    HT <DefinePtr> defines_ht;
    HT <VarPtr> global_vars_ht;
    vector <SrcFilePtr> main_files;
    CompilerArgs *args_;

    bool add_to_function_set (FunctionSetPtr function_set, FunctionPtr function,
                              bool req = false);
    FunctionPtr create_function (VertexAdaptor <meta_op_function>  function_root);

    inline bool try_require_file (SrcFilePtr file) {
      return __sync_bool_compare_and_swap (&file->is_required, false, true);
    }
  public:
    CompilerCore();
    void start();
    void finish();
    void register_args (CompilerArgs *args);
    const CompilerArgs &args();
    string unify_file_name (const string &file_name);
    SrcFilePtr register_file (const string &file_name);

    template <class DataStream>
    void register_main_file (const string &file_name, DataStream &os);
    template <class DataStream>
    pair <SrcFilePtr, bool> require_file (const string &file_name, DataStream &os);

    template <class DataStream>
    void require_function_set (FunctionSetPtr function_set, FunctionPtr by_function,
                               DataStream &os);

    template <class DataStream>
    void require_function_set (function_set_t type, const string &name,
                               FunctionPtr by_function, DataStream &os);
    template <class DataStream>
    void register_function_header (VertexAdaptor <meta_op_function> function_header, DataStream &os);
    template <class DataStream>
    void register_function (VertexPtr root, DataStream &os);
    FunctionSetPtr get_function_set (function_set_t type, const string &name, bool force);

    bool register_define (DefinePtr def_id);
    DefinePtr get_define (const string &name);

    VarPtr create_var (const string &name, VarData::Type type);
    VarPtr get_global_var (const string &name, VarData::Type type, VertexPtr init_val);
    VarPtr create_local_var (FunctionPtr function, const string &name, VarData::Type type);

    const vector <SrcFilePtr> &get_main_files();
    vector <VarPtr> get_global_vars();

    void load_index();
    void save_index();
    FileInfo *get_file_info (const string &file_name);
    void del_extra_files();
    void init_dest_dir();
};

extern CompilerCore *G;

/*** Misc functions ***/
bool try_optimize_var (VarPtr var);
VertexPtr conv_to_func_ptr (VertexPtr call);
VertexPtr set_func_id (VertexPtr call, FunctionPtr func);
VertexPtr try_set_func_id (VertexPtr call);

#include "compiler-core.hpp"
