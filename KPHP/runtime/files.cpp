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

#define _FILE_OFFSET_BITS 64

#include <errno.h>
#include <libgen.h>
#include <sys/utsname.h>
#include <unistd.h>

#undef basename

#include "files.h"
#include "interface.h"
#include "string_functions.h"//php_buf, TODO

string raw_post_data;//TODO static

static int opened_fd;

const string LETTER_a ("a", 1);

int close_safe (int fd) {
  dl::enter_critical_section();//OK
  int result = close (fd);
  php_assert (fd == opened_fd);
  opened_fd = -1;
  dl::leave_critical_section();
  return result;
}

int open_safe (const char *pathname, int flags) {
  dl::enter_critical_section();//OK
  opened_fd = open (pathname, flags);
  dl::leave_critical_section();
  return opened_fd;
}

int open_safe (const char *pathname, int flags, mode_t mode) {
  dl::enter_critical_section();
  opened_fd = open (pathname, flags, mode);
  dl::leave_critical_section();
  return opened_fd;
}

ssize_t read_safe (int fd, void *buf, size_t len) {
  dl::enter_critical_section();//OK
  size_t full_len = len;
  do {
    ssize_t cur_res = read (fd, buf, len);
    if (cur_res == -1) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      dl::leave_critical_section();
      return -1;
    }
    php_assert (cur_res >= 0);

    buf = (char *)buf + cur_res;
    len -= cur_res;
  } while (len > (size_t)0);
  dl::leave_critical_section();

  return full_len - len;
}

ssize_t write_safe (int fd, const void *buf, size_t len) {
  dl::enter_critical_section();//OK
  size_t full_len = len;
  do {
    ssize_t cur_res = write (fd, buf, len);
    if (cur_res == -1) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      dl::leave_critical_section();
      return -1;
    }
    php_assert (cur_res >= 0);

    buf = (const char *)buf + cur_res;
    len -= cur_res;
  } while (len > (size_t)0);
  dl::leave_critical_section();

  return full_len - len;
}


#define read read_disabled
#define write write_disabled


string f$basename (const string &name, const string &suffix) {
  string name_copy (name.c_str(), name.size());
  dl::enter_critical_section();//OK
  const char *result_c_str = __xpg_basename (name_copy.buffer());
  dl::leave_critical_section();
  int l = (int)strlen (result_c_str);
  if ((int)suffix.size() <= l && !strcmp (result_c_str + l - suffix.size(), suffix.c_str())) {
    l -= suffix.size();
  }
  return string (result_c_str, (dl::size_type)l);
}

bool f$chmod (const string &s, int mode) {
  dl::enter_critical_section();//OK
  bool result = (chmod (s.c_str(), (mode_t)mode) >= 0);
  dl::leave_critical_section();
  return result;
}

void f$clearstatcache (void) {
  //TODO
}

bool f$copy (const string &from, const string &to) {
  struct stat stat_buf;
  dl::enter_critical_section();//OK
  int read_fd = open (from.c_str(), O_RDONLY);
  if (read_fd < 0) {
    dl::leave_critical_section();
    return false;
  }
  if (fstat (read_fd, &stat_buf) < 0) {
    close (read_fd);
    dl::leave_critical_section();
    return false;
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    php_warning ("Regular file expected as first argument in function copy, \"%s\" is given", from.c_str());
    close (read_fd);
    dl::leave_critical_section();
    return false;
  }

  int write_fd = open (to.c_str(), O_WRONLY | O_CREAT | O_TRUNC, stat_buf.st_mode);
  if (write_fd < 0) {
    close (read_fd);
    dl::leave_critical_section();
    return false;
  }

  size_t size = stat_buf.st_size;
  while (size > 0) {
    size_t len = min (size, (size_t)PHP_BUF_LEN);
    if (read_safe (read_fd, php_buf, len) < (ssize_t)len) {
      break;
    }
    if (write_safe (write_fd, php_buf, len) < (ssize_t)len) {
      break;
    }
    size -= len;
  }

  close (read_fd);
  close (write_fd);
  dl::leave_critical_section();
  return size == 0;
}

string f$dirname (const string &name) {
  string name_copy (name.c_str(), name.size());
  dl::enter_critical_section();//OK
  const char *result_c_str = dirname (name_copy.buffer());
  dl::leave_critical_section();
  return string (result_c_str, (dl::size_type)strlen (result_c_str));;
}

OrFalse <array <string> > f$file (const string &name) {
  struct stat stat_buf;
  dl::enter_critical_section();//OK
  int file_fd = open_safe (name.c_str(), O_RDONLY);
  if (file_fd < 0) {
    dl::leave_critical_section();
    return false;
  }
  if (fstat (file_fd, &stat_buf) < 0) {
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    php_warning ("Regular file expected as first argument in function file, \"%s\" is given", name.c_str());
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  size_t size = stat_buf.st_size;
  if (size > string::max_size) {
    php_warning ("File \"%s\" is too large", name.c_str());
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }
  dl::leave_critical_section();

  string res ((dl::size_type)size, false);

  dl::enter_critical_section();//OK
  char *s = &res[0];
  if (read_safe (file_fd, s, size) < (ssize_t)size) {
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  close_safe (file_fd);
  dl::leave_critical_section();

  array <string> result;
  int prev = -1;
  for (int i = 0; i < (int)size; i++) {
    if (s[i] == '\n' || i + 1 == (int)size) {
      result.push_back (string (s + prev + 1, i - prev));
      prev = i;
    }
  }

  return result;
}

OrFalse <string> f$file_get_contents (const string &name) {
  if ((int)name.size() == 11 && !memcmp (name.c_str(), "php://input", 11)) {
    return raw_post_data;
  }

  struct stat stat_buf;
  dl::enter_critical_section();//OK
  int file_fd = open_safe (name.c_str(), O_RDONLY);
  if (file_fd < 0) {
    dl::leave_critical_section();
    return false;
  }
  if (fstat (file_fd, &stat_buf) < 0) {
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    php_warning ("Regular file expected as first argument in function file_get_contents, \"%s\" is given", name.c_str());
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  size_t size = stat_buf.st_size;
  if (size > string::max_size) {
    php_warning ("File \"%s\" is too large", name.c_str());
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }
  dl::leave_critical_section();

  string res ((dl::size_type)size, false);

  dl::enter_critical_section();//OK
  if (read_safe (file_fd, &res[0], size) < (ssize_t)size) {
    close_safe (file_fd);
    dl::leave_critical_section();
    return false;
  }

  close_safe (file_fd);
  dl::leave_critical_section();
  return res;
}

bool f$file_exists (const string &name) {
  dl::enter_critical_section();//OK
  bool result = (access (name.c_str(), F_OK) == 0);
  dl::leave_critical_section();
  return result;
}

OrFalse <int> f$filesize (const string &name) {
  struct stat stat_buf;
  dl::enter_critical_section();//OK
  int file_fd = open (name.c_str(), O_RDONLY);
  if (file_fd < 0) {
    dl::leave_critical_section();
    return false;
  }
  if (fstat (file_fd, &stat_buf) < 0) {
    close (file_fd);
    dl::leave_critical_section();
    return false;
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    php_warning ("Regular file expected as first argument in function filesize, \"%s\" is given", name.c_str());
    close (file_fd);
    dl::leave_critical_section();
    return false;
  }

  size_t size = stat_buf.st_size;
  if (size > INT_MAX) {
    php_warning ("File \"%s\" size is greater than 2147483647", name.c_str());
    size = INT_MAX;
  }

  close (file_fd);
  dl::leave_critical_section();
  return (int)size;
}

bool f$is_dir (const string &name) {
  struct stat stat_buf;
  dl::enter_critical_section();//OK
  if (lstat (name.c_str(), &stat_buf) < 0) {
    dl::leave_critical_section();
    return false;
  }
  dl::leave_critical_section();

  return S_ISDIR (stat_buf.st_mode);
}

bool f$is_file (const string &name) {
  struct stat stat_buf;
  dl::enter_critical_section();//OK
  if (lstat (name.c_str(), &stat_buf) < 0) {
    dl::leave_critical_section();
    return false;
  }
  dl::leave_critical_section();

  return S_ISREG (stat_buf.st_mode);
}

bool f$is_readable (const string &name) {
  dl::enter_critical_section();//OK
  bool result = (access (name.c_str(), R_OK) == 0);
  dl::leave_critical_section();
  return result;
}

bool f$is_writeable (const string &name) {
  dl::enter_critical_section();//OK
  bool result = (access (name.c_str(), W_OK) == 0);
  dl::leave_critical_section();
  return result;
}

bool f$mkdir (const string &name, int mode) {
  dl::enter_critical_section();//OK
  bool result = (mkdir (name.c_str(), (mode_t)mode) >= 0);
  dl::leave_critical_section();
  return result;
}

string f$php_uname (const string &name) {
  utsname res;
  dl::enter_critical_section();//OK
  if (uname (&res)) {
    dl::leave_critical_section();
    return string();
  }
  dl::leave_critical_section();

  char mode = name[0];
  switch (mode) {
    case 's':
      return string (res.sysname, (dl::size_type)strlen (res.sysname));
    case 'n':
      return string (res.nodename, (dl::size_type)strlen (res.nodename));
    case 'r':
      return string (res.release, (dl::size_type)strlen (res.release));
    case 'v':
      return string (res.version, (dl::size_type)strlen (res.version));
    case 'm':
      return string (res.machine, (dl::size_type)strlen (res.machine));
    default: {
      string result (res.sysname, (dl::size_type)strlen (res.sysname));
      result.push_back (' ');
      result.append (res.nodename, (dl::size_type)strlen (res.nodename));
      result.push_back (' ');
      result.append (res.release, (dl::size_type)strlen (res.release));
      result.push_back (' ');
      result.append (res.version, (dl::size_type)strlen (res.version));
      result.push_back (' ');
      result.append (res.machine, (dl::size_type)strlen (res.machine));
      return result;
    }
  }
}

bool f$rename (const string &oldname, const string &newname) {
  dl::enter_critical_section();//OK
  bool result = (rename (oldname.c_str(), newname.c_str()) == 0);
  if (!result && errno == EXDEV) {
    result = f$copy (oldname, newname);
    if (result) {
      result = f$unlink (oldname);
      if (!result) {
        f$unlink (newname);
      }
    }
  }
  if (!result) {
    php_warning ("Can't rename \"%s\" to \"%s\"", oldname.c_str(), newname.c_str());
  }
  dl::leave_critical_section();
  return result;
}

OrFalse <string> f$realpath (const string &path) {
  char real_path[PATH_MAX];

  dl::enter_critical_section();//OK
  bool result = (realpath (path.c_str(), real_path) != NULL);
  dl::leave_critical_section();

  if (result) {
    return string (real_path, (dl::size_type)strlen (real_path));
  } else {
    php_warning ("Realpath is false on string \"%s\": %m", path.c_str());
  }
  return false;
}

static OrFalse <string> full_realpath (const string &path) {
  static char full_realpath_cache_storage[sizeof (array <string>)];
  static array <string> *full_realpath_cache = reinterpret_cast <array <string> *> (full_realpath_cache_storage);
  static long long full_realpath_last_query_num = -1;

  if ((int)path.size() == 1 && path[0] == '/') {
    return path;
  }

  char real_path[PATH_MAX];

  if (dl::query_num != full_realpath_last_query_num) {
    new (full_realpath_cache_storage) array <string>();
    full_realpath_last_query_num = dl::query_num;
  }

  string &result_cache = (*full_realpath_cache)[path];
  if (!result_cache.empty()) {
    return result_cache;
  }

  string dir = f$dirname (path);
  dl::enter_critical_section();//OK
  bool result = (realpath (dir.c_str(), real_path) != NULL);
  dl::leave_critical_section();

  if (result) {
    result_cache = (static_SB.clean() + real_path + '/' + f$basename (path)).str();
    return result_cache;
  }
  return false;
}

OrFalse <string> f$tempnam (const string &dir, const string &prefix) {
  string prefix_new = f$basename (prefix);
  prefix_new.shrink (5);
  if (prefix_new.empty()) {
    prefix_new.assign ("tmp.", 4);
  }

  string dir_new;
  OrFalse <string> dir_real;
  if (dir.empty() || !f$boolval (dir_real = f$realpath (dir))) {
    dl::enter_critical_section();//OK
    const char *s = getenv ("TMPDIR");
    dl::leave_critical_section();
    if (s != NULL && s[0] != 0) {
      int len = (int)strlen (s);

      if (s[len - 1] == '/') {
        len--;
      }

      dir_new.assign (s, len);
    } else if (P_tmpdir != NULL) {
      dir_new.assign (P_tmpdir, (dl::size_type)strlen (P_tmpdir));
    }

    if (dir_new.empty()) {
      php_critical_error ("can't find directory for temporary file in function tempnam");
      return false;
    }

    dir_real = f$realpath (dir_new);
    if (!f$boolval (dir_real)) {
      php_critical_error ("wrong directory \"%s\" found in function tempnam", dir_new.c_str());
      return false;
    }
  }

  dir_new = dir_real.val();
  php_assert (!dir_new.empty());

  if (dir_new[dir_new.size() - 1] != '/' && prefix_new[0] != '/') {
    dir_new.append (1, '/');
  }

  dir_new.append (prefix_new);
  dir_new.append (6, 'X');

  dl::enter_critical_section();//OK
  int fd = mkstemp (dir_new.buffer());
  if (fd == -1 || close (fd)) {
    dl::leave_critical_section();
    php_warning ("Can't create temporary file \"%s\" in function tempnam", dir_new.c_str());
    return false;
  }

  dl::leave_critical_section();
  return dir_new;
}

bool f$unlink (const string &name) {
  dl::enter_critical_section();//OK
  bool result = (unlink (name.c_str()) >= 0);
  dl::leave_critical_section();
  return result;
}

const MyFile STDOUT ("::STDOUT::", 10);
const MyFile STDERR ("::STDERR::", 10);

static char opened_files_storage[sizeof (array <FILE *>)];
static array <FILE *> *opened_files = reinterpret_cast <array <FILE *> *> (opened_files_storage);
static long long opened_files_last_query_num = -1;

MyFile f$fopen (const string &filename, const string &mode) {
  if (dl::query_num != opened_files_last_query_num) {
    new (opened_files_storage) array <FILE *>();
    opened_files->set_value (STDOUT, stdout);
    opened_files->set_value (STDERR, stderr);

    opened_files_last_query_num = dl::query_num;
  }
  if (eq2 (filename, STDOUT) || eq2 (filename, STDOUT)) {
    php_warning ("Can't open STDERR or STDOUT");
    return false;
  }

  OrFalse <string> real_filename_or_false = full_realpath (filename);
  if (!f$boolval (real_filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", filename.c_str());
    return false;
  }
  string real_filename = real_filename_or_false.val();
  if (opened_files->has_key (real_filename)) {
    php_warning ("File \"%s\" already opened. Closing previous one.", real_filename.c_str());
    f$fclose (MyFile (real_filename));
  }

  dl::enter_critical_section();//NOT OK: opened_files
  FILE *file = fopen (real_filename.c_str(), mode.c_str());
  if (file != NULL) {
    opened_files->set_value (real_filename, file);
    dl::leave_critical_section();

    return MyFile (real_filename);
  } else {
    dl::leave_critical_section();

    return MyFile (false);
  }
}

OrFalse <int> f$fwrite (const MyFile &file, const string &text) {
  if (eq2 (file, STDOUT)) {
    *coub += text;
    return (int)text.size();
  }

  int res = -1;
  if (eq2 (file, STDERR)) {
    dl::enter_critical_section();//OK
    res = (int)fwrite (text.c_str(), text.size(), 1, stderr);
    dl::leave_critical_section();
  } else {
    OrFalse <string> filename_or_false = full_realpath (file.to_string());
    if (!f$boolval (filename_or_false)) {
      php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
      return false;
    }
    string filename = filename_or_false.val();
    if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
      FILE *f = opened_files->get_value (filename);
      dl::enter_critical_section();//OK
      res = (int)fwrite (text.c_str(), text.size(), 1, f);
      dl::leave_critical_section();
    }
  }

  if (res < 0) {
    return false;
  }
  return res;
}

int f$fseek (const MyFile &file, int offset, int whence) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    php_warning ("Can't use fseek with STDERR and STDOUT\n");
    return -1;
  }
  const static int whences[3] = {SEEK_SET, SEEK_END, SEEK_CUR};
  if ((unsigned int)whence >= 3u) {
    php_warning ("Wrong parameter whence in function fseek\n");
    return -1;
  }
  whence = whences[whence];

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return -1;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    FILE *f = opened_files->get_value (filename);
    dl::enter_critical_section();//OK
    int res = fseek (f, (long)offset, whence);
    dl::leave_critical_section();
    return res;
  } else {
    php_warning ("File \"%s\" is not opened\n", filename.c_str());
    return -1;
  }
}

bool f$rewind (const MyFile &file) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    php_warning ("Can't use frewind with STDERR and STDOUT\n");
    return false;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    FILE *f = opened_files->get_value (filename);
    dl::enter_critical_section();//OK
    rewind (f);
    dl::leave_critical_section();
    return true;
  } else {
    php_warning ("File \"%s\" is not opened\n", filename.c_str());
    return false;
  }
}

OrFalse <int> f$ftell (const MyFile &file) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    php_warning ("Can't use ftell with STDERR and STDOUT\n");
    return false;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    FILE *f = opened_files->get_value (filename);
    dl::enter_critical_section();//OK
    int result = (int)ftell (f);
    dl::leave_critical_section();
    return result;
  } else {
    php_warning ("File \"%s\" is not opened\n", filename.c_str());
    return false;
  }
}

OrFalse <string> f$fread (const MyFile &file, int length) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    php_warning ("Can't use fread with STDERR and STDOUT\n");
    return false;
  }
  if (length <= 0) {
    php_warning ("Parameter length in function fread must be positive\n");
    return false;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  string res (length, false);
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    FILE *f = opened_files->get_value (filename);
    dl::enter_critical_section();//OK
    clearerr (f);
    size_t res_size = fread (&res[0], 1, length, f);
    if (ferror (f)) {
      dl::leave_critical_section();
      php_warning ("Error happened during fread from file \"%s\"", filename.c_str());
      return false;
    }
    dl::leave_critical_section();

    res.shrink ((dl::size_type)res_size);
    return res;
  } else {
    php_warning ("File \"%s\" is not opened\n", filename.c_str());
    return false;
  }
}

OrFalse <int> f$fpassthru (const MyFile &file) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    php_warning ("Can't use fpassthru with STDERR and STDOUT\n");
    return false;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    int result = 0;

    FILE *f = opened_files->get_value (filename);
    dl::enter_critical_section();//OK
    while (!feof (f)) {
      clearerr (f);
      size_t res_size = fread (&php_buf[0], 1, PHP_BUF_LEN, f);
      if (ferror (f)) {
        dl::leave_critical_section();
        php_warning ("Error happened during fpassthru from file \"%s\"", filename.c_str());
        return false;
      }
      coub->append (php_buf, (dl::size_type)res_size);
      result += (int)res_size;
    }
    dl::leave_critical_section();
    return result;
  } else {
    php_warning ("File \"%s\" is not opened\n", filename.c_str());
    return false;
  }
}

bool f$fflush (const MyFile &file) {
  if (eq2 (file, STDOUT)) {
    //TODO flush
    return false;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    dl::enter_critical_section();//OK
    fflush (opened_files->get_value (filename));
    dl::leave_critical_section();
    return true;
  }
  return false;
}

bool f$fclose (const MyFile &file) {
  if (eq2 (file, STDOUT) || eq2 (file, STDERR)) {
    return true;
  }

  OrFalse <string> filename_or_false = full_realpath (file.to_string());
  if (!f$boolval (filename_or_false)) {
    php_warning ("Wrong file \"%s\" specified", file.to_string().c_str());
    return false;
  }
  string filename = filename_or_false.val();
  if (dl::query_num == opened_files_last_query_num && opened_files->has_key (filename)) {
    dl::enter_critical_section();//NOT OK: opened_files
    fclose (opened_files->get_value (filename));
    opened_files->unset (filename);
    dl::leave_critical_section();
    return true;
  }
  return false;
}


void files_init_static (void) {
  opened_fd = -1;

  dl::enter_critical_section();//OK
  INIT_VAR(string, raw_post_data);
  dl::leave_critical_section();
}

void files_free_static (void) {
  dl::enter_critical_section();//OK
  if (dl::query_num == opened_files_last_query_num) {
    const array <FILE *> *const_opened_files = opened_files;
    for (array <FILE *>::const_iterator p = const_opened_files->begin(); p != const_opened_files->end(); ++p) {
      if (neq2 (p.get_key(), STDOUT) && neq2 (p.get_key(), STDERR)) {
        fclose (p.get_value());
      }
    }
    opened_files_last_query_num--;
  }

  if (opened_fd != -1) {
    close_safe (opened_fd);
  }

  CLEAR_VAR(string, raw_post_data);
  dl::leave_critical_section();
}

