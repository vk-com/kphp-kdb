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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
*/

#ifndef __FILESYS_PENDING_OPERATIONS_H__
#define __FILESYS_PENDING_OPERATIONS_H__

#include <sys/stat.h>

enum pending_operation_tp {
  pot_null = -1,
  pot_mkdir = 0,
  pot_symlink = 1,
  pot_rename = 2,
  pot_remove = 3,
  pot_copy_attrs = 4
};

struct pending_operation {
  enum pending_operation_tp type;
  char *oldpath;
  char *newpath;
  struct stat st;
  struct pending_operation *next;
};

struct static_pending_operation {
  enum pending_operation_tp type;
  char oldpath[PATH_MAX];
  char newpath[PATH_MAX];
  struct stat st;
  char oldpath_set;
  char newpath_set;
};

extern struct pending_operation *pol_head, *pol_tail;

void pending_operation_fill (struct static_pending_operation *P, enum pending_operation_tp type, const char *const oldpath, const char *const newpath, struct stat *st);
void pending_operations_init (const char *const olddir_prefix, const char *const newdir_prefix);
void pending_operations_reset (char use_mark);
struct pending_operation *pending_operation_create (enum pending_operation_tp type, const char *const oldpath, const char *const newpath, struct stat *st);
struct pending_operation *pending_operation_load (struct static_pending_operation *P);
void pending_operation_push (struct pending_operation *P);
void pending_operations_apply (void);

/* if P == NULL: insert immediately in PO queue, otherwise returns PO in *P - static structure
   which could be inserted into queue using push (load (P))
   if release_mark != NULL && P == NULL: zmemory will be released before pending_operation_push (pending_operation_create (...)) call
*/
void pending_operation_copyfile (int transaction_id, int *transaction_file_no, const char *const name, void *data, int data_size, struct stat *S, struct static_pending_operation *P, dyn_mark_t release_mark);

#endif
