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

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include "filesys-utils.h"
#include "kdb-data-common.h"
#include "filesys-pending-operations.h"
#include "server-functions.h"

/////////////////////////////////// Pending operations /////////////////////////////////////////////
static char *po_olddir = NULL, *po_newdir = NULL;
static int po_olddir_length, po_newdir_length;

struct pending_operation *pol_head = NULL, *pol_tail = NULL;

void pending_operations_init (const char *const olddir_prefix, const char *const newdir_prefix) {
  po_olddir = zstrdup (olddir_prefix);
  po_newdir = zstrdup (newdir_prefix);
  po_olddir_length = strlen (po_olddir);
  po_newdir_length = strlen (po_newdir);
}

struct pending_operation *pending_operation_create (enum pending_operation_tp type, const char *const oldpath, const char *const newpath, struct stat *st) {
  struct pending_operation *P = zmalloc0 (sizeof (struct pending_operation));
  P->type = type;
  if (oldpath) {
    P->oldpath = zstrdup (oldpath);
  }
  if (newpath) {
    P->newpath = zstrdup (newpath);
  }
  if (st) {
    memcpy (&P->st, st, sizeof (struct stat));
  }
  return P;
}

struct pending_operation *pending_operation_load (struct static_pending_operation *Q) {
  struct pending_operation *P = zmalloc0 (sizeof (struct pending_operation));
  P->type = Q->type;
  if (Q->oldpath_set) {
    P->oldpath = zstrdup (Q->oldpath);
  }
  if (Q->newpath_set) {
    P->newpath = zstrdup (Q->newpath);
  }
  memcpy (&P->st, &Q->st, sizeof (struct stat));
  return P;
}

void pending_operation_fill (struct static_pending_operation *P, enum pending_operation_tp type, const char *const oldpath, const char *const newpath, struct stat *st) {
  P->type = type;
  P->oldpath_set = P->newpath_set = 0;
  if (oldpath) {
    assert (strlen (oldpath) < PATH_MAX);
    strcpy (P->oldpath, oldpath);
    P->oldpath_set = 1;
  }
  if (newpath) {
    assert (strlen (newpath) < PATH_MAX);
    strcpy (P->newpath, newpath);
    P->newpath_set = 1;
  }
  if (st) {
    memcpy (&P->st, st, sizeof (struct stat));
  }
}

void pending_operation_push (struct pending_operation *P) {
  P->next = NULL;
  if (pol_head) {
    pol_tail->next = P;
    pol_tail = P;
  } else {
    pol_head = pol_tail = P;
  }
}

static dyn_mark_t po_mrk;
static char po_use_mark;

void pending_operations_reset (char use_mark) {
  pol_head = pol_tail = NULL;
  po_use_mark = use_mark;
  if (use_mark) {
    dyn_mark (po_mrk);
  }
}

static void pending_operations_clear (void) {
  struct pending_operation *P = pol_head, *W;
  for (P = pol_head; P != NULL; P = W) {
    W = P->next;
    if (P->oldpath) { zfree (P->oldpath, strlen (P->oldpath) + 1); }
    if (P->newpath) { zfree (P->newpath, strlen (P->newpath) + 1); }
    zfree (P, sizeof (*P));
  }
  pol_head = pol_tail = NULL;
}

static void pending_operation_dump (struct pending_operation *P) {
  kprintf ("type:%d, oldpath: %s, newpath: %s\n", P->type, P->oldpath, P->newpath);
}

#define PO_ASSERT(x) { int res = x; if (res < 0) { pending_operation_dump (P); kprintf ("PO_ASSERT (%s) fail in %s:%d (exit_code:%d). %m\n", __STRING(x), __FILE__, __LINE__, res); } }
void pending_operation_apply (struct pending_operation *P) {
  char full_oldpath[PATH_MAX], full_newpath[PATH_MAX];
  full_oldpath[0] = full_newpath[0] = 0;
  if (P->oldpath) {
    assert (snprintf (full_oldpath, PATH_MAX, "%s/%s", po_olddir, P->oldpath) < PATH_MAX);
  }
  if (P->newpath) {
    assert (snprintf (full_newpath, PATH_MAX, "%s/%s", po_newdir, P->newpath) < PATH_MAX);
  }
  switch (P->type) {
    case pot_null:
      kprintf ("pending_operation_apply (P.type == pot_null)\n");
      exit (1);
      break;
    case pot_mkdir:
      PO_ASSERT (mkdir (full_newpath, P->st.st_mode));
      PO_ASSERT (lcopy_attrs (full_newpath, &P->st));
      break;
    case pot_symlink:
      PO_ASSERT (symlink (P->oldpath, full_newpath));
      PO_ASSERT (lcopy_attrs (full_newpath, &P->st));
      break;
    case pot_rename:
      PO_ASSERT (rename (full_oldpath, full_newpath));
      PO_ASSERT (lcopy_attrs (full_newpath, &P->st));
      break;
    case pot_remove:
      PO_ASSERT (delete_file (full_newpath));
      break;
    case pot_copy_attrs:
      PO_ASSERT (lcopy_attrs (full_newpath, &P->st));
      break;
  }
}

void pending_operations_apply (void) {
  struct pending_operation *P;
  for (P = pol_head; P != NULL; P = P->next) {
    pending_operation_apply (P);
  }
  if (po_use_mark) {
    dyn_release (po_mrk);
  } else {
    pending_operations_clear ();
  }
  pol_head = pol_tail = NULL;
}

static void replace_char (char *s, char old, char new) {
  while (*s) {
    if (*s == old) {
      *s = new;
    }
    s++;
  }
}

static void compute_temporary_filename (int transaction_id, int *transaction_file_no, const char *const name, char out[PATH_MAX]) {
  const int MAX_PREFIX_LENGTH = 64;
  char tmp_name[PATH_MAX];
  strcpy (tmp_name, name);
  replace_char (tmp_name, '/', '$');
  const char *p = tmp_name, *q = p + strlen (p);
  if (q - p >= MAX_PREFIX_LENGTH) { p = q - MAX_PREFIX_LENGTH; }
  assert (snprintf (out, PATH_MAX, "%s/%s.%d.%d", po_olddir, p, transaction_id, (*transaction_file_no)++) < PATH_MAX);
}

void pending_operation_copyfile (int transaction_id, int *transaction_file_no, const char *const name, void *data, int data_size, struct stat *S, struct static_pending_operation *P, dyn_mark_t release_mark) {
  enum pending_operation_tp type;
  char *oldpath;
  if (S_ISLNK (S->st_mode)) {
    type = pot_symlink;
    oldpath = (char *) data;
  } else if (S_ISDIR (S->st_mode)) {
    type = pot_mkdir;
    oldpath = NULL;
  } else {
    char full_tmp_filename[PATH_MAX];
    compute_temporary_filename (transaction_id, transaction_file_no, name, full_tmp_filename);
    vkprintf (4, "full_tmp_filename = %s\n", full_tmp_filename);
    int fd = open (full_tmp_filename, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
    if (fd < 0) {
      kprintf ("fail to open file '%s', flags: O_CREAT | O_TRUNC | O_WRONLY | O_EXCL. %m\n", full_tmp_filename);
      exit (1);
    }
    assert (fd >= 0);
    assert (data_size == write (fd, data, data_size));
    assert (!close (fd));
    type = pot_rename;
    oldpath = full_tmp_filename + po_olddir_length + 1;
  }
  if (P) {
    pending_operation_fill (P, type, oldpath, name, S);
  } else {
    if (release_mark) {
      dyn_release (release_mark);
    }
    pending_operation_push (pending_operation_create (type, oldpath, name, S));
  }
}
