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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include "crc32.h"
#include "filesys-data.h"
#include "kdb-filesys-binlog.h"
#include "kdb-data-common.h"
#include "server-functions.h"

#define FILESYS_INDEX_MAGIC 0xe942ecd


#define MAX_FILENAME_LENGTH 16383
#define MAX_WRITE_SIZE (1 << 19)

#define INODE_HASH_SIZE (1 << 20)

extern int now;

/************************** LRU load *******************************************/
static int idx_fd, newidx_fd;

long long index_size;
long long max_loaded_index_data, max_allocated_data;
long long tot_loaded_index_data, tot_allocated_data;
int tot_loaded_metafiles;
struct filesys_inode *lru_head, *lru_tail;
int tot_inodes, tot_directory_nodes;

static void del_use (struct filesys_inode *I) {
  if (I->prev == NULL) {
    assert (lru_head == I);
    if (I->next == NULL) {
      lru_head = lru_tail = NULL;
    } else {
      lru_head = lru_head->next;
      lru_head->prev = NULL;
    }
  } else if (I->next == NULL) {
    assert (lru_tail == I);
    lru_tail = lru_tail->prev;
    lru_tail->next = NULL;
  } else {
    I->prev->next = I->next;
    I->next->prev = I->prev;
  }
  I->prev = I->next = NULL;
}

static void filesys_inode_unload (struct filesys_inode *I) {
  if (I->index_data == NULL) { return; }
  free (I->index_data);
  I->index_data = NULL;
  tot_loaded_index_data -= I->index_filesize;
  tot_loaded_metafiles--;
  del_use (I);
}

static void add_use (struct filesys_inode *I) {
  if (lru_tail == NULL) {
    lru_head = lru_tail = I;
    I->prev = I->next = NULL;
  } else {
    lru_tail->next = I;
    I->prev = lru_tail;
    I->next = NULL;
    lru_tail = I;
  }
}

static void filesys_inode_load (struct filesys_inode *I) {
  if (I->index_offset < 0) {
    return;
  }

  if (I->index_data != NULL) {
    del_use (I);
    add_use (I);
    return;
  }

  I->index_data = malloc (I->index_filesize);
  assert (lseek (idx_fd, I->index_offset, SEEK_SET) >= 0);
  long r = read (idx_fd, I->index_data, I->index_filesize);
  assert (r == I->index_filesize);
  tot_loaded_index_data += I->index_filesize;
  tot_loaded_metafiles++;
  add_use (I);
  while (tot_loaded_metafiles > 1 && tot_loaded_index_data > max_loaded_index_data) {
    filesys_inode_unload (lru_head);
  }
}

void set_memory_limit (long max_memory) {
  max_loaded_index_data = max_memory >> 1;
  max_allocated_data = max_memory >> 1;
}
/************************** tree operations ************************************/
int alloc_tree_nodes, free_tree_nodes;

static void rec_dump_tree (filesys_tree_t *T, int depth) {
  if (T != NULL) {
    fprintf (stderr, "[%d] [%u, %u)\n", depth, T->block_offset, T->block_offset + T->block_length);
    rec_dump_tree (T->left, depth + 1);
    rec_dump_tree (T->right, depth + 1);
  }
}

void filesys_dump_tree (filesys_tree_t *T) {
  fprintf (stderr, "================= Tree =======================\n");
  rec_dump_tree (T, 0);
  fprintf (stderr, "==============================================\n");
}

static filesys_tree_t *new_tree_node (unsigned int block_offset, int y, unsigned int block_length, char *data) {
  filesys_tree_t *P;
  P = zmalloc (sizeof (filesys_tree_t));
  assert (P);
  alloc_tree_nodes++;
  P->left = P->right = 0;
  P->block_offset = block_offset;
  P->y = y;
  P->data = data;
  P->block_length = block_length;
  return P;
}

static filesys_tree_t *tree_lowerbound (filesys_tree_t *T, unsigned int block_offset) {
  filesys_tree_t *R = NULL;
  while (T) {
    if (block_offset < T->block_offset) {
      R = T;
      T = T->left;
    } else if (block_offset > T->block_offset) {
      T = T->right;
    } else {
      return T;
    }
  }
  return R;
}

/* swap right and left and multiply block_offset by -1 */
static filesys_tree_t *tree_upperbound (filesys_tree_t *T, unsigned int block_offset) {
  filesys_tree_t *R = NULL;
  while (T) {
    if (block_offset > T->block_offset) {
      R = T;
      T = T->right;
    } else if (block_offset < T->block_offset) {
      T = T->left;
    } else {
      return T;
    }
  }
  return R;
}

static void tree_split (filesys_tree_t **L, filesys_tree_t **R, filesys_tree_t *T, unsigned int block_offset) {
  if (!T) {
    *L = *R = NULL;
    return;
  }
  if (block_offset < T->block_offset) {
    *R = T;
    tree_split (L, &T->left, T->left, block_offset);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, block_offset);
  }
}

static filesys_tree_t *tree_insert (filesys_tree_t *T, unsigned int block_offset, int y, unsigned int block_length, char *data) {
  filesys_tree_t *P;
  if (!T) {
    P = new_tree_node (block_offset, y, block_length, data);
    return P;
  }
  if (T->y >= y) {
    if (block_offset < T->block_offset) {
      T->left = tree_insert (T->left, block_offset, y, block_length, data);
    } else {
      T->right = tree_insert (T->right, block_offset, y, block_length, data);
    }
    return T;
  }
  P = new_tree_node (block_offset, y, block_length, data);
  tree_split (&P->left, &P->right, T, block_offset);
  return P;
}

static void free_tree_node (filesys_tree_t *T) {
  alloc_tree_nodes--;
  if (T->data) {
    tot_allocated_data -= T->block_length;
    free (T->data);
  }
  zfree (T, sizeof (*T));
  free_tree_nodes++;
}

static filesys_tree_t *tree_delete (filesys_tree_t *T, unsigned int block_offset) {
  filesys_tree_t *Root = T, **U = &Root, *L, *R;
  while (block_offset != T->block_offset) {
    U = (block_offset < T->block_offset) ? &T->left : &T->right;
    T = *U;
    if (!T) { return Root; }
  }

  L = T->left;
  R = T->right;
  free_tree_node (T);
  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }
  *U = L ? L : R;
  return Root;
}

static void free_tree (filesys_tree_t *T) {
  if (T != NULL) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}

static void free_inode (struct filesys_inode *I) {
  if (I->index_data) {
    filesys_inode_unload (I);
  }
  free_tree (I->updates);
  I->updates = NULL;
  tot_inodes--;
  zfree (I, sizeof (struct filesys_inode));
}

/*
unsigned int filesys_get_filesize (const struct filesys_inode *I) {
  unsigned int r = I->filesize;
  filesys_tree_t *T = I->updates;
  if (T != NULL) {
    while (T->right != NULL) { T = T->right; }
    unsigned int w = T->block_offset + T->block_length;
    if (r < w) { r = w; }
  }
  return r;
}
*/

static int filesys_ftruncate (struct filesys_inode *I, unsigned int size) {
  if (I->index_filesize > size) {
    if (I->index_data) {
      I->index_data = realloc (I->index_data, size);
      tot_loaded_index_data -= I->index_filesize - size;
    }
    I->index_filesize = size;
  }

  filesys_tree_t *T;
  while ((T = tree_lowerbound (I->updates, size))) {
    I->updates = tree_delete (I->updates, T->block_offset);
  }

  if (size > 0) {
    T = tree_upperbound (I->updates, size - 1);
    if (T) {
      unsigned int to = T->block_offset, tr = to + T->block_length;
      if (tr > size) {
        T->block_length -= tr - size;
        T->data = realloc (T->data, T->block_length);
        tot_allocated_data -= tr - size;
      }
    }
  }
  I->filesize = size;
  return 0;
}

/*
static int filesys_inode_truncate (inode_id_t inode, unsigned int size) {
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EBADF; }
  return filesys_truncate (I, size);
}
*/

static void filesys_write (struct filesys_inode *I, unsigned int offset, unsigned int length, const char *data) {
  if (verbosity >= 3) {
    fprintf (stderr, "Before write: I = %p\n", I);
    filesys_dump_tree (I->updates);
  }
  I->modification_time = now;
  filesys_tree_t *T = tree_lowerbound (I->updates, offset);
  const unsigned int right = offset + length;
  if (I->filesize < right) {
    I->filesize = right;
  }
  while (T) {
    const unsigned int to = T->block_offset;
    if (to >= right) {
      break;
    }
    const unsigned int tr = to + T->block_length;
    if (tr <= right) {
      /* old block completely contained in new block,
         and should be removed                        */
      I->updates = tree_delete (I->updates, to);
      T = tree_lowerbound (I->updates, offset);
    } else {
      assert (tr > right);
      const unsigned int tail_length = tr - right;

      vkprintf (3, "tr = %d, right = %d, tail_length = %d\n", tr, right, tail_length);

      char *d = malloc (tail_length);
      tot_allocated_data += tail_length;
      memcpy (d, &T->data[right - to], tail_length);
      I->updates = tree_delete (I->updates, to);
      I->updates = tree_insert (I->updates, right, lrand48 (), tail_length, d);
      break;
    }
  }

  if (offset > 0) {
    T = tree_upperbound (I->updates, offset - 1);
    if (T != NULL) {
      unsigned int to = T->block_offset, tr = to + T->block_length;
      assert (to < offset);
      if (tr > offset) {
        if (right < tr) {
          const unsigned right_part_cutted_block_length = tr - right;
          char *d = malloc (right_part_cutted_block_length);
          tot_allocated_data += right_part_cutted_block_length;
          memcpy (d, &T->data[right - to], right_part_cutted_block_length);
          I->updates = tree_insert (I->updates, right, lrand48 (), right_part_cutted_block_length, d);
        }
        T->block_length -= tr - offset;
        tot_allocated_data -= tr - offset;
        T->data = realloc (T->data, T->block_length);
      }
    }
  }

  char *d = malloc (length);
  tot_allocated_data += length;
  memcpy (d, data, length);
  I->updates = tree_insert (I->updates, offset, lrand48 (), length, d);

  if (verbosity >= 3) {
    fprintf (stderr, "After write\n");
    filesys_dump_tree (I->updates);
  }
}

static unsigned int copy_updates (struct filesys_inode *I, unsigned int offset, unsigned int length, char *data, unsigned int *blocksize) {
  const unsigned int right = offset + length;
  unsigned int bytes = 0;
  *blocksize = 0;

  if (verbosity >= 3) {
    filesys_dump_tree (I->updates);
  }

  if (offset > 0) {
    filesys_tree_t *T = tree_upperbound (I->updates, offset - 1);
    if (verbosity >= 3) {
      fprintf (stderr, "copy_updates: T = %p\n", T);
      fwrite (T->data, 1, T->block_length, stderr);
    }
    if (T != NULL) {
      unsigned int to = T->block_offset, tr = to + T->block_length;

      vkprintf (3, "copy_updates: tr = %d, to = %d\n", to, tr);

      *blocksize = tr - offset;
      assert (to < offset);
      if (tr > offset) {
        unsigned int moved_bytes = tr - offset;
        if (moved_bytes > length) {
          moved_bytes = length;
        }

        vkprintf (3, "copy_updates: moved_bytes = %d, start pos %d\n", moved_bytes, offset - T->block_offset);

        memcpy (data, &T->data[offset - T->block_offset], moved_bytes);
        bytes += moved_bytes;
      }
    }
  }
  unsigned int l = offset;
  while (l < right) {
    filesys_tree_t *T = tree_lowerbound (I->updates, l);
    if (T == NULL) {
      break;
    }
    l = T->block_offset;
    if (l >= right) {
      break;
    }
    *blocksize = (l + T->block_length) - offset;
    unsigned int moved_bytes = length - (l - offset);
    if (moved_bytes > T->block_length) {
      moved_bytes = T->block_length;
    }

    vkprintf (3, "copy_updates: moved_bytes = %d, dest pos %d\n", moved_bytes, l - offset);

    bytes += moved_bytes;
    memcpy (&data[l - offset], &T->data[0], moved_bytes);
    l += T->block_length;
  }
  assert (bytes <= length);

  vkprintf (3, "copy_updates: returns %d\n", bytes);

  return bytes;
}

static int filesys_read (struct filesys_inode *I, unsigned int offset, unsigned int length, char *data) {
  unsigned int blocksize;
  memset (data, 0, length);
  if (copy_updates (I, offset, length, data, &blocksize) == length) {
    return length;
  }

  vkprintf (3, "filesys_read: before memset\n");

  if (offset >= I->index_filesize) {
    /* avoid loading metafile, if we really doesn't need it */
    return blocksize;
  }
  filesys_inode_load (I);
  if (I->index_data) {
    unsigned int moved_bytes = I->index_filesize - offset;
    if (moved_bytes > length) {
      moved_bytes = length;
    }
    memcpy (data, &I->index_data[offset], moved_bytes);
    if (blocksize < moved_bytes) {
      blocksize = moved_bytes;
    }
  }
  unsigned int tmp;
  copy_updates (I, offset, length, data, &tmp);
  return blocksize;
}

static struct filesys_inode *H[INODE_HASH_SIZE];

struct filesys_inode *get_inode_f (inode_id_t inode, int force) {
  if (inode < 0) {
    return NULL;
  }
  unsigned h = ((unsigned) inode) & (INODE_HASH_SIZE - 1);
  struct filesys_inode **p = H + h, *V;
  while (*p) {
    V = *p;
    if (V->inode == inode) {
      *p = V->hnext;
      int keep = 1;
      if (force < 0) {
        if (--(V->reference_count) <= 0) {
          keep = 0;
        }
      }
      if (keep) {
        V->hnext = H[h];
        H[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force <= 0) {
    return NULL;
  }
  V = zmalloc0 (sizeof (struct filesys_inode));
  V->inode = inode;
  V->hnext = H[h];
  //V->reference_count = 1;
  H[h] = V;
  tot_inodes++;
  return V;
}


static struct filesys_directory_node *Root;
static inode_id_t cur_inode = 0;

static void rec_dump_all_files (struct filesys_directory_node *D, int depth) {
  int i;
  for (i = 0; i < 2 * depth; i++) {
    fprintf (stderr, " ");
  }
  fprintf (stderr, "%s\n", D->name);
  if (D->inode >= 0) { return; }
  struct filesys_directory_node *T = D->head;
  while (T != NULL) {
    rec_dump_all_files (T, depth + 1);
    T = T->next;
  }
}

void dump_all_files (void) {
  if (Root == NULL) {
    return;
  }
  rec_dump_all_files (Root, 0);
}

static void filesys_update_modification_time (struct filesys_directory_node *T) {
  while (T != NULL) {
    T->modification_time = now;
    T = T->parent;
  }
}

struct filesys_directory_node **sd_buf;
int sd_buf_size;

static int cmp_names (const void *a, const void *b) {
  return strcmp ((*((const struct filesys_directory_node **) a))->name,
                 (*((const struct filesys_directory_node **) b))->name);
}


static void filesys_sort_directory_tree (struct filesys_directory_node *T) {
  if (T->inode >= 0) {
    return;
  }
  struct filesys_directory_node *p;
  int k = 0;
  for (p = T->head; p != NULL; p = p->next) {
    k++;
  }
  if (k <= 1) {
    return;
  }
  if (sd_buf_size < k) {
    sd_buf = realloc (sd_buf, sizeof (sd_buf[0]) * k);
    assert (sd_buf != NULL);
    sd_buf_size = k;
  }
  k = 0;
  for (p = T->head; p != NULL; p = p->next) {
    sd_buf[k++] = p;
  }
  qsort (sd_buf, k, sizeof (sd_buf[0]), cmp_names);
  T->head = sd_buf[0];
  int i;
  for (i = 1; i < k; i++) {
    sd_buf[i-1]->next = sd_buf[i];
  }
  sd_buf[k-1]->next = NULL;
  for (p = T->head; p != NULL; p = p->next) {
    filesys_sort_directory_tree (p);
  }
}

int do_check_perm (struct filesys_directory_node *D, int mode) {
  unsigned short uid = geteuid ();
  if (!uid) {
    return 0;
  }
  int o = (D->mode >> ((D->uid == uid) ? 6 : (D->gid == getegid ()) ? 3 : 0)) & 7;
  if ((o & mode) != mode) {
    errno = EACCES;
    return -EACCES;
  }
  return 0;
}

struct filesys_directory_node *filesys_lookup_file (const char *name, enum filesys_lookup_file_type force) {

  vkprintf (3, "filesys_lookup_file (%s, %d)\n", name, force);

  if (*name != '/') {
    /* isn't absolute path */
    return NULL;
  }
  name++;
  if (Root == NULL) {
    tot_directory_nodes++;
    Root = zmalloc0 (sizeof (struct filesys_directory_node));
    Root->inode = -1;
    Root->mode = 0777 | S_IFDIR;
    Root->name = zmalloc0 (1);
    Root->modification_time = now;
  }

  if (!*name) {
    return Root;
  }

  struct filesys_directory_node *T = Root;
  while (1) {
    const char *p = strchr (name, '/');
    int l = (p == NULL) ? strlen (name) : (p - name);
    if (l > MAX_FILENAME_LENGTH) {
      return NULL;
    }
    assert (T != NULL);
    struct filesys_directory_node **w = &T->head;
    int found = 0;
    while (*w != NULL) {
      struct filesys_directory_node *q = *w;
      if (!memcmp (q->name, name, l) && !q->name[l]) {
        name += l;
        if (!(*name)) {
          switch (force) {
            case lf_rmdir:
              if (q->inode >= 0 || q->head != NULL || do_check_perm (q->parent, 2)) {
                /* don't rmdir not empty directory */
                return NULL;
              }
              *w = q->next;
              filesys_update_modification_time (T);
              return q;
            case lf_unlink:
              if (q->inode < 0 || do_check_perm (q->parent, 2)) {
                return NULL;
              }
              *w = q->next;
              filesys_update_modification_time (T);
              return q;
            case lf_find:
              *w = q->next;
              q->next = T->head;
              T->head = q;
              return q;
            case lf_creat:
              return NULL;
          }
        }

        *w = q->next;
        q->next = T->head;
        T->head = q;

        if (q->inode >= 0) {
          /* isn't directory */
          return NULL;
        }
        name++; /* skip '/' */
        T = q;
        found = 1;
        break;
      }
      w = &q->next;
    }
    if (!found) {
      if (force > 0 && !name[l] && T->inode < 0 && !do_check_perm (T, 2)) {
        tot_directory_nodes++;
        struct filesys_directory_node *D = zmalloc (sizeof (struct filesys_directory_node));
        D->parent = T;
        D->name = zmalloc (l + 1);
        strcpy (D->name, name);
        D->head = NULL;
        D->next = T->head;
        D->inode = -1;
        T->head = D;
        filesys_update_modification_time (D);
        return D;
      }
      return NULL;
    }
  }
}

static void filesys_directory_node_free (struct filesys_directory_node *D) {
  tot_directory_nodes--;
  if (D->name != NULL) {
    zfree (D->name, strlen (D->name) + 1);
  }
  zfree (D, sizeof (struct filesys_directory_node));
}

static int filesys_rename (const char *src_filename, const char *dest_filename) {
  vkprintf (2, "filesys_rename (%s, %s)\n", src_filename, dest_filename);

  struct filesys_directory_node *D = filesys_lookup_file (src_filename, lf_find);
  if (D == NULL) { return -ENOENT; }
  if (D->inode < 0) { return -ENOENT; }
  struct filesys_directory_node *E = filesys_lookup_file (dest_filename, lf_find);
  if (E != NULL && E->inode == D->inode) {
    /* If  oldpath  and  newpath are existing hard links referring to the same
       file, then rename() does nothing, and returns a success status. */
    return 0;
  }

  inode_id_t inode = D->inode;
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EFAULT; }
  if (E != NULL) {
    E = filesys_lookup_file (dest_filename, lf_unlink);
    assert (E != NULL);
    filesys_directory_node_free (E);
  }

  E = filesys_lookup_file (dest_filename, lf_creat);
  if (E == NULL) { return -ENOENT; }
  I->reference_count++;
  E->inode = inode;
  E->mode = D->mode;
  E->uid = D->uid;
  E->gid = D->gid;
  D = filesys_lookup_file (src_filename, lf_unlink);
  assert (D != NULL);
  filesys_directory_node_free (D);
  return 0;
}

static int filesys_link (const char *src_filename, const char *dest_filename) {
  vkprintf (2, "filesys_link (%s, %s)\n", src_filename, dest_filename);

  struct filesys_directory_node *E = filesys_lookup_file (dest_filename, lf_find);
  if (E != NULL) { return -EEXIST; }
  struct filesys_directory_node *D = filesys_lookup_file (src_filename, lf_find);
  if (D == NULL) { return -ENOENT; }
  if (D->inode < 0) { return -ENOENT; }
  inode_id_t inode = D->inode;
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EFAULT; }
  E = filesys_lookup_file (dest_filename, lf_creat);
  if (E == NULL) { return -ENOENT; }
  I->reference_count++;
  E->inode = inode;
  E->mode = D->mode;
  E->uid = D->uid;
  E->gid = D->gid;
  return 0;
}

static int filesys_mkdir (const char *path, int mode, unsigned short uid, unsigned short gid) {
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_creat);
  if (D == NULL) {
    return -ENOENT;
  }
  D->mode = mode | S_IFDIR;
  D->uid = uid;
  D->gid = gid;
  return (D != NULL) ? 0 : -ENOENT;
}


static int filesys_unlink (const char *path) {
  vkprintf (2, "filesys_unlink (%s)\n", path);
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_unlink);
  if (D == NULL) { return -ENOENT; }
  struct filesys_inode *I = get_inode_f (D->inode, -1);
  if (I == NULL) { return -EFAULT; }
  if (I->reference_count <= 0) {
    free_inode (I);
  }
  filesys_directory_node_free (D);
  return 0;
}

static int filesys_rmdir (const char *path) {
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_rmdir);
  if (D == NULL) {
    return -ENOENT;
  }
  filesys_directory_node_free (D);
  return 0;
}

static int filesys_chmod (const char *path, int mode) {
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) {
    return -ENOENT;
  }
  if (geteuid () && geteuid () != D->uid) {
    return -EACCES;
  }
  D->mode &= ~0777;
  D->mode |= mode;
  return 0;
}

static int filesys_chown (const char *path, unsigned short uid, unsigned short gid) {
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) {
    return -ENOENT;
  }
  if (geteuid () && geteuid () != D->uid) {
    return -EACCES;
  }
  D->uid = uid;
  D->gid = gid;
  return 0;
}

static int filesys_set_mtime (const char *path, int modification_time) {
  struct filesys_directory_node *D = filesys_lookup_file (path, lf_find);
  if (D == NULL) {
    return -ENOENT;
  }
  D->modification_time = modification_time;
  return 0;
}

static inode_id_t filesys_create (const char *path, int mode, unsigned short uid, unsigned short gid) {
  struct filesys_directory_node *F = filesys_lookup_file (path, lf_find);
  if (F != NULL) {
    return -3;
  }
  F = filesys_lookup_file (path, lf_creat);
  if (F == NULL) {
    return -2;
  }
  F->mode = mode | S_IFREG;
  F->uid = uid;
  F->gid = gid;
  F->inode = cur_inode++;

  vkprintf (2, "creat: %s, mode = %o, uid = %d, gid = %d, inode = %lld\n", path, mode, uid, gid, F->inode);

  struct filesys_inode *I = get_inode_f (F->inode, 1);
  I->reference_count = 1;
  I->modification_time = now;
  I->filesize = I->index_filesize = 0;
  I->index_offset = -1;
  return F->inode;
}

static int filesys_symlink (const char *oldpath, const char *newpath) {
  inode_id_t inode = filesys_create (newpath, 0777 | S_IFLNK, getuid (), getgid ());
  if (inode < 0) {
    return -ENOENT;
  }
  struct filesys_inode *I = get_inode_f (inode, 0);
  filesys_write (I, 0, strlen (oldpath), oldpath);
  return 0;
}


int do_symlink (const char *src_filename, const char *dest_filename) {
  int l_src = strlen (src_filename);
  int l_dst = strlen (dest_filename);
  if (l_src >= 65536 || l_dst >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_rename, data) + l_src + l_dst + 2;
  struct lev_filesys_rename *E = alloc_log_event (LEV_FILESYS_SYMLINK, sz, 0);
  E->dst_filename_size = l_dst;
  E->src_filename_size = l_src;
  strcpy (E->data, src_filename);
  strcpy (E->data + l_src + 1, dest_filename);
  return filesys_symlink (src_filename, dest_filename);
}

int do_rename (const char *src_filename, const char *dest_filename) {
  int l_src = strlen (src_filename);
  int l_dst = strlen (dest_filename);
  if (l_src >= 65536 || l_dst >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_rename, data) + l_src + l_dst + 2;
  struct lev_filesys_rename *E = alloc_log_event (LEV_FILESYS_RENAME, sz, 0);
  E->dst_filename_size = l_dst;
  E->src_filename_size = l_src;
  strcpy (E->data, src_filename);
  strcpy (E->data + l_src + 1, dest_filename);
  return filesys_rename (src_filename, dest_filename);
}

int do_link (const char *src_filename, const char *dest_filename) {
  int l_src = strlen (src_filename);
  int l_dst = strlen (dest_filename);
  if (l_src >= 65536 || l_dst >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_rename, data) + l_src + l_dst + 2;
  struct lev_filesys_rename *E = alloc_log_event (LEV_FILESYS_LINK, sz, 0);
  E->dst_filename_size = l_dst;
  E->src_filename_size = l_src;
  strcpy (E->data, src_filename);
  strcpy (E->data + l_src + 1, dest_filename);
  return filesys_link (src_filename, dest_filename);
}

int do_chmod (const char *path, int mode) {
  int l = strlen (path);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_chmod, path) + l + 1;
  struct lev_filesys_chmod *E = alloc_log_event (LEV_FILESYS_CHMOD, sz, 0);
  E->path_size = l;
  E->mode = mode;
  strcpy (E->path, path);
  return filesys_chmod (path, mode);
}

int do_chown (const char *path, unsigned short uid, unsigned short gid) {
  int l = strlen (path);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_chown, path) + l + 1;
  struct lev_filesys_chown *E = alloc_log_event (LEV_FILESYS_CHOWN, sz, 0);
  E->path_size = l;
  E->uid = uid;
  E->gid = gid;
  strcpy (E->path, path);
  return filesys_chown (path, uid, gid);
}

int do_set_mtime (const char *path, int modification_time) {
  int l = strlen (path);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_chmod, path) + l + 1;
  struct lev_filesys_chmod *E = alloc_log_event (LEV_FILESYS_SET_MTIME, sz, 0);
  E->path_size = l;
  E->mode = modification_time;
  strcpy (E->path, path);
  return filesys_set_mtime (path, modification_time);
}


int do_mkdir (const char *dirname, int mode, unsigned short uid, unsigned short gid) {
  int l = strlen (dirname);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_mkdir, dirpath) + l + 1;
  struct lev_filesys_mkdir *E = alloc_log_event (LEV_FILESYS_MKDIR, sz, 0);
  E->dirpath_size = l;
  E->mode = mode;
  E->uid = uid;
  E->gid = uid;
  strcpy (E->dirpath, dirname);
  return filesys_mkdir (dirname, mode, uid, gid);
}

int do_rmdir (const char *dirname) {
  int l = strlen (dirname);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_rmdir, dirpath) + l + 1;
  struct lev_filesys_rmdir *E = alloc_log_event (LEV_FILESYS_RMDIR, sz, 0);
  E->dirpath_size = l;
  strcpy (E->dirpath, dirname);
  return filesys_rmdir (dirname);
}

int do_unlink (const char *filename) {
  int l = strlen (filename);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_rmdir, dirpath) + l + 1;
  struct lev_filesys_rmdir *E = alloc_log_event (LEV_FILESYS_UNLINK, sz, 0);
  E->dirpath_size = l;
  strcpy (E->dirpath, filename);
  return filesys_unlink (filename);
}

int do_creat (const char *path, int mode, unsigned short uid, unsigned short gid, inode_id_t *inode) {
  int l = strlen (path);
  if (l >= 65536) { return -ENAMETOOLONG; }
  int sz = offsetof (struct lev_filesys_create, filename) + l + 1;
  struct lev_filesys_create *E = alloc_log_event (LEV_FILESYS_CREATE, sz, 0);
  E->filename_size = l;
  E->mode = mode;
  E->uid = uid;
  E->gid = gid;
  strcpy (E->filename, path);
  *inode = filesys_create (path, mode, uid, gid);
  return *inode < 0 ? -EISDIR : 0;
}

int do_inode_read (unsigned int offset, unsigned int size, inode_id_t inode, char *output) {
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EBADF; }
  return filesys_read (I, offset, size, output);
}

int do_inode_write (unsigned int offset, unsigned int size, inode_id_t inode, const char *input) {
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EBADF; }

  unsigned int i, l;
  for (i = 0; i < size; i += l) {
    l = size - i;
    if (l > MAX_WRITE_SIZE) {
      l = MAX_WRITE_SIZE;
    }
    const int sz = offsetof (struct lev_filesys_write, data) + l;

    if (compute_uncommitted_log_bytes () > (1 << 18)) {
      flush_binlog_forced (1);
    }

    struct lev_filesys_write *E = alloc_log_event (LEV_FILESYS_WRITE, sz, 0);
    E->inode = inode;
    E->offset = offset + i;
    E->data_size = l;
    memcpy (E->data, input + i, l);
  }
  filesys_write (I, offset, size, input);
  return size;
}

int do_inode_truncate (inode_id_t inode, unsigned int size) {
  struct filesys_inode *I = get_inode_f (inode, 0);
  if (I == NULL) { return -EBADF; }
  struct lev_filesys_ftruncate *E = alloc_log_event (LEV_FILESYS_FTRUNCATE, sizeof (struct lev_filesys_ftruncate), 0);
  E->inode = inode;
  E->size = size;
  return filesys_ftruncate (I, size);
}

int do_read (unsigned int offset, unsigned int size, const char *filename, char *output) {
  struct filesys_directory_node *D = filesys_lookup_file (filename, lf_find);
  if (D == NULL) { return -1; }
  if (D->inode < 0) { return -2; }
  struct filesys_inode *I = get_inode_f (D->inode, 0);
  if (I == NULL) { return -3; }

  vkprintf (3, "do_read: inode = %lld\n", D->inode);

  return filesys_read (I, offset, size, output);
}

int do_write (unsigned int offset, unsigned int size, char *input, const char *filename) {
  struct filesys_directory_node *D = filesys_lookup_file (filename, lf_find);
  if (D == NULL) { return -1; }
  if (D->inode < 0) { return -2; }
  return do_inode_write (offset, size, D->inode, input);
}

int filesys_replay_logevent (struct lev_generic *E, int size);

int init_filesys_data (int schema) {
  replay_logevent = filesys_replay_logevent;

  memset (H, 0, sizeof (H));

  //Root = zmalloc0 (sizeof (struct filesys_directory_node));
  //Root->inode = -1;
  return 0;
}

long long volume_id = -1;

static int filesys_le_start (struct lev_start *E) {
  if (E->schema_id != FILESYS_SCHEMA_V1) {
    return -1;
  }
  long long l;
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  assert (E->extra_bytes == 8);
  memcpy (&l, E->str, 8);
  if (volume_id >= 0 && l != volume_id) {
    fprintf (stderr, "Binlog volume_id isn't matched.\n");
    exit (1);
  }
  volume_id = l;
  return 0;
}

int filesys_replay_logevent (struct lev_generic *E, int size) {
  int s;
  struct filesys_inode *I;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return filesys_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_FILESYS_MKDIR:
      s = offsetof (struct lev_filesys_mkdir, dirpath);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_mkdir *) E)->dirpath_size + 1;
      if (size < s) { return -2; }
      filesys_mkdir (((struct lev_filesys_mkdir *) E)->dirpath,
                     ((struct lev_filesys_mkdir *) E)->mode,
                     ((struct lev_filesys_mkdir *) E)->uid,
                     ((struct lev_filesys_mkdir *) E)->gid);
      return s;
    case LEV_FILESYS_RMDIR:
      s = offsetof (struct lev_filesys_rmdir, dirpath);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_rmdir *) E)->dirpath_size + 1;
      if (size < s) { return -2; }
      filesys_rmdir (((struct lev_filesys_rmdir *) E)->dirpath);
      return s;
    case LEV_FILESYS_CHMOD:
      s = offsetof (struct lev_filesys_chmod, path);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_chmod *) E)->path_size + 1;
      if (size < s) { return -2; }
      filesys_chmod (((struct lev_filesys_chmod *) E)->path, ((struct lev_filesys_chmod *) E)->mode);
      return s;
    case LEV_FILESYS_CHOWN:
      s = offsetof (struct lev_filesys_chown, path);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_chown *) E)->path_size + 1;
      if (size < s) { return -2; }
      filesys_chown (((struct lev_filesys_chown *) E)->path, ((struct lev_filesys_chown *) E)->uid, ((struct lev_filesys_chown *) E)->gid);
      return s;
    case LEV_FILESYS_SET_MTIME:
      s = offsetof (struct lev_filesys_chmod, path);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_chmod *) E)->path_size + 1;
      if (size < s) { return -2; }
      filesys_set_mtime (((struct lev_filesys_chmod *) E)->path, ((struct lev_filesys_chmod *) E)->mode);
      return s;
    case LEV_FILESYS_RENAME:
      s = offsetof (struct lev_filesys_rename, data);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_rename *) E)->src_filename_size + ((struct lev_filesys_rename *) E)->dst_filename_size + 2;
      if (size < s) { return -2; }
      filesys_rename (((struct lev_filesys_rename *) E)->data, ((struct lev_filesys_rename *) E)->data + ((struct lev_filesys_rename *) E)->src_filename_size + 1);
      return s;
    case LEV_FILESYS_LINK:
      s = offsetof (struct lev_filesys_rename, data);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_rename *) E)->src_filename_size + ((struct lev_filesys_rename *) E)->dst_filename_size + 2;
      if (size < s) { return -2; }
      filesys_link (((struct lev_filesys_rename *) E)->data, ((struct lev_filesys_rename *) E)->data + ((struct lev_filesys_rename *) E)->src_filename_size + 1);
      return s;
    case LEV_FILESYS_SYMLINK:
      s = offsetof (struct lev_filesys_rename, data);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_rename *) E)->src_filename_size + ((struct lev_filesys_rename *) E)->dst_filename_size + 2;
      if (size < s) { return -2; }
      filesys_symlink (((struct lev_filesys_rename *) E)->data, ((struct lev_filesys_rename *) E)->data + ((struct lev_filesys_rename *) E)->src_filename_size + 1);
      return s;
    case LEV_FILESYS_UNLINK:
      s = offsetof (struct lev_filesys_rmdir, dirpath);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_rmdir *) E)->dirpath_size + 1;
      if (size < s) { return -2; }
      filesys_unlink (((struct lev_filesys_rmdir *) E)->dirpath);
      return s;
    case LEV_FILESYS_WRITE:
      s = offsetof (struct lev_filesys_write, data);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_write *) E)->data_size;
      if (size < s) { return -2; }
      I = get_inode_f (((struct lev_filesys_write *) E)->inode, 0);
      if (I == NULL) { return -3; }
      filesys_write (I, ((struct lev_filesys_write *) E)->offset,
                        ((struct lev_filesys_write *) E)->data_size,
                        ((struct lev_filesys_write *) E)->data);
      return s;
    case LEV_FILESYS_FTRUNCATE:
      if (size < sizeof (struct lev_filesys_ftruncate)) { return -2; }
      I = get_inode_f (((struct lev_filesys_ftruncate *) E)->inode, 0);
      if (I == NULL) { return -3; }
      filesys_ftruncate (I, ((struct lev_filesys_ftruncate *) E)->size);
      return sizeof (struct lev_filesys_ftruncate);
    case LEV_FILESYS_CREATE:
      s = offsetof (struct lev_filesys_create, filename);
      if (size < s) { return -2; }
      s += ((struct lev_filesys_create *) E)->filename_size + 1;
      if (size < s) { return -2; }
      filesys_create (((struct lev_filesys_create *) E)->filename,
                      ((struct lev_filesys_create *) E)->mode,
                      ((struct lev_filesys_create *) E)->uid,
                      ((struct lev_filesys_create *) E)->gid);
      return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;
}

#pragma	pack(push,4)
typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long volume_id;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;

  inode_id_t cur_inode;
  int tot_inodes;
  long long tree_size;
  unsigned header_crc32; //should be always last field
} index_header;
#pragma	pack(pop)

/*
 *
 * GENERIC BUFFERED READ/WRITE
 *
 */

#define	BUFFSIZE	16777216

static char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff;
static long long bytes_read;

static void flushout (void) {
  int w, s;
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (newidx_fd, rptr, s);
    assert (w == s);
  }
  rptr = wptr = Buff;
}

static long long bytes_written;
static unsigned idx_crc32_complement;

static void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
  bytes_read = 0;
  bytes_written = 0;
  idx_crc32_complement = -1;
}

static int writeout (const void *D, size_t len) {
  bytes_written += len;
  idx_crc32_complement = crc32_partial (D, len, idx_crc32_complement);
  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  int r = read (idx_fd, wptr, Buff + BUFFSIZE - wptr);
  if (r < 0) {
    fprintf (stderr, "error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

static void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
}

void bread (void *b, size_t len) {
  void *p = readin (len);
  assert (p != NULL);
  memcpy (b, p, len);
  idx_crc32_complement = crc32_partial (b, len, idx_crc32_complement);
  readadv (len);
  bytes_read += len;
}

static struct filesys_directory_node *filesys_load_directory_tree (struct filesys_directory_node *parent) {
  inode_id_t inode;
  bread (&inode, sizeof (inode_id_t));
  assert (inode >= -2);
  if (inode == -2LL) {
    return NULL;
  }
  tot_directory_nodes++;
  struct filesys_directory_node *T = zmalloc0 (sizeof (struct filesys_directory_node));
  bread (&T->mode, sizeof (T->mode));
  bread (&T->uid, sizeof (T->uid));
  bread (&T->gid, sizeof (T->gid));
  bread (&T->modification_time, sizeof (T->modification_time));
  int l;
  bread (&l, sizeof (l));
  T->inode = inode;
  T->parent = parent;
  T->name = zmalloc (l + 1);
  T->head = NULL;
  struct filesys_directory_node *tail = NULL;
  bread (T->name, l);
  T->name[l] = 0;

  vkprintf (3, "load_index: %s\n", T->name);

  if (T->inode < 0) {
    struct filesys_directory_node *P;
    while ((P = filesys_load_directory_tree (T)) != NULL ) {
      if (T->head == NULL) {
        T->head = tail = P;
      } else {
        tail->next = P;
        tail = P;
      }
    }
  }
  return T;
}

int load_index_crc32_check (void) {
  const unsigned computed_crc32 = ~idx_crc32_complement;
  unsigned idx_crc32;
  bread (&idx_crc32, 4);
  idx_crc32_complement = -1;
  if (idx_crc32 != computed_crc32) {
    vkprintf (1, "crc32 read = %x, crc32 computed = %x\n", idx_crc32, computed_crc32);
    return -1;
  }
  return 0;
}

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    index_size = 0;
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }
  idx_fd = Index->fd;
  index_header header;
  clearin ();
  bread (&header, sizeof (index_header));
  if (header.magic != FILESYS_INDEX_MAGIC) {
    kprintf ("index file is not for filesys\n");
    return -1;
  }

  if (volume_id >= 0 && header.volume_id != volume_id) {
    kprintf ("Index volume_id isn't matched.\n");
    exit (1);
  }

  if (compute_crc32 (&header, offsetof (index_header, header_crc32)) != header.header_crc32) {
    kprintf ("Index header corrupted (CRC32 failed).\n");
    return -2;
  }

  volume_id = header.volume_id;
  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  cur_inode = header.cur_inode;

  vkprintf (2, "tree_size = %lld, tot_inodes = %d\n", header.tree_size, header.tot_inodes);

  const long long tree_offset = sizeof (header);
  const long long inodes_offset = tree_offset + header.tree_size + 4;

  const long long data_offset = inodes_offset + (long long) INODE_RECORD_SIZE * header.tot_inodes + 4;
  assert (tree_offset == bytes_read);
  idx_crc32_complement = -1;
  Root = filesys_load_directory_tree (NULL);

  if (load_index_crc32_check () < 0) {
    kprintf ("Index directory tree is corrupted (CRC32 failed).\n");
    return -2;
  }

  assert (inodes_offset == bytes_read);
  int i;
  assert (tot_inodes == 0);
  for (i = 0; i < header.tot_inodes; i++) {
    inode_id_t inode;
    bread (&inode, sizeof (inode));

    vkprintf (3, "reading %lld inode\n", inode);

    struct filesys_inode *I = get_inode_f (inode, 1);
    bread (&I->modification_time, sizeof (I->modification_time));
    bread (&I->index_filesize, sizeof (I->index_filesize));
    I->filesize = I->index_filesize;
    //bread (&I->attributes, sizeof (I->attributes));
    bread (&I->reference_count, sizeof (I->reference_count));
    bread (&I->index_offset, sizeof (I->index_offset));
  }
  vkprintf (3, "tot_inodes = %d, header.tot_inodes = %d\n", tot_inodes, header.tot_inodes);
  assert (tot_inodes == header.tot_inodes);
  assert (data_offset == bytes_read + 4);

  vkprintf (2, "Inodes table size = %lld (offset: %lld)\n", bytes_read + 4 - inodes_offset, inodes_offset);

  if (load_index_crc32_check () < 0) {
    kprintf ("Index inodes table is corrupted (CRC32 failed).\n");
    return -2;
  }

  assert (data_offset == bytes_read);

  index_size = Index->info->file_size;
  return 0;
}

static long long filesys_save_directory_tree (struct filesys_directory_node *T) {
  long long r = 0;
  int l = strlen (T->name);
  r += writeout (&T->inode, sizeof (inode_id_t));
  r += writeout (&T->mode, sizeof (T->mode));
  r += writeout (&T->uid, sizeof (T->uid));
  r += writeout (&T->gid, sizeof (T->gid));
  r += writeout (&T->modification_time, sizeof (T->modification_time));
  r += writeout (&l, sizeof (l));
  r += writeout (T->name, l);
  if (T->inode < 0) {
    struct filesys_directory_node *p;
    for (p = T->head; p != NULL; p = p->next) {
      r += filesys_save_directory_tree (p);
    }
    long long end = -2;
    r += writeout (&end, sizeof (end));
  }
  return r;
}

static long long save_metafiles_cur_offset;
#define SM_JUST_WRITTEN ((void *) -1)
void save_metafiles (struct filesys_directory_node *T) {
  if (T->inode < 0) {
    struct filesys_directory_node *p;
    for (p = T->head; p != NULL; p = p->next) {
      save_metafiles (p);
    }
  } else {
    struct filesys_inode *I = get_inode_f (T->inode, 0);
    assert (I != NULL);
    if (I->index_data == SM_JUST_WRITTEN) {
      return;
    }
    filesys_inode_load (I);
    const unsigned int sz = I->filesize;
    unsigned int k;
    for (k = 0; k < sz; k += BUFFSIZE) {
      unsigned int l = sz - k;
      if (l > BUFFSIZE) {
        l = BUFFSIZE;
      }
      filesys_read (I, k, l, Buff);
      int w = write (newidx_fd, Buff, l);
      if (w != l) {
        kprintf ("Syscall write failed. %m");
        exit (1);
      }
      idx_crc32_complement = crc32_partial (Buff, l, idx_crc32_complement);
    }
    I->index_filesize = sz;
    I->index_offset = save_metafiles_cur_offset;
    save_metafiles_cur_offset += sz;
    filesys_inode_unload (I);
    I->index_data = SM_JUST_WRITTEN;
  }
}

/* Index parts (every part are defended by crc32)
 * header
 * directory tree
 * inodes (attributes: mode, mtime, ...)
 * inodes (file bodies)
*/


static void vk_lseek (long long offset) {
  assert (lseek (newidx_fd, offset, SEEK_SET) == offset);
}

int save_index (int writing_binlog) {
  int i;
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  }

  vkprintf (1, "creating index %s at log position %lld\n", newidxname, log_cur_pos());

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.magic = FILESYS_INDEX_MAGIC;
  header.created_at = time (NULL);
  header.volume_id = volume_id;
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;

  header.cur_inode = cur_inode;
  header.tot_inodes = tot_inodes;

  const long long tree_offset = sizeof (header);
  vk_lseek (tree_offset);
  clearin ();
  header.tree_size = filesys_save_directory_tree (Root);
  const unsigned tree_crc32 = ~idx_crc32_complement;
  writeout (&tree_crc32, 4);
  flushout ();

  vkprintf (2, "tree_size = %lld, tot_inodes = %d\n", header.tree_size, header.tot_inodes);

  const long long inodes_offset = tree_offset + header.tree_size + 4;
  const long long data_offset = inodes_offset + (long long) INODE_RECORD_SIZE * tot_inodes + 4;

  vk_lseek (data_offset);
  clearin ();
  save_metafiles_cur_offset = data_offset;
  if (Root != NULL) {
    sd_buf = NULL;
    sd_buf_size = 0;
    filesys_sort_directory_tree (Root);
    if (sd_buf != NULL) {
      free (sd_buf);
      sd_buf_size = 0;
    }
    save_metafiles (Root);
  }
  const unsigned data_crc32 = ~idx_crc32_complement;
  writeout (&data_crc32, 4);
  flushout ();

  vk_lseek (inodes_offset);
  clearin ();
  /* second pass writing inodes headers */
  long long b = 0;
  int written_inodes = 0;
  for (i = 0; i < INODE_HASH_SIZE; i++) {
    struct filesys_inode *I;
    for (I = H[i]; I != NULL; I = I->hnext) {

      vkprintf (3, "writing %lld inode\n", I->inode);

      if (I->index_data == SM_JUST_WRITTEN) {
        I->index_data = NULL;
      }
      assert (I->index_data == NULL);
      b += writeout (&I->inode, sizeof (I->inode));
      b += writeout (&I->modification_time, sizeof (I->modification_time));
      b += writeout (&I->index_filesize, sizeof (I->index_filesize));
      //b += writeout (&I->attributes, sizeof (I->attributes));
      b += writeout (&I->reference_count, sizeof (I->reference_count));
      b += writeout (&I->index_offset, sizeof (I->index_offset));
      written_inodes++;
    }
  }
  assert (written_inodes == tot_inodes);
  assert (b == (long long) INODE_RECORD_SIZE * tot_inodes);
  assert (bytes_written == b);
  const unsigned inodes_crc32 = ~idx_crc32_complement;
  writeout (&inodes_crc32, 4);
  vkprintf (2, "Inodes table size = %lld, crc32 = %x (offset: %lld)\n", bytes_written, inodes_crc32, inodes_offset);
  flushout ();

  clearin ();
  vk_lseek (0);

  header.header_crc32 = compute_crc32 (&header, offsetof (index_header, header_crc32));
  writeout (&header, sizeof (header));
  flushout ();

  vkprintf (3, "writing binary data done\n");

  assert (!fsync (newidx_fd));
  assert (!close (newidx_fd));

  vkprintf (3, "writing index done\n");

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}
