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

    Copyright 2010-2013	Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/
#define _FILE_OFFSET_BITS	64

#include <time.h>
#include <math.h>
#include <stdlib.h>

#include "maccub.h"

// ---------------------  CHANGES   --------------------------

// changes x
// (long)(x) < 0 => 1 element -> -(long)(x)
// (long)(x) == 0 => 0 elements
// (long)(x) > 0 => x[0] = +-len
//                  +len -> array
//                  -len -> treap
void chg_add (changes *_x, int val) {
  assert (val > 0);

  changes x = *_x;

  int *t = 0;
#ifdef __x86_64__
  if ((long)x < 0) {
    if ((long)x == -val) {
      return;
    } else if ((-(long)x) == (val ^ 1)) {
      x = (int *)((long)-val);
    } else {
      int old = -(long)x;
      x = qmalloc  (sizeof (int) * 3);

      assert (x != NULL);

      x[0] = 2;
      if (val < old) {
        x[1] = old;
        x[2] = val;
      } else {
        x[1] = val;
        x[2] = old;
      }
    }
  } else
#endif
  if (x) {
    if (x[0] > 0) {
      int len = x[0], i = 1;
      while (i <= len && (x[i] & -2) > (val & -2)) {
        i++;
      }
      if (i <= len && x[i] == val) {
        return;
      } else if (i <= len && x[i] == (val ^ 1)) {
        x[i] = val;
        return;
      }
      int j = i;
      while (j <= len && x[j]) {
        j++;
      }

//      fprintf (stderr, "i = %d, j = %d\n", i, j);

      if (j > len) {
        if (len == 8) {
          trp_node_ptr root = trp_conv_from_array ((int *)x + 1, len);
          qfree (x, sizeof (int) * (len + 1));
          x = qmalloc (sizeof (treap));
          //x[0] = -len;

          ((treap *)(x))->size = -len;
          ((treap *)(x))->root = root;
        } else {
          t = qmalloc0 (sizeof (int) * (len * 2 + 1));

          assert (t != NULL);

          t[0] = len * 2;
          memcpy (t + 1, x + 1, sizeof (int) * len);
          qfree (x, sizeof (int) * (len + 1));
          len *= 2;
          x = t;
        }
      }

      if (j <= len) {
        while (j != i) {
          x[j] = x[j - 1];
          j--;
        }
        x[j] = val;
      }
    }

    if (x[0] < 0) {
      trp_add_or_set ((treap *)(x), val, my_rand());
    }

  } else {
#ifdef __x86_64__
    x = (changes)((long)-val);
#else
    x = qmalloc0 (sizeof (int) * 2);
    x[0] = 1;
    x[1] = val;
#endif
  }

  *_x = x;
}

void chg_del (changes *x, int val) {
  chg_add (x, val ^ 1);
}

int  chg_has (changes x, int val) {
  val *= 2;

#ifdef __x86_64__
  if ((long)(x) < 0) {
    if (((-(long)(x)) & -2) == val) {
      return (-(long)(x)) & 1 ? 1 : -1;
    }
    return 0;
  } else
#endif
  if (x) {
    if (x[0] > 0) {
      int f = 0, i, n = abs (x[0]);;
      for (i = 1; i <= n; i++) {
        if ((x[i] & -2) == val) {
          return x[i] & 1 ? 1 : -1;
        }
      }
      return f;
    } else {
      return trp_has (*(treap *)(x), val);
    }

  }
  return 0;
}

int chg_max_size (changes x) {
#ifdef __x86_64__
  if ((long)(x) < 0) {
    return 1;
  } else
#endif
  if (x) {
    if (x[0] < 0) {
      return -x[0];
    } else {
      return x[0];
    }
  }
  return 0;
}

void chg_free (changes *_x) {
  changes x = *_x;

#ifdef __x86_64__
  if ((long)(x) < 0) {
  } else
#endif
  if (x) {
    if (x[0] < 0) {
      trp_free (((treap *)x)->root);
      qfree (x, sizeof (treap));
    } else {
      qfree (x, sizeof (int) * (x[0] + 1));
    }
  }

  *_x = NULL;
}


void chg_upd (chg_iterator *it) {
  int t = it->stack_top;
  trp_node_ptr *stack_ptr = it->stack_ptr;
  int *stack_state = it->stack_state;

  int f = 1;
  while (f) {
    if (t < 0) {
      break;
    }
    switch (it->stack_state[t]) {
    case 0:
      stack_state[t]++;
      if (stack_ptr[t]->l != NULL) {
        stack_ptr[t + 1] = stack_ptr[t]->l;
        stack_state[t + 1] = 0;
        t++;
        break;
      }
    case 1:
      f = 0;
      break;
    case 2:
      stack_state[t]++;
      if (stack_ptr[t]->r != NULL) {
        stack_ptr[t + 1] = stack_ptr[t]->r;
        stack_state[t + 1] = 0;
        t++;
        break;
      }
    case 3:
      t--;
      break;
    default:
      assert (0);
      break;
    }
  }
  it->stack_top = t;
}

void chg_iter_init (chg_iterator *it, changes x) {
  it->x = x;

#ifdef __x86_64__
  if ((long)x < 0) {
    it->i = 0;
  } else
#endif
  if (x) {
    if (x[0] > 0) {
      it->i = 0;
    } else {
      it->i = -1;
      it->stack_top = 0;
      it->stack_ptr[0] = ((treap *)(x))->root;
      it->stack_state[0] = 0;

      chg_upd (it);
    }
  }
}

void chg_iter_next (chg_iterator *it) {
  if (it->x == NULL) {
    return;
  } else if (it->i == -1) {
    it->stack_state[it->stack_top]++;
    chg_upd (it);
  } else {
    // bugs, bugs, bugs...
    it->i++;
  }

}

int chg_iter_val (chg_iterator *it) {
  if (it->x == NULL) {
    return 0;
  } else if (it->i == -1) {
    if (it->stack_top >= 0) {
      return it->stack_ptr[it->stack_top]->x;
    } else {
      return 0;
    }
  } else
#ifdef __x86_64__
  if ((long)it->x < 0) {
    if (it->i == 0) {
      return -(long)it->x;
    } else {
      return 0;
    }
  } else
#endif
  if (it->x) { // TODO optimize
    if (it->i < it->x[0]) {
      return it->x[it->i + 1];
    } else {
      return 0;
    }
  }

  assert (0);
  return 0;
}



void blist_iter_init (blist_iterator *it, blist x, int tot_items, int len, int r);
void blist_iter_next (blist_iterator *it);
int  blist_iter_can_has (void);
int  blist_iter_has (blist_iterator *it, int val);
int  blist_iter_val (blist_iterator *it);
int  blist_max_size (blist x, int tot_items, int len);
//int  blist_encode_list (int *P, int len, int tot_items, unsigned char *res);


void golomb_iter_init (golomb_iterator *it, golomb x, int tot_items, int len, int r);
void golomb_iter_next (golomb_iterator *it);
int  golomb_iter_can_has (void);
int  golomb_iter_has (golomb_iterator *it, int val);
int  golomb_iter_val (golomb_iterator *it);
int  golomb_max_size (golomb ptr, int tot_items, int len);
//int  golomb_encode_list (int *P, int len, int tot_items, unsigned char *res);

void bcode_iter_init (bcode_iterator *it, bcode x, int tot_items, int len, int r);
void bcode_iter_next (bcode_iterator *it);
int  bcode_iter_can_has (void);
int  bcode_iter_has (bcode_iterator *it, int val);
int  bcode_iter_val (bcode_iterator *it);
int  bcode_max_size (bcode ptr, int tot_items, int len);
//int  bcode_encode_list (int *P, int len, int tot_items, unsigned char *res);

void iCode_iter_init (iCode_iterator *it, iCode x, int tot_items, int len, int r);
void iCode_iter_next (iCode_iterator *it);
int  iCode_iter_can_has (void);
int  iCode_iter_has (iCode_iterator *it, int val);
int  iCode_iter_val (iCode_iterator *it);
int  iCode_max_size (iCode ptr, int tot_items, int len);
//int  iCode_encode_list (int *P, int len, int tot_items, unsigned char *res);

// ---------------------  list   --------------------------
// list x
// (long)(x) > 0 => x[0] = +len
//                  +len -> array only


int blist_max_size (blist x, int tot_items, int len) {
  if (x == NULL) {
    return 0;
  }

  return x[0];
}

int blist_iter_val (blist_iterator *it) {
  return it->val;
}

void blist_iter_init (blist_iterator *it, blist x, int tot_items, int len, int nr) {
  it->x = x;

  int n;

  if (x == NULL) {
    n = 0;
  } else {
    n = it->x[0];
  }

  int l = 0, r = n + 1;

  while (l + 1 < r) {
    int c = (l + r) >> 1;
    if (it->x[c] <= nr) {
      r = c;
    } else {
      l = c;
    }
  }
  it->i = r - 1;
  blist_iter_next (it);
}

void blist_iter_next (blist_iterator *it) {
  if (it->x != NULL && it->i + 1 <= it->x[0]) {
    it->val = it->x[++it->i];
  } else {
    it->val = 0;
  }
}

int blist_iter_can_has (void) {
  return 1;
}

int blist_iter_has (blist_iterator *it, int val) {
  if (it->x == NULL) {
    return 0;
  }

  int n = it->x[0];

  if (n <= 8) {
    int i;
    for (i = 1; i <= n; i++) {
      if (it->x[i] == val) {
        return 1;
      }
    }
    return 0;
  } else {
    int l = 1, r = n + 1, c;
    while (l + 1 < r) {
      c = (l + r) / 2;
      if (it->x[c] >= val) {
        l = c;
      } else {
        r = c;
      }
    }
    return it->x[l] == val;
  }
}

int blist_encode_list (int *P, int len, int tot_items, unsigned char *res) {
  int r;
  memcpy (res, &len, r = sizeof (len));
  memcpy (res + sizeof (len), P, r += sizeof (*P) * len);
  return r;
}



static int golomb_crit[10] = {
29976418, 45406673, 60720539, 75997183, 91257138,
106508163, 121753849, 136996088, 152235971, 167474174 };
// log (2) = 5278688/7615537
// golomb_crit[k] = 2*5278688/(1-solve (X=0,1,X^(k+1)*(X+1)-1))+(7615537-5278688)

int compute_golomb_parameter (int N, int K) {
  if (K == 0) {
    return 1;
  }
  assert (K > 0 && K <= N);
  long long t = (long long) 2*5278688*N / K + (7615537-5278688);
  if (t <= 167474174) {
    int i;
    for (i = 1; i <= 10; i++) {
      if ((int) t <= golomb_crit[i-1]) {
        return i;
      }
    }
  }
  int u = t / (2*7615537);
  return u;
}



#define	cur_bit	(it->m < 0)
#define	load_bit()	{ it->m <<= 1; if (it->m == (-1 << 31)) { it->m = ((int) *(it->ptr)++ << 24) + (1 << 23); } }

int golomb_iter_val (golomb_iterator *it) {
  return it->a;
}

void golomb_iter_next (golomb_iterator *it) {
  if (it->len <= 0) {
    it->a = 0;
    return;
  }

  it->len--;

  int d = 0, i;
  while (cur_bit) {
    it->a -= it->M;
    load_bit();
  }
  load_bit();
  for (i = it->k; i > 1; i--) {
    d <<= 1;
    if (cur_bit) {
      d++;
    }
    load_bit();
  }
  if (d >= it->p) {
    d <<= 1;
    if (cur_bit) {
      d++;
    }
    load_bit();
    d -= it->p;
  }

  it->a -= d;
}

void golomb_iter_init (golomb_iterator *it, golomb x, int tot_items, int len, int r) {
  it->ptr = x;

  if (x == NULL) {
    it->a = 0;
    it->len = 0;
    return;
  }

  it->k = LEN_BITS;
  it->p = 1 << (LEN_BITS + 1);


  it->m = ((int) *(it->ptr)++ << 24) + (1 << 23);

  int tmp = 0;
  while (it->k >= 0) {
    if (cur_bit) { tmp += (1 << it->k); }
    load_bit ();
    it->k--;
  }

  it->len = tmp;

  assert (it->len >= 0);

  it->a = tot_items + it->len + 1;
  it->M = compute_golomb_parameter (tot_items + it->len, it->len);

  it->k = 0, it->p = 1;
  while (it->M >= it->p) {
    it->p <<= 1;
    it->k++;
  }
  it->p -= it->M;

  golomb_iter_next (it);

  while (it->a > r) {
    golomb_iter_next (it);
  }
}

#undef cur_bit
#undef load_bit


#define	cur_bit	(m < 0)
#define	load_bit()	{ m <<= 1; if (m == (-1 << 31)) { m = ((int) *ptr++ << 24) + (1 << 23); } }

int golomb_max_size (golomb ptr, int tot_items, int len) {
  if (ptr == NULL) {
    return 0;
  }

  int k = -1, p = 1;
  while (p < tot_items) {
    p += p;
    k++;
  }
  k = LEN_BITS;
  p = 1 << (LEN_BITS + 1);

  int m = ((int) *ptr++ << 24) + (1 << 23);

  int tmp = 0;
  while (k >= 0) {
    if (cur_bit) { tmp += (1 << k); }
    load_bit ();
    k--;
  }

  return tmp;
}


int _golomb_decode_list (golomb ptr, int tot_items, int bytes, int *P) {
  const char *end = ptr + bytes;

  int k = LEN_BITS, p = 1 << (LEN_BITS + 1);

  int m = ((int) *ptr++ << 24) + (1 << 23);

  int tmp = 0;
  while (k >= 0) {
    if (cur_bit) { tmp += (1 << k); }
    load_bit ();
    k--;
  }

  int len = tmp;
//  fprintf (stderr, "len = %d\n", len);

  //assert (len > 0 && bytes > 0);

  int a = tot_items + len + 1, M = compute_golomb_parameter (tot_items + len, len);
  k = 0, p = 1;
  while (M >= p) {
    p <<= 1;
    k++;
  }
  p -= M;

  while (len--) {
    int d = 0, i;
    while (cur_bit) {
      a -= M;
      load_bit();
    }
    load_bit();
    for (i = k; i > 1; i--) {
      d <<= 1;
      if (cur_bit) {
        d++;
      }
      load_bit();
    }
    if (d >= p) {
      d <<= 1;
      if (cur_bit) {
        d++;
      }
      load_bit();
      d -= p;
    }
    a -= d;
//    fprintf (stderr, "golomb decode: %d (delta=%d) %d\n", a, d, *P);
    assert (a == *P);
    P++;
  }
  if (m & (1 << 23)) { ptr--; }
  assert (ptr == end);

  return tmp;
#undef cur_bit
#undef load_bit
}


int golomb_encode_list (int *P, int len, int tot_items, unsigned char *res) {
  //assert (len > 0);

  int k = LEN_BITS, p = 1 << (LEN_BITS + 1);

  int m = 0x80;
  unsigned char *ptr = res;
  *ptr = 0;

  int tmp = len;
  while (k >= 0) {
    if (!m) { *++ptr = 0; m = 0x80; }
    if ((tmp >> k) & 1) { *ptr += m;}
    m >>= 1;
    k--;
  }

  int M = compute_golomb_parameter (tot_items + len, len);

  k = 31, p = 1;
  while (p <= M) {
    p += p;
    k--;
  }
  p -= M;

  int a = tot_items + len + 1, d;

  while (len > 0) {
    len--;
    d = a - *P;
    a -= d;
//    fprintf (stderr, "golomb encode: %d (delta = %d)\n", a, d-1);
    P++;
    //assert (d > 0);
    //d--;

// d = qM + r
// output: q ones, 1 zero
// if r < p:=2^k-M, output: r using k-1 digits
// if r >= 2^k-M, output: r + 2^k - M using k digits
    while (d >= M) {
      if (!m) { *++ptr = 0x80;  m = 0x40; }
      else { *ptr += m;  m >>= 1; }
      d -= M;
    }
    if (!m) { *++ptr = 0;  m = 0x40; }
    else { m >>= 1; }
    if (d < p) {
      d = ((4*d + 2) << k);
    } else {
      d = ((2*(d + p) + 1) << k);
    }
    while (d != (-1 << 31)) {
      if (!m) { *++ptr = 0; m = 0x80; }
      if (d < 0) { *ptr += m; }
      d <<= 1;
      m >>= 1;
    }
  }
  if (m != 0x80) { ptr++; }
  a = ptr - res;
/*
  if (verbosity > 1) {
    fprintf (stderr, "%d bytes output\n", a);
    for (d = 0; d < a; d++) {
      fprintf (stderr, "%02x", (unsigned char) res[d]);
    }
    fprintf (stderr, "\n");
  }
*/
  _golomb_decode_list ((golomb)res, tot_items, a, P - tmp);

  return a;
}

int golomb_iter_can_has (void) {
  return 0;
}

int golomb_iter_has (golomb_iterator *it, int val) {
  assert (0);

  return 0;
}

int bcode_iter_get_val (bcode_iterator *it, int i) {
  if (i >= it->len) {
    return 0;
  }
  int pred = it->k * i;
  const unsigned char *tmp = it->ptr + (pred >> 3);
  pred &= 7;

  if (pred + it->k <= 8) {
    return ( ( *tmp ) >> pred ) & ( (1 << it->k) - 1 );
  }

  int res = ( ( *tmp++ ) >> pred );
  pred = it->k - (8 - pred);

  while (pred > 8) {
    res = (res << 8) + *tmp++;
    pred -= 8;
  }

  res = (res << pred) + ( *tmp & ( (1 << pred) - 1) );
  return res;
}

void bcode_iter_init (bcode_iterator *it, bcode x, int tot_items, int len, int nr) {
  it->ptr = x;
  it->i = -1;

  it->k = 0;
  int p = 1;

  while (p <= tot_items) {
    p += p;
    it->k++;
  }

  if (it->k > 0) {
  //  assert (x != NULL);
    it->len = 8 * len / it->k;
  } else {
    assert (x == NULL);
    it->len = 0;
  }

  int n = it->len;
  int l = -1, r = n;

  while (l + 1 < r) {
    int c = (l + r) >> 1;
    if (bcode_iter_get_val (it, c) <= nr) {
      r = c;
    } else {
      l = c;
    }
  }
  it->i = r - 1;
  bcode_iter_next (it);
}

void bcode_iter_next (bcode_iterator *it) {
  if (it->ptr != NULL && it->i + 1 < it->len) {
    it->val = bcode_iter_get_val (it, ++it->i);
  } else {
    it->val = 0;
  }
}

int  bcode_iter_can_has (void) {
  return 1;
}

int  bcode_iter_has (bcode_iterator *it, int val) {
  if (it->ptr == NULL) {
    return 0;
  }

  int n = it->len;

  int l = 0, r = n, c;
  while (l + 1 < r) {
    c = (l + r) / 2;
    if (bcode_iter_get_val (it, c) >= val) {
      l = c;
    } else {
      r = c;
    }
  }

  return bcode_iter_get_val (it, l) == val;
}

int  bcode_iter_val (bcode_iterator *it) {
  return it->val;
}

int  bcode_max_size (bcode ptr, int tot_items, int len) {
  return len; // this is wrong but.. but it is OK
  /*
  int k = 0;
  int p = 1;

  while (p <= tot_items) {
    p += p;
    k++;
  }
  if (k > 0) {
    assert (x != NULL);
    return 8 * len / it->k;
  } else {
    assert (x == NULL);
    return 0;
  }
  */
}

int bcode_encode_list (int *P, int len, int tot_items, unsigned char *res) {
  //assert (len > 0);

  unsigned char *cur = res;

  int k = 0;
  int p = 1;

  while (p <= tot_items) {
    p += p;
    k++;
  }
  int left = 8, i;
  *cur = 0;

  for (i = 0; i < len; i++) {
    int c = P[i];
    int ck = k;

    while (ck > left) {
      ck -= left;
      *cur += (unsigned char)((c >> ck) << (8 - left));
      left = 8;
      *++cur = 0;
    }

    *cur += (unsigned char)((c & ((1 << ck) - 1))  << (8 - left));
    left -= ck;
  }


  bcode_iterator it;
  bcode_iter_init (&it, res, tot_items, (cur - res) + 1, tot_items);
/*  fprintf (stderr, "len = %d, tot_items = %d, k = %d\n", len, tot_items, k);
  for (i = 0; i + res <= cur; i++) {
    fprintf (stderr, "%x ", (int)res[i]);
  }
  fprintf (stderr, "\n");
  for (i = 0; i < len; i++) {
    fprintf (stderr, "%d ", P[i]);
  }
  fprintf (stderr, "\n");*/
  for (i = 0; i < len; i++) {
//    fprintf (stderr, "%d vs %d\n", bcode_iter_get_val (&it, i), P[i]);
    assert (bcode_iter_get_val (&it, i) == P[i]);
  }
  return (cur - res) + 1;
}

int iCode_iter_val (iCode_iterator *it) {
  return it->val;
}

int iCode_get_next_int (iCode_iterator *it, int r) {
  int k = 0, p = 1, res;

  if (r == 1) {
    return 0;
  }

  while (p < r) {
    p += p;
    k++;
  }

  if (it->pred + k <= 8) {
    res = ( ( *it->ptr ) >> it->pred ) & ( (1 << k) - 1 );
    it->pred += k;
    return res;
  }

  res = ( ( *it->ptr++ ) >> it->pred );
  it->pred = k - (8 - it->pred);

  while (it->pred > 8) {
    res = (res << 8) + *it->ptr++;
    it->pred -= 8;
  }

  res = (res << it->pred) + ( *it->ptr & ( (1 << it->pred) - 1) );
  return res;

}

void iCode_iter_next (iCode_iterator *it) {
  if (it->top == -1) {
    it->val = 0;
    return;
  }

  int *l = it->l, *r = it->r, *n = it->n, t = it->top, *st = it->st, *val = it->s_val;

  int f = 1, x, c;
  while (f) {
    switch (st[t]) {
    case 0:
      x = iCode_get_next_int (it, l[t] - r[t]), c = n[t] / 2;
      val[t] = l[t] - x;
      st[t] = 1;
      if (c) {
        l[t + 1] = l[t];
        r[t + 1] = val[t] - 1;
        n[t + 1] = c;
        st[++t] = 0;
      }
      break;

    case 1:
      it->val = val[t];
      st[t] = 2;
      f = 0;
      break;

    case 2:
      st[t] = 3;
      if (n[t] > 2) {
        l[t + 1] = val[t];
        r[t + 1] = r[t];
        n[t + 1] = n[t] - n[t] / 2 - 1;
        st[++t] = 0;
      }
      break;

    case 3:
      t--;
      break;

    default: // TODO optimize
      assert (0);
    }
    if (t < 0) {
      it->val = 0;
      f = 0;
    }
  }
  it->top = t;
}

void iCode_iter_init (iCode_iterator *it, iCode x, int tot_items, int len, int r) {
  it->ptr = x;

  if (x == NULL) {
    it->val = 0;
    return;
  }

  it->pred = 0;
  it->l[0] = tot_items;
  it->r[0] = 0;
  it->n[0] = iCode_get_next_int (it, 1 << MAX_LIST_LEN_EXP);
  it->st[0] = 0;
  it->top = 0;

  if (it->n[0] == 0) {
    it->top = -1;
  }

  iCode_iter_next (it);

  while (it->val > r) {
    iCode_iter_next (it);
  }
}

int iCode_max_size (iCode ptr, int tot_items, int len) {
  if (ptr == NULL) {
    return 0;
  }
  assert (0);
  return 0;
}


void iCode_write (int c, unsigned char **res, int *skip, int r) {
  int k = 0, p = 1;

  if (r == 1) {
    return;
  }

  while (p < r) {
    p += p;
    k++;
  }


  while (k > 8 - *skip) {
    k -= 8 - *skip;
    **res += (unsigned char)((c >> k) << *skip);
    *skip = 0;
    *++(*res) = 0;
  }

  **res += (unsigned char)((c & ((1 << k) - 1)) << (*skip));
  *skip += k;
}

void iCode_f (int *a, int n, int l, int r, unsigned char **res, int *skip) {
  if (n == 0) {
    return;
  }

  int c = n / 2;
  iCode_write (l - a[c], res, skip, l - r);

  iCode_f (a, c, l, a[c] - 1, res, skip);
  iCode_f (a + c + 1, n - c - 1, a[c], r, res, skip);
}

int iCode_encode_list (int *P, int len, int tot_items, unsigned char *res) {
//  assert (len > 0);

  unsigned char *st = res;

  int skip = 8;
  res--;
  iCode_write (len, &res, &skip, 1 << MAX_LIST_LEN_EXP);
  iCode_f (P, len, tot_items, 0, &res, &skip);


  int i;
  iCode_iterator it;
  iCode_iter_init (&it, st, tot_items, res - st + 1, tot_items);

/*  for (i = 0; i < res - st + 1; i++)
    fprintf (stderr, "%x%c", (int)st[i], " \n"[i==res-st]);*/

  for (i = 0; i < len; i++) {
//    fprintf (stderr, "%d vs %d\n", iCode_iter_val (&it), P[i]);
    assert (iCode_iter_val (&it) == P[i]);
    iCode_iter_next (&it);
  }
  assert (iCode_iter_val (&it) == 0);

  return res - st + 1;
}

int iCode_iter_can_has (void) {
  return 0;
}

int iCode_iter_has (iCode_iterator *it, int val) {
  assert (0);

  return 0;
}

int LIST_ (encode) (int *P, int len, int tot_items, unsigned char *res) {
  int add = 0;

#ifdef CRC32_CHECK
  unsigned h = compute_crc32 (P, sizeof (int) * len);
  ((int *)res)[0] = h;
  res += add = sizeof (int);
#endif

  return add + LIST_ (encode_list) (P, len, tot_items, res);
}

int LIST_ (decode) (void *v, int len, int tot_items, int *a) {
#ifdef CRC32_CHECK
  unsigned h = ((int *)v)[0];
  v += sizeof (int);
  len -= sizeof (int);
#endif

  LIST_ (iterator) it;
  LIST_ (iter_init) (&it, v, tot_items, len, tot_items);

  int x, i = 0;
  while ((x = LIST_ (iter_val) (&it))) {
    a[i++] = x;
    LIST_ (iter_next) (&it);
  }

#ifdef CRC32_CHECK
  assert (compute_crc32 (a, sizeof (int) * i) == h);
#endif

  return i;
}


void data_iter_init (data_iterator *it, LIST x, changes y, int tot_items, int len, int l, int r) {
  it->l = l;
  it->r = r;
  it->tot_items = tot_items;

  LIST_(iter_init) (&it->list_it, x, tot_items, len, r);
  chg_iter_init (&it->chg_it, y);
}

int data_iter_val_and_next (data_iterator *it) {
  while (1) {
    int chg_val = chg_iter_val (&it->chg_it),
        val = LIST_(iter_val)(&it->list_it);

    if (val <= it->l && chg_val / 2 <= it->l) {
      return it->val = 0;
    }

    if (val == (chg_val >> 1)) {
      /*if (val == 0) { //TODO deoptimize
        return it->val = 0;
      } else*/ {
        LIST_(iter_next) (&it->list_it);
        chg_iter_next (&it->chg_it);

        if (chg_val & 1) {
          return it->val = val;
        } else {
          continue;
        }
      }
    } else if (val * 2 > chg_val) {
      LIST_(iter_next) (&it->list_it);

      return it->val = val;
    } else {
      chg_iter_next (&it->chg_it);
      if (chg_val & 1) {
        chg_val /= 2;
        if (chg_val > it->r && chg_val <= it->tot_items) { //TODO deoptimize
          continue;
        }
        return it->val = chg_val;
      } else {
        continue;
      }
    }
  }
}

int data_iter_can_has (void) {
  return LIST_(iter_can_has) ();
}

int data_iter_has (data_iterator *it, int val) {
  int i;

  if ( (i = chg_has (it->chg_it.x, val)) ) {
      return i > 0;
  }

  return LIST_(iter_has) (&it->list_it, val);
}


/*
int main (void) {
  int a[30] = {5, 4, 3, 3, 3, 1, 1};
  char buff[1000];

  golomb_encode_list (a, 7, 6, buff);
  return 0;
}
*/
