#include <stdio.h>
#include <assert.h>
#include <string.h>

typedef struct rbtree rbtree_t;

struct rbtree {
  rbtree_t *left, *right;
  int x;
  int extra[0];
};

typedef struct rbtree_root rbtree_root_t;

struct rbtree_root {
  rbtree_root_t *root;
  char depth;
  char extra_words;
  char reserved[2];
};

char heap[10000000], *heap_a = heap;

int lp = 0, np = 0, ld = 0, nd = 0;

void *zmalloc (int size) {
  void *p = heap_a;
  heap_a += size;
  return p;
}

void zfree (void *data, int size) {
}

static void *rbtree_lookup (rbtree_t *T, int x) {
  x <<= 1;
  while (T) {
    if (T->x < x) {
      T = T->right;
    } else if (T->x > x + 1) {
      T = T->left;
    } else {
      return T;
    }
  }
  return 0;
}

static rbtree_t *new_node (int x, int extra, const void *data) {
  //printf ("new_node: np = %d\n", np);
  rbtree_t *N = (rbtree_t *) zmalloc (sizeof (rbtree_t) + 4*extra);
  N->x = x;
  N->left = N->right = 0;
  np++;
  if (extra > 0) {
    memcpy (N->extra, data, extra * 4);
  }
  //printf ("new_node exit: np = %d\n", np);
  return N;
}


static void free_node (rbtree_t *N, int extra) {
  //printf ("delete node\n");
  N->left = N->right = 0;
  N->x = 0;
  ++nd;
  zfree ((char *) N, sizeof (rbtree_t) + 4*extra);
}

static void dump (rbtree_t *T, int extra) {
  int i;
  if (!T) return;
  printf ("[ ");
  dump (T->left, extra);

  printf ("%d", T->x);

  for (i = 0; i < extra; i++) {
    printf (":%d", T->extra[i]);
  }

  putchar (' ');

  dump (T->right, extra);
  printf ("] ");
}

//x has least bit set if it is RED

#define IS_RED(__t)	((__t)->x & 1)
#define IS_BLACK(__t)	(!IS_RED(__t))
#define	REDDEN(__t)	((__t)->x |= 1)
#define	BLACKEN(__t)	((__t)->x &= -2)

// checks whether the element is already there
static rbtree_t *rbtree_insert (rbtree_t *Root, int x, int extra, int *Data) {
  rbtree_t *st[70];
  rbtree_t *T, *N, *U;
  int sp;

  x <<= 1;

  //empty tree case
  if (!Root) {
    return new_node (x, extra, Data);
  }

  sp = 0;
  T = Root;
  while (T) {
    st[sp++] = T;
    if (T->x < x) {
      T = T->right;
    } else if (T->x > x + 1) {
      T = T->left;
    } else {
// x already there ...
      return T;
    }
  }

  N = new_node (x+1, extra, Data);

  while (sp > 0) {
    T = st[--sp];
    // one of subtrees of T is to be replaced with RED N
    // after that, tree would be RB unless T is also RED
    if (x < T->x) {
      // N replaces left subtree of T
      if (IS_BLACK(T)) {
        // if T is BLACK, we are done
        T->left = N;
        return Root;
      }
      if (!sp) {
        // if T is RED and is the root, simply make it BLACK
        BLACKEN(T);
        T->left = N;
        return Root;
      }
      U = st[--sp];
      // here T is RED, so its parent U must be BLACK
      if (x < U->x) {
        // T is the left subtree of U
        // now U:[ T:{ N:{.x.} y.} z (right) ]
        // --> new_U=T:{ N:[.x.] y [.z (right) ]}
        U->left = T->right;
        T->right = U;
        BLACKEN(N);
        N = U;
      } else {
        // now U:[ T:{.u N:{.x.}} y (right) ]
        // --> new_U=N:{ T:[.u.] x U:[.y (right) ]}
        T->right = N->left;
        BLACKEN(T);
        N->left = T;
        U->left = N->right;
        N->right = U;
      }
    } else {
      // N replaces right subtree of T
      if (IS_BLACK(T)) {
        // if T is BLACK, we are done
        T->right = N;
        return Root;
      }
      if (!sp) {
        // if T is RED and is the root, simply make it BLACK
        BLACKEN(T);
        T->right = N;
        return Root;
      }
      U = st[--sp];
      // here T is RED, so its parent U must be BLACK
      if (x < U->x) {
        // now U:[ (left) u T:{ N:{.x.} y.} ]
        // --> new_U=N:{ U:[.u.] x T:[.y.]}
        T->left = N->right;
        BLACKEN(T);
        N->right = T;
        U->right = N->left;
        N->left = U;
      } else {
        // now U:[ (left) u T:{.v N:{.x.} } ]
        // --> new_U=T:{ U:[(left) u.] v N:[.x.]}
        U->right = T->left;
        T->left = U;
        BLACKEN(N);
        N = T;
      }
    }
  }
  // if we come here, the whole tree is to be replaced with N
  return N;
}


// does not check that the element is already there
static rbtree_t *rbtree_delete (rbtree_t *Root, int extra) {
  rbtree_t *st[40];
  int sp, i;
  rbtree_t *T, *U, *V;

  x <<= 1;

  sp = 0;
  T = Root;
  while (T) {
    st[sp++] = T;
    if (T->x < x) {
      T = T->right;
    } else if (T->x > x + 1) {
      T = T->left;
    } else {
      // x found...
      break;
    }
  }

  if (!T) {
    // x not found
    return 0;
  }

  if (sp == 1) {
    // deleting root
    V = T->left ? T->left : T->right;
    free_node (T, extra);
    return V;
  }

  if (T->right && T->left) {
    // if T has two subtrees, have to exchange T with its successor
    U = T;
    T = T->right;
    i = sp;
    do {
      st[sp++] = T;
      T = T->left;
    } while (T);

    T = st[sp-1];
    // have to exchange node T with value x'>x with node U with value x  
    if (extra <= 5) {
      // if node data is at most 20 bytes, copy data, but preserve color
      U->x = (T->x & -2) | (U->x & 1);
      if (extra > 0) {
        memcpy (U->extra, T->extra, extra * 4);
      }
    } else {
      // if node data is large, replace pointers instead
    if (i < 2) {
      Root = T;
    } else {
      // if U has parent V, replace U with T in the children of V
      V = st[i - 2];
      if (x < V->x) {
        V->left = T;
      } else {
        V->right = T;
      }
    }
    V = st[sp-2]; // V is the parent of T
    if (V == U) {
      // T is the right child of U=V
      U->right = T->right;
      T->left = U->left;
      T->right = U;
      U->left = 0;
    } else {
      // T is the left child of V, replace it with U
      V->left = U;
      T->left = U->left;
      U->left = 0;
      V = U->right;
      U->right = T->right;
      T->right = V;
    }
    if ((U->x ^ T->x) & 1) {
      // exchange colors
      U->x ^= 1;
      T->x ^= 1;
    }
    T = U;
  }

  // here we have to delete node T=st[sp-1] with at most one child
  // if this child exists, it must be RED, and T must be BLACK
  U = st[sp-2];
  V = T->right;
  if (!V) {
    V = T->left;
  }
  if (V || IS_RED(T)) {
    // if child V exists, or if T is RED, all is simple
    free_node (T, extra);
    if (U->right == T) {
      U->right = V;
    } else {
      U->left = V;
    }
    BLACKEN(V);
    return Root;
  }

  // now we delete a BLACK leaf node T without children
  free_node (T, extra);
  N = 0;
  sp--;

  while (sp > 0) {
    U = st[--sp];
    // here U is a node, T is its ex-child, to be replaced with N
    // however, black_depth(N) = black_depth(T) - 1


  }


    
    




  return Root;
}

static int *sort_rec (tree23_t *T, int *st, int depth) {
  if (--depth >= 0) {
    st = sort_rec (T->left, st, depth);
    *st++ = T->x1;
    if (T->x2 > T->x1) {
      st = sort_rec (T->middle, st, depth);
      *st++ = T->x2;
    }
    st = sort_rec (T->right, st, depth);
  } else {
    *st++ = T->x1;
    if (T->x2 > T->x1) {
      *st++ = T->x2;
    }
  }
  return st;
}

static int *sort (tree23_root_t *R, int *st) {
  if (!R->root) {
    return st;
  }
  return sort_rec (R->root, st, R->depth);
}

static int check_rec (tree23_t *T, int ll, int rr, int depth) {
  if (!T) {
    return 0;
  }
  if (T->x1 <= ll || T->x1 >= rr || T->x2 <= ll || T->x2 >= rr) {
    return 0;
  }
  if (T->x1 > T->x2) {
    return 0;
  }
  if (--depth >= 0) {
    if (!check_rec (T->left, ll, T->x1, depth) || 
        !check_rec (T->right, T->x2, rr, depth)) {
      return 0;
    }
    if (T->x1 < T->x2) {
      return check_rec (T->middle, T->x1, T->x2, depth);
    }
  }
  return 1;
}

static int check (tree23_root_t *R) {
  if (!R->root) {
    if (R->depth) {
      return -1;
    }
    return 0;
  }
  if (R->depth < 0) {
    return -1;
  }
  return check_rec (R->root, -1 << 31, ~(-1 << 31), R->depth) ? R->depth : -1;
}

static void count_rec (tree23_t *T, int depth, int *A) {
  if (--depth < 0) {
    A[1]++;
    A[0]++;
    if (T->x2 > T->x1) {
      A[0]++;
    }
  } else {
    A[2]++;
    count_rec (T->left, depth, A);
    if (T->x1 < T->x2) {
      A[0]++;
      count_rec (T->middle, depth, A);
    }
    A[0]++;
    count_rec (T->right, depth, A);
  }
}


static void count (tree23_root_t *R, int *A) {
  A[0] = A[1] = A[2] = 0;
  if (!R->root) {
    return;
  }
  count_rec (R->root, R->depth, A);
}

tree23_root_t Root;

int arr[1000000];


int main (int argc, const char *argv[]) {
  int n, t, p, i, j;
  int counters[3], extra[4];
  int *tmp;
  if (argc >= 2 && !strcmp (argv[1], "-e")) {
    Root.extra_words = 1;
  }
  scanf ("%d", &n);
  for (i = 0; i < n; i++) {
    scanf ("%d", &t);
    switch (t) {
      case 1:
        scanf ("%d", &p);
        if (!tree23_lookup (&Root, p)) {
          extra[0] = p+3;
          tree23_insert (&Root, p, extra);
        }
        break;
      case 2:
        scanf ("%d", &p);
        puts (tree23_lookup (&Root, p)?"YES":"NO");
        break;
      case 3:
        scanf ("%d", &p);
        tmp = sort (&Root, arr);
        p = tmp - arr;
        for (j = 0; j < p; j++)
          printf ("%d%c", arr[j], '\n');
        break;
      case 4:
        scanf ("%d", &p);
        if (tree23_lookup (&Root, p))
          tree23_delete (&Root, p);
        break;
    }
    //dump (T);
    if (check (&Root) < 0) printf ("BAD TREE\n");
  }
  printf ("%d\n", check (&Root));
  dump (&Root);
  tmp = sort (&Root, arr);
  p = tmp - arr;
  for (j = 0; j < p; j++)
    printf ("%d%c", arr[j], '\n');
  count (&Root, counters);
  printf ("leaves allocated %d\nnodes allocated %d\nleaves freed %d\nnodes freed %d\nleaves current %d\nnodes current %d\n", lp, np, ld, nd, lp - ld, np - nd);
  printf ("leaves in tree %d\nnodes in tree %d\nnumbers in tree %d\n", counters[1], counters[2], counters[0]);
  return 0;
}
