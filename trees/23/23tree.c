#include <stdio.h>
#include <assert.h>

typedef struct tree23 tree23_t;

struct tree23 {
  int x1, x2;
  tree23_t *left, *middle, *right;
};

char leaves[1000000][8];
char nodes[1000000][20];

int lp = 0, np = 0, ld = 0, nd = 0;


static int tree23_lookup (tree23_t *T, int x) {
  if (!T) return 0;
  while (!(T->x1 & 1)) {
    if (x > T->x2) T = T->right;
    else
    if (x < T->x1) T = T->left;
    else
    if (x == T->x1) return 1;
    else
    if (x < (T->x2 & -2)) T = T->middle;
    else
    return 1;
  }
  return (x == (T->x1 & -2)) || (x == (T->x2 & -2));
}

static tree23_t *new_leaf (int x) {
  //printf ("new_leaf: lp = %d\n", lp);
  tree23_t *ret = (tree23_t *)&leaves[lp++];
  ret->x1 = ret->x2 = x | 1;
  //printf ("new_leaf exit: lp = %d\n", lp);
  return ret;
}


static tree23_t *new_node2 (int x, tree23_t *l, tree23_t *r) {
  //printf ("new_node: np = %d\n", np);
  tree23_t *ret = (tree23_t *)&nodes[np++];
  ret->x1 = x;
  ret->x2 = x | 1;
  ret->left = l;
  ret->right = r;
  //printf ("new_node exit: np = %d\n", np);
  return ret;
}


static void free_leaf (tree23_t *pp) {
  //printf ("delete leaf\n");
  assert (pp >= (tree23_t *)leaves && pp < (tree23_t *)(leaves + lp));
  pp->x1 = pp->x2 = 0;
  ++ld;
}


static void free_node (tree23_t *pp) {
  //printf ("delete node\n");
  assert (pp >= (tree23_t *)nodes && pp < (tree23_t *)(nodes + np));
  pp->left = pp->middle = pp->right = 0;
  pp->x1 = pp->x2 = 0;
  ++nd;
}

static void dump (tree23_t *T) {
  if (!T) return;
  printf ("[ ");
  if (!(T->x1 & 1)) {
    dump (T->left);
    printf ("%d ", T->x1);
    if (!(T->x2 & 1)) {
      dump (T->middle);
      printf ("%d ", T->x2);
    }
    dump (T->right);
  } else {
    printf ("%d ", T->x1);
    if (!(T->x2 & 1)) printf ("%d ", T->x2);
  }
  printf ("] ");
}

//x1 has least bit set if it's leaf
//x2 has least bit set if it's equal to x1 (fake node)

// does not check that the element is already there
static void tree23_insert (tree23_t **T, int x) {
  tree23_t *st[100];
  tree23_t *cur, *s, *l;
  int sp;

  //empty tree case
  if (!*T) *T = new_leaf (x);
  else {
    sp = 0;
    cur = *T;
    while (!(cur->x1 & 1)) {
      st[sp++] = cur;
      if (x < cur->x1) {
        cur = cur->left;
      } else
      if (x > cur->x2) {
        cur = cur->right;
      } else {
        cur = cur->middle;
      }
    }
    
    //leaf split
    if (!(cur->x2 & 1)) {
      //case 1. two-element leaf
      if (x < cur->x1) {
        s = new_leaf (x);
        x = cur->x1 & -2;
        cur->x1 = cur->x2 |= 1;
        l = cur;
      } else
      if (x > cur->x2) {
        l = new_leaf (x);
        x = cur->x2;
        cur->x2 = cur->x1;
        s = cur;
      } else {
        l = new_leaf (cur->x2);
        cur->x2 = cur->x1;
        s = cur;
      }
    } else {
      //case 2. single-element leaf
      if (x < cur->x1) {
        cur->x2 = cur->x1 & -2;
        cur->x1 = x | 1;
      } else {
        cur->x2 = x;
      }
      return;
    }

    while (sp) {
      cur = st[--sp];
      if (!(cur->x2 & 1)) {
        //case 1. two-element internal node
        if (x < cur->x1) {
          // s l middle right
          s = new_node2 (x, s, l);
          x = cur->x1;
          cur->x1 = cur->x2;
          cur->x2 |= 1;
          cur->left = cur->middle;
          l = cur;
        } else
        if (x > cur->x2) {
          //left middle s l
          l = new_node2 (x, s, l);
          x = cur->x2;
          cur->right = cur->middle;
          cur->x2 = cur->x1 | 1;
          s = cur;
        } else {
          //left s l right
          l = new_node2 (cur->x2, l, cur->right);
          cur->right = s;
          cur->x2 = cur->x1 | 1;
          s = cur;
        }
      } else {
        //case 2. single-element internal node
        if (x < cur->x1) {
          //s l right
          cur->left = s;
          cur->middle = l;
          cur->x2 &= -2;
          cur->x1 = x;
        } else {
          //left s l 
          cur->middle = s;
          cur->right = l;
          cur->x2 = x;
        }
        return;
      }

    };

    //root split
    *T = new_node2 (x, s, l);
  }
}


// does not check that the element is already there
static void tree23_delete (tree23_t **T, int x) {
  int *P = 0, *PP = 0;
  tree23_t *st[100];
  int sp = 0;
  tree23_t *cur = *T, *up, *succ;


  while (!(cur->x1 & 1)) {
    st[sp++] = cur;
    if (x > cur->x2) cur = cur->right;
    else
    if (x < cur->x1) cur = cur->left;
    else
    if (x == cur->x1) {
      P = &cur->x1;
      if (cur->x2 & 1) PP = &cur->x2;
      x += 2;
      --sp;
      break;
    }
    else
    if (x < (cur->x2 & -2)) cur = cur->middle;
    else {
      P = &cur->x2;
      x += 2;
      --sp;
      break;
    }
  }
  while (!(cur->x1 & 1)) {
    st[sp++] = cur;
    if (x < cur->x1) cur = cur->left;
    else
    if (x > cur->x2) cur = cur->right;
    else
    cur = cur->middle;
  }
  if (P) {
    *P = cur->x1 & -2;
    if (PP) *PP = cur->x1;
  } else 
  if (x == cur->x2) {
    cur->x2 = cur->x1;
    return;
  }
  if (!(cur->x2&1)) {
    cur->x1 = cur->x2 |= 1;
    return;
  }
  //oh, no...
  //printf ("%d\n", sp);
  if (sp == 0) {
    free_leaf (cur);
    *T = 0;
    return;
  }
  up = st[--sp];
  if (up->right == cur) {
    if (up->x2 & 1) {
      if (up->left->x2 & 1) {
        up->left->x2 = up->x1;
        free_leaf (cur);
        succ = up->left;
        //continue to the top
      } else {
        cur->x1 = cur->x2 = up->x2;
        up->x1 = up->left->x2;
        up->left->x2 = up->left->x1;
        up->x2 = up->x1 | 1;
        return;
      }
    } else {
      if (up->middle->x2 & 1) {
        if (up->left->x2 & 1) {
          up->middle->x2 = up->x2;
          free_leaf (cur);
          up->x2 = up->x1 | 1;
          up->right = up->middle;
          return;
        } else {
          cur->x1 = cur->x2 = up->x2 | 1;
          up->x2 = up->middle->x1 & -2;
          up->middle->x1 = up->middle->x2 = up->x1 | 1;
          up->x1 = up->left->x2;
          up->left->x2 = up->left->x1;
          return;
        }
      } else {
        cur->x1 = cur->x2 = up->x2 | 1;
        up->x2 = up->middle->x2;
        up->middle->x2 = up->middle->x1;
        return;
      }
    }
  } else 
  if (up->left == cur) {
    if (up->x2 & 1) {
      if (up->right->x2 & 1) {
        up->right->x1 = up->x1 | 1;
        up->right->x2 &= -2;
        free_leaf (cur);
        succ = up->right;
        //continue to the top
      } else {
        cur->x1 = cur->x2 = up->x2;
        up->x1 = (up->x2 = up->right->x1) & -2;
        up->right->x1 = up->right->x2 |= 1;
        return;
      }
    } else {
      if (up->middle->x2 & 1) {
        if (up->right->x2 & 1) {
          up->middle->x1 = up->x1 | 1;
          up->middle->x2 &= -2;
          up->left = up->middle;
          up->x1 = up->x2;
          up->x2 |= 1;
          free_leaf (cur);
          return;
        } else {
          cur->x1 = cur->x2 = up->x1 | 1;
          up->x1 = up->middle->x2 & -2;
          up->middle->x1 = up->middle->x2 = up->x2 | 1;
          up->x2 = up->right->x1 & -2;
          up->right->x1 = up->right->x2 |= 1;
          return;
        }
      } else {
        cur->x1 = cur->x2 = up->x1 | 1;
        up->x1 = up->middle->x1 & -2;
        up->middle->x1 = up->middle->x2 |= 1;
        return;
      }
    }
  } else {
    if (up->left->x2 & 1) {
      if (up->right->x2 & 1) {
        up->right->x2 &= -2;
        up->right->x1 = up->x2 | 1;
        up->x2 = up->x1 | 1;
        free_leaf (cur);
        return;
      } else {
        cur->x1 = cur->x2 = up->x2 | 1;
        up->x2 = up->right->x1 & -2;
        up->right->x1 = up->right->x2 |= 1;
        return;
      }
    } else {
      cur->x1 = cur->x2 = up->x1 | 1;
      up->x1 = up->left->x2;
      up->left->x2 = up->left->x1;
      return;
    }
  }

  while (sp) {
    cur = up;
    up = st[--sp];
    if (up->right == cur) {
      if (up->x2 & 1) {
        if (up->left->x2 & 1) {
          up->left->x2 = up->x1;
          up->left->middle = up->left->right;
          up->left->right = succ;
          free_node (cur);
          succ = up->left;
        } else {
          cur->x2 = up->x2;
          cur->x1 = cur->x2 & -2;
          cur->right = succ;
          cur->left = up->left->right;
          up->x1 = up->left->x2;
          up->left->x2 = up->left->x1 | 1;
          up->x2 = up->x1 | 1;
          up->left->right=up->left->middle;
          return;
        }
      } else {
        if (up->middle->x2 & 1) {
          if (up->left->x2 & 1) {
            up->middle->x2 = up->x2;
            free_node (cur);
            up->x2 = up->x1 | 1;
            up->middle->middle = up->middle->right;
            up->middle->right = succ;
            up->right = up->middle;
            return;
          } else {
            cur->x1 = up->x2;
            cur->x2 = cur->x1 | 1;
            cur->right = succ;
            cur->left = up->middle->right;
            up->x2 = up->middle->x1;
            up->middle->x1 = up->x1;
            up->middle->x2 = up->x1 | 1;
            up->middle->right = up->middle->left;
            up->middle->left = up->left->right;
            up->x1 = up->left->x2;
            up->left->x2 = up->left->x1 | 1;
            up->left->right = up->left->middle;
            return;
          }
        } else {
          cur->x1 = up->x2;
          cur->x2 = cur->x1 | 1;
          cur->right = succ;
          cur->left = up->middle->right;
          up->x2 = up->middle->x2;
          up->middle->x2 = up->middle->x1 | 1;
          up->middle->right = up->middle->middle;
          return;
        }
      }
    } else
    if (up->left == cur) {
      if (up->x2 & 1) {
        if (up->right->x2 & 1) {
          up->right->x1 = up->x1;
          up->right->x2 &= -2;
          free_node (cur);
          up->right->middle = up->right->left;
          up->right->left = succ;
          succ = up->right;
          //continue to the top
        } else {
          cur ->x2 = (cur->x1 = up->x1) | 1;
          cur->left = succ;
          cur->right = up->right->left;
          up->right->left = up->right->middle;
          up->x2 = (up->x1 = up->right->x1) | 1;
          up->right->x2 = (up->right->x1 = up->right->x2) | 1;
          return;
        }
      } else {
        if (up->middle->x2 & 1) {
          if (up->right->x2 & 1) {
            up->middle->x1 = up->x1;
            up->middle->x2 &= -2;
            up->middle->middle = up->middle->left;
            up->middle->left = succ;
            up->left = up->middle;
            up->x1 = up->x2;
            up->x2 |= 1;
            free_node (cur);
            return;
          } else {
            cur->x2 = (cur->x1 = up->x1) | 1;
            cur->left = succ;
            cur->right = up->middle->left;
            up->middle->left = up->middle->right;
            up->middle->right = up->right->left;
            up->right->left = up->right->middle;
            up->x1 = up->middle->x2 & -2;
            up->middle->x2 = (up->middle->x1 = up->x2) | 1;
            up->x2 = up->right->x1;
            up->right->x1 = up->right->x2;
            up->right->x2 |= 1;
            return;
          }
        } else {
          cur->left = succ;
          cur->right = up->middle->left;
          up->middle->left = up->middle->middle;
          cur->x2 = (cur->x1 = up->x1) | 1;
          up->x1 = up->middle->x1;
          up->middle->x1 = up->middle->x2;
          up->middle->x2 |= 1;
          return;
        }
      }
    } else {
      if (up->left->x2 & 1) {
        if (up->right->x2 & 1) {
          up->right->middle = up->right->left;
          up->right->left = succ;
          up->right->x2 &= -2;
          up->right->x1 = up->x2;
          up->x2 = up->x1 | 1;
          free_node (cur);
          return;
        } else {
          cur->left = succ;
          cur->right = up->right->left;
          up->right->left = up->right->middle;
          cur->x2 = (cur->x1 = up->x2) | 1;
          up->x2 = up->right->x1;
          up->right->x1 = up->right->x2;
          up->right->x2 |= 1;
          return;
        }
      } else {
        cur->right = succ;
        cur->left = up->left->right;
        up->left->right = up->left->middle;
        cur->x2 = (cur->x1 = up->x1) | 1;
        up->x1 = up->left->x2;
        up->left->x2 = up->left->x1 | 1;
        return;
      }
    }
  }

  free_node (up);
  *T = succ;
}

static int *sort (tree23_t *T, int *st) {
  if (!T) return st;
  if (!(T->x1 & 1)) {
    st = sort (T->left, st);
    *st++ = T->x1;
    if (!(T->x2 & 1)) {
      st = sort (T->middle, st);
      *st++ = T->x2;
    }
    st = sort (T->right, st);
  } else {
    *st++ = T->x1 & -2;
    if (!(T->x2 & 1)) *st++ = T->x2;
  }
  return st;
}

static int check (tree23_t *T, int ll, int rr) {
  if (!T) return 0;
  if (T->x1 < ll || T->x1 > rr || T->x2 < ll || T->x2 > rr) return -239017;
  if (T->x1 & 1) {
    if (T->x2 & 1) {
      if (T->x2 != T->x1) return -239017;
    } else {
      if (T->x2 < T->x1) return -239017;
    }
    return 1;
  }
  int ld = check (T->left, ll, T->x1 - 1);
  int rd = check (T->right, (T->x2 | 1) + 1, rr);
  if (ld != rd) ld = -239017;
  if (!(T->x2 & 1)) {
    rd = check (T->middle, T->x1 + 2, T->x2 - 1);
    if (ld != rd) ld = -239017;
    if (T->x2 < T->x1) return -239017;
  } else {
    if (T->x2 != T->x1 + 1) return -239017;
  }
  return ++ld;
}


static void count (tree23_t *T, int *lc, int *nc, int *cc) {
  if (!T) {
    *lc = 0; *nc = 0; *cc = 0;
    return;
  }
  if (T->x1 & 1) {
    ++*lc;
    ++*cc;
    if (!(T->x2 & 1)) ++*cc;
  } else {
    ++*nc;
    count (T->left, lc, nc, cc);
    if (!(T->x2 & 1)) {
      ++*cc;
      count (T->middle, lc, nc, cc);
    }
    ++*cc;
    count (T->right, lc, nc, cc);
  }
}

tree23_t *T = 0;

int arr[1000000];


int main () {
  int n, t, p, i, j;
  int lc = 0, nc = 0 , cc = 0;
  int *tmp;
  scanf ("%d", &n);
  for (i = 0; i < n; i++) {
    scanf ("%d", &t);
    switch (t) {
      case 1:
        scanf ("%d", &p);
        if (!tree23_lookup (T, p))
          tree23_insert (&T, p);
        break;
      case 2:
        scanf ("%d", &p);
        puts (tree23_lookup (T, p)?"YES":"NO");
        break;
      case 3:
        scanf ("%d", &p);
        tmp = sort (T, arr);
        p = tmp - arr;
        for (j = 0; j < p; j++)
          printf ("%d%c", arr[j], '\n');
        break;
      case 4:
        scanf ("%d", &p);
        if (tree23_lookup (T, p))
          tree23_delete (&T, p);
        break;
    }
    //dump (T);
    if (check (T, -1000000000, 1000000000) < 0) printf ("BAD TREE\n");
  }
  printf ("%d\n", check (T, -1000000000, 1000000000));
  dump (T);
  tmp = sort (T, arr);
  p = tmp - arr;
  for (j = 0; j < p; j++)
    printf ("%d%c", arr[j], '\n');
  count (T, &lc, &nc, &cc);
  printf ("leaves allocated %d\nnodes allocated %d\nleaves freed %d\nnodes freed %d\nleaves current %d\nnodes current %d\n", lp, np, ld, nd, lp - ld, np - nd);
  printf ("leaves in tree %d\nnodes in tree %d\nnumbers in tree %d\n", lc, nc, cc);
}
