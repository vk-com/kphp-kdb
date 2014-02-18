#include <stdio.h>
#include <assert.h>
#include <string.h>

typedef struct tree23 tree23_t;
typedef struct tree23_leaf tree23_leaf_t;

struct tree23 {
  int extra[0];
  int x1, x2;
  tree23_t *left, *middle, *right;
};

struct tree23_leaf {
  int extra[0];
  int x1, x2;
};

typedef struct tree23_root tree23_root_t;

struct tree23_root {
  tree23_t *root;
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

static void *tree23_lookup (tree23_root_t *R, int x) {
  tree23_t *T = R->root;
  int i;
  if (!T) {
    return 0;
  }
  for (i = R->depth; i > 0; i--) {
    if (x < T->x1) {
      T = T->left;
    } else if (x == T->x1) {
      return T->extra;
    } else if (x > T->x2) {
      T = T->right;
    } else if (x == T->x2) {
      return T->extra + R->extra_words;
    } else {
      T = T->middle;
    }
    if (!T) {
      return 0;
    }
  }

  if (T->x1 == x) {
    return ((tree23_leaf_t *) T)->extra;
  } else if (T->x2 == x) {
    return ((tree23_leaf_t *) T)->extra + R->extra_words;
  }

  return 0;
}

static tree23_t *new_leaf (int x, int extra) {
  tree23_t *L = (tree23_t *) (zmalloc (sizeof (tree23_leaf_t) + 8*extra) + 8*extra);
  L->x1 = L->x2 = x;
  lp++;
  return L;
}


static tree23_t *new_node2 (int x, tree23_t *l, tree23_t *r, int extra) {
  //printf ("new_node: np = %d\n", np);
  tree23_t *N = (tree23_t *) (zmalloc (sizeof (tree23_t) + 8*extra) + 8*extra);
  N->x1 = x;
  N->x2 = x;
  N->left = l;
  N->right = r;
  np++;
  //printf ("new_node exit: np = %d\n", np);
  return N;
}


static void free_leaf (tree23_t *pp, int extra) {
  //printf ("delete leaf\n");
  pp->x1 = pp->x2 = 0;
  ++ld;
  zfree ((char *) pp - 8*extra, sizeof (tree23_leaf_t) + 8*extra);
}


static void free_node (tree23_t *pp, int extra) {
  //printf ("delete node\n");
  pp->left = pp->middle = pp->right = 0;
  pp->x1 = pp->x2 = 0;
  ++nd;
  zfree ((char *) pp - 8*extra, sizeof (tree23_t) + 8*extra);
}

static void dump_rec (tree23_t *T, int depth, int extra) {
  int i;
  if (!T) return;
  printf ("[ ");
  if (depth--) {
    dump_rec (T->left, depth, extra);
    printf ("%d", T->x1);
    for (i = -extra; i < 0; i++) {
      printf (":%d", T->extra[i]);
    }
    putchar (' ');
    if (T->x2 > T->x1) {
      dump_rec (T->middle, depth, extra);
      printf ("%d", T->x2);
      for (i = -2*extra; i < -extra; i++) {
        printf (":%d", T->extra[i]);
      }
      putchar (' ');
    }
    dump_rec (T->right, depth, extra);
  } else {
    printf ("%d", T->x1);
    for (i = -extra; i < 0; i++) {
      printf (":%d", ((tree23_leaf_t *) T)->extra[i]);
    }
    putchar (' ');
    if (T->x2 > T->x1) {
      printf ("%d", T->x2);
      for (i = -2*extra; i < -extra; i++) {
        printf (":%d", ((tree23_leaf_t *) T)->extra[i]);
      }
      putchar (' ');
    }
  }
  printf ("] ");
}

static void dump (tree23_root_t *R) {
  dump_rec (R->root, R->depth, R->extra_words);
}

//x1 has least bit set if it's leaf
//x2 has least bit set if it's equal to x1 (fake node)

// does not check whether the element is already there
static void tree23_insert (tree23_root_t *R, int x, int *Data) {
  tree23_t *st[40];
  int x_Data[8];
  tree23_t *cur, *s, *l;
  int sp, extra_words = R->extra_words;

#define Extra_words	extra_words
#define x1_Data		extra-Extra_words
#define x2_Data		extra-Extra_words*2
#define DATA(__x)	(__x##_Data)
#define	CPY(__x,__y)	{if(Extra_words>0) {memcpy(__x,__y,Extra_words*4);}}
#define DCPY(__x,__y)	CPY(DATA(__x),DATA(__y))
#define	LET(__x,__y)	{__x = __y; DCPY(__x,__y);}
#define IS_2N(__t)	((__t)->x1 == (__t)->x2)
#define IS_3N(__t)	(!IS_2N(__t))
#define LEAF(__t)	((tree23_leaf_t *)(__t))

  //empty tree case
  if (!R->root) {
    R->root = new_leaf (x, extra_words);
    CPY(DATA(R->root->x1), Data);
    R->depth = 0;
    return;
  }

  sp = 0;
  cur = R->root;
  while (sp < R->depth) {
    st[sp++] = cur;
    if (x < cur->x1) {
      cur = cur->left;
    } else if (x > cur->x2) {
      cur = cur->right;
    } else {
      cur = cur->middle;
    }
  }
  
  //leaf split
  if (IS_3N(cur)) {
    //case 1. two-element leaf: have cur:[ B D ]
    if (x < cur->x1) {
      // cur:[ B D ] + x:A --> [ A B D ] --> (new)s:[ A ] new_x:B (cur)l:[ D ]
      s = new_leaf (x, Extra_words);
      CPY (DATA(s->x1), Data)
      LET (x, cur->x1);
      LET (cur->x1, cur->x2);
      l = cur;
    } else if (x > cur->x2) {
      // cur:[ B D ] + x:E --> [ A D E ] --> (cur)s:[ B ] new_x:D (new)l:[ E ]
      l = new_leaf (x, Extra_words);
      CPY (DATA(l->x1), Data)
      LET (x, cur->x2)
      cur->x2 = cur->x1;
      s = cur;
    } else {
      // cur:[ B D ] + x:C --> [ A C E ] --> (cur)s:[ B ] new_x:C (new)l:[ D ]
      l = new_leaf (cur->x2, Extra_words);
      CPY (DATA(l->x1), DATA(cur->x2))
      CPY (DATA(x), Data);
      cur->x2 = cur->x1;
      s = cur;
    }
  } else {
    //case 2. single-element leaf:  have cur:[ B ]
    if (x < cur->x1) {
      // cur:[ B ] + x:A --> cur:[ A B ]
      LET (cur->x2, cur->x1);
      cur->x1 = x;
      CPY (DATA(cur->x1), Data);
    } else {
      // cur:[ B ] + x:C --> cur:[ B C ]
      cur->x2 = x;
      CPY (DATA(cur->x2), Data);
    }
    return;
  }

  while (sp) {
    cur = st[--sp];
    // here cur is a parent node cur: [ ... old_cur:[.E.G.] ... ]
    // we are replacing its subtree [.E.G.] with two: s:[.E.] x:F l:[.G.]
    if (IS_3N(cur)) {
      //case 1. two-element internal node
      // cur: [ (left) x1 (middle) x2 (right) ]
      if (x < cur->x1) {
        // s l middle right
        // cur: [ old_cur:[.E.G.] x1:H [.I.] x2:J [.K.] ]
        // -->  [ s:[.E.] x:F l:[.G.] x1:H [.I.] J [.K.] ]
        // -->  (new)new_s:[ s:[.E.] x:F l:[.G.] ] new_x:H (cur)new_l:[ [.I.] J [.K.] ]
        s = new_node2 (x, s, l, Extra_words);
        DCPY(s->x1, x);
        LET (x, cur->x1);
        LET (cur->x1, cur->x2);
        cur->left = cur->middle;
        l = cur;
      } else
      if (x > cur->x2) {
        // left middle s l
        // cur: [ [.A.] B [.C.] D old_cur:[.E.G.] ]
        // -->  [ [.A.] x1:B [.C.] x2:D s:[.E.] x:F l:[.G.] ]
        // -->  (cur)new_s:[ [.A.] x1:B [.C.] ] new_x:D (new)new_l:[ s:[.E.] x:F l:[.G.] ]
        l = new_node2 (x, s, l, Extra_words);
        DCPY(l->x1, x);
        LET (x, cur->x2);
        cur->right = cur->middle;
        cur->x2 = cur->x1;
        s = cur;
      } else {
        //left s l right
        // cur: [ [.C.] x1:D old_cur:[.E.G.] x2:H [.I.] ]
        // --> [ [.C.] x1:D s:[.E.] x:F l:[.G.] x2:H [.I.] ]
        // --> (cur)new_s:[ [.C.] x1:D s:[.E.] ] new_x:F (new)new_l:[l:[.G.] x2:H [.I.] ]
        l = new_node2 (cur->x2, l, cur->right, Extra_words);
        DCPY(l->x1, cur->x2);
        cur->right = s;
        cur->x2 = cur->x1;
        s = cur;
      }
    } else {
      //case 2. single-element internal node
      // cur: [ (left) x1=x2 (right) ]
      if (x < cur->x1) {
        // s l right
        // cur: [ old_cur:[.E.G.] x1:H [.I.] ]
        // -->  [ s:[.E.] x:F l:[.G.] x1:H [.I.] ]
        cur->left = s;
        cur->middle = l;
        cur->x1 = x;
        DCPY(cur->x2, cur->x1);
        DCPY(cur->x1, x);
      } else {
        //left s l 
        // cur: [ [.C.] x1:D old_cur:[.E.G.] ]
        // -->  [ [.C.] x1:D s:[.E.] x:F l:[.G.] ]
        cur->middle = s;
        cur->right = l;
        LET(cur->x2, x);
      }
      return;
    }

  }

  //root split
  // here  s:[.E.] x:F l:[.G.] comes to the top
  // create new root [ [.E.] F [.G.] ]
  R->root = new_node2 (x, s, l, Extra_words);
  R->depth++;
  CPY (DATA(R->root->x1), Data);
}


// does not check that the element is already there
static int tree23_delete (tree23_root_t *R, int x) {
  int *P = 0, *PP = 0;
  tree23_t *st[40];
  int sp, *y_Data = 0;
  tree23_t *cur = R->root, *up, *succ;
  int extra_words = R->extra_words;

  for (sp = 0; sp < R->depth; sp++) {
    st[sp] = cur;
    if (x > cur->x2) {
      cur = cur->right;
    } else if (x < cur->x1) {
      cur = cur->left;
    } else if (x == cur->x1) {
      P = &cur->x1;
      y_Data = DATA(cur->x1);
      if (cur->x2 == cur->x1) {
        PP = &cur->x2;
      }
      x++;
      break;
    } else if (x < cur->x2) {
      cur = cur->middle;
    } else {
      P = &cur->x2;
      y_Data = DATA(cur->x2);
      x++;
      break;
    }
  }

  // if x belongs to an inner node:
  // - P points to the key equal to (original) x in node cur
  // - PP points to cur->x2 if cur->x2 = cur->x1 = (original) x
  // - x equals original x+1 for some reason
  // if x is in a leaf, cur is this leaf, and P=PP=0

  while (sp < R->depth) {
    st[sp++] = cur;
    if (x < cur->x1) {
      cur = cur->left;   // actually will go left at all steps except the first one
    } else if (x > cur->x2) {
      cur = cur->right;
    } else {
      cur = cur->middle;
    }
  }

  // now cur is the leaf containing next value after (original) x, if x was in a inner node
  // otherwise, cur is the leaf containing x

  if (P) {
    // case 1: x was found in some inner node, ancestor of leaf cur
    // then x':=cur->x1 is the next value in tree after x
    // and we replace references to x with references to x'
    *P = cur->x1;
    if (PP) {
      *PP = cur->x1;
    }
    DCPY (y, cur->x1); // copy extra data words as well
    // after that, we just need to remove x' from leaf node cur
    if (cur->x1 < cur->x2) {
      // case 1a: cur: [ x' y ] , replace with [ y ]
      LET (cur->x1, cur->x2);
      return 1;
    }
  } else if (x == cur->x1) {
    if (x < cur->x2) {
      // case 0a: x was found in leaf cur: [ x y ], x < y
      // replace with [ y ]
      LET (cur->x1, cur->x2);
      return 1;
    }
  } else if (x == cur->x2) {
    // case 0b: x was found in leaf cur: [ u x ], u < x
    // simply replace it with [ u ]
    cur->x2 = cur->x1;
    return 1;
  } else {
    // x NOT FOUND in tree (?)
    return 0;
  }

  // here we have to remove x' from leaf node cur: [ x' ]

  //oh, no...
  //printf ("%d\n", sp);
  if (sp == 0) {
    // we are deleting the root!
    free_leaf (cur, Extra_words);
    R->root = 0;
    return 1;
  }

  up = st[--sp];
  // up is the parent of leaf cur: [ x' ]  ( we are deleting x': "cur --> []")
  if (up->right == cur) {
    if (IS_2N(up)) {
      // up: [ (left) x1 cur:[ x' ] ]
      if (IS_2N(up->left)) {
        // up:  [ [ u ] x1 cur:[ x' ] ]
        // -->  [ [ u ] x1 [] ]
        // -->  [ succ:[ u x1 ] ]
        LET (up->left->x2, up->x1);
        free_leaf (cur, Extra_words);
        succ = up->left;
        //continue to the top
      } else {
        // up: [ [ u v ] x1 cur:[ x' ] ]
        // --> [ [ u v ] x1 [] ]
        // --> [ [ u ] v [ x1 ] ]
        cur->x1 = cur->x2 = up->x1;
        DCPY (cur->x1, up->x1);
        LET (up->x1, up->left->x2);
        up->left->x2 = up->left->x1;
        up->x2 = up->x1;
        return 1;
      }
    } else {
      // up: [ (left) x1 (middle) x2 cur:[ x' ] ]
      if (IS_2N(up->middle)) {
        // ! ELIMINATED CASE: if (up->left->x2 == up->left->x1) 
        // up: [ (left) x1 [ u ] x2 cur:[ x' ] ]
        // --> [ (left) x1 [ u ] x2 [] ]
        // --> [ (left) x1 [ u x2 ] ]
        LET (up->middle->x2, up->x2);
        up->x2 = up->x1;
        up->right = up->middle;
        free_leaf (cur, Extra_words);
        return 1;
      } else {
        // up: [ (left) x1 [ u v ] x2 cur:[ x' ] ]
        // --> [ (left) x1 [ u v ] x2 [] ]
        // --> [ (left) x1 [ u ] v [ x2 ] ]
        LET (cur->x1, up->x2);
        cur->x2 = cur->x1;
        LET (up->x2, up->middle->x2);
        up->middle->x2 = up->middle->x1;
        return 1;
      }
    }
  } else if (up->left == cur) {
    if (IS_2N(up)) {
      // up: [ cur:[ x' ] x1 (right) ]
      if (IS_2N(up->right)) {
        // up: [ cur:[ x' ] x1 succ:[ y ] ]
        // --> [ ? ? succ: [ x1 y ] ]
        DCPY (up->right->x2, up->right->x1)
        LET (up->right->x1, up->x1);
        free_leaf (cur, Extra_words);
        succ = up->right;
        //continue to the top
      } else {
        // up: [ cur:[ x' ] x1 [ y z ] ]
        // --> [ [] x1 [ y z ] ]
        // --> [ [ x1 ] y [ z ] ]
        LET (cur->x1, up->x1);
        cur->x2 = cur->x1;
        LET (up->x1, up->right->x1);
        up->x2 = up->x1;
        LET (up->right->x1, up->right->x2);
        return 1;
      }
    } else {
      // up: [ cur:[ x' ] x1 (middle) x2 (right) ]
      if (IS_2N(up->middle)) {
        // ! ELIMINATED CASE: if (up->right->x2 & 1) {
        // up: [ cur:[ x' ] x1 [ y ] x2 (right) ]
        // --> [ [] x1 [ y ] x2 (right) ]
        // --> [ [ x1 y ] x2 (right) ]
        DCPY (up->middle->x2, up->middle->x1);
        LET (up->middle->x1, up->x1);
        up->left = up->middle;
        LET (up->x1, up->x2);
        free_leaf (cur, Extra_words);
        return 1;
      } else {
        // up: [ cur:[ x' ] x1 [ y z ] x2 (right) ]
        // --> [ [] x1 [ y z ] x2 (right) ]
        // --> [ [ x1 ] y [ z ] x2 (right) ]
        LET (cur->x1, up->x1);
        cur->x2 = cur->x1;
        LET (up->x1, up->middle->x1);
        LET (up->middle->x1, up->middle->x2);
        return 1;
      }
    }
  } else {  
    // here cur == up->middle
    // up: [ (left) x1 cur:[ x' ] x2 (right) ]
    if (IS_2N(up->left)) {
      // up: [ [ v ] x1 cur:[ x' ] x2 (right) ]
      if (IS_2N(up->right)) {
        // up: [ [ v ] x1 cur:[ x' ] x2 [ y ] ]
        // --> [ [ v ] x1 [] x2 [ y ] ]
        // --> [ [ v ] x1 [ x2 y ] ]
        DCPY (up->right->x2, up->right->x1);
        LET (up->right->x1, up->x2);
        up->x2 = up->x1;
        free_leaf (cur, Extra_words);
        return 1;
      } else {
        // up: [ [ v ] x1 cur:[ x' ] x2 [ y z ] ]
        // --> [ [ v ] x1 [] x2 [ y z ] ]
        // --> [ [ v x1 ] x2 [ y z ] ]
        // ! WAS: --> [ [ v ] x1 [ x2 ] y [ z ] ]
        LET (up->left->x2, up->x1);
        LET (up->x1, up->x2);
        free_leaf (cur, Extra_words);
        return 1;
      }
    } else {
      // up: [ [ u v ] x1 cur:[ x' ] x2 (right) ]
      // --> [ [ u v ] x1 [] x2 (right) ]
      // up: [ [ u ] v cur:[ x1 ] x2 (right) ]
      LET (cur->x1, up->x1);
      cur->x2 = cur->x1;
      LET (up->x1, up->left->x2);
      up->left->x2 = up->left->x1;
      return 1;
    }
  }

  // we come here exactly in two of the above cases:
  // namely, if `cur`, its parent `up` and sibling `succ` are 2-nodes
  // then the subtree at `up` contains only 3 elements, and after removal of x'
  // it must contain only two entries, which is impossible
  
  // here: succ: [.u.v.] is the new replacement for the tree at `up`
  // informally: "current" value of `up` is assumed to be [ succ:[.u.v.] ]
  // 	but actually `up` cannot be a "1-node", so we want to correct this

  while (sp) {
    cur = up;
    up = st[--sp];
    // now `cur` is the root of the subtree to be replaced with `succ`
    // `up` is the parent of `cur`
    if (up->right == cur) {
      // up: [ ... cur:(right) ]
      if (IS_2N(up)) {
        // up: [ (left) x1 cur:(right) ]
        if (IS_2N(up->left)) {
          // up: [ [.t.] x1 cur:(right) ]
          // --> [ [.t.] x1 cur:[ (succ) ] ] , succ has incorrect depth!
          // --> [ new_succ:[.t.x1 (succ) ] ]
          // after that: succ is at a good place, but up is to be replaced with [ new_succ ]
          LET (up->left->x2, up->x1);
          up->left->middle = up->left->right;
          up->left->right = succ;
          free_node (cur, Extra_words);
          succ = up->left;
        } else {
          // up: [ [.s.t.] x1 cur:(right) ]
          // --> [ [.s.t.] x1 cur:[ (succ) ] ]
          // --> [ [.s.] t cur:[.x1 (succ) ] ]
          LET (cur->x1, up->x2);
          cur->x2 = cur->x1;
          cur->right = succ;
          cur->left = up->left->right;
          LET (up->x1, up->left->x2);
          up->x2 = up->x1;
          up->left->x2 = up->left->x1;
          up->left->right = up->left->middle;
          return 1;
        }
      } else {
        // up: [ (left) x1 (middle) x2 cur:(right) ]
        if (IS_2N(up->middle)) {
          // up: [ (left) x1 [.t.] x2 cur:[ (succ) ] ]
          // --> [ (left) x1 [.t.x2.(succ)] ]
          up->right = up->middle;
          LET (up->right->x2, up->x2);
          up->x2 = up->x1;
          up->right->middle = up->right->right;
          up->right->right = succ;
          free_node (cur, Extra_words);
          return 1;
        } else {
          // up: [ (left) x1 [.s.t.] x2 cur:[ (succ) ] ]
          // --> [ (left) x1 [.s.] t cur:[.x2 (succ)] ]
          LET (cur->x1, up->x2);
          cur->x2 = cur->x1;
          cur->right = succ;
          cur->left = up->middle->right;
          LET (up->x2, up->middle->x2);
          up->middle->x2 = up->middle->x1;
          up->middle->right = up->middle->middle;
          return 1;
        }
      }
    } else if (up->left == cur) {
      // up: [ cur:(left) ... ]
      if (IS_2N(up)) {
        // up: [ cur:(left) x1 (right) ]
        if (IS_2N(up->right)) {
          // up: [ cur:[ (succ) ] x1 [.y.] ]
          // --> [ new_succ:[ (succ) x1.y.] ]
          DCPY (up->right->x2, up->right->x1);
          LET (up->right->x1, up->x1);
          up->right->middle = up->right->left;
          up->right->left = succ;
          succ = up->right;
          free_node (cur, Extra_words);
          //continue to the top
        } else {
          // up: [ cur:[ (succ) ] x1 [.y.z.] ]
          // --> [ cur:[ (succ) x1. ] y [.z.] ]
          LET (cur->x1, up->x1);
	  cur->x2 = cur->x1;
          cur->left = succ;
          cur->right = up->right->left;
          up->right->left = up->right->middle;
          LET (up->x1, up->right->x1);
          up->x2 = up->x1;
          LET (up->right->x1, up->right->x2);
          return 1;
        }
      } else {
        // up: [ cur:(left) x1 (middle) x2 (right) ]
        if (IS_2N(up->middle)) {
          // up: [ cur:[(succ)] x1 [.y.] x2 (right) ]
          // --> [ [(succ) x1.y.] x2 (right) ]
          DCPY (up->middle->x2, up->middle->x1);
          LET (up->middle->x1, up->x1);
          up->middle->middle = up->middle->left;
          up->middle->left = succ;
          up->left = up->middle;
          LET (up->x1, up->x2);
          free_node (cur, Extra_words);
          return 1;
        } else {
          // up: [ cur:[(succ)] x1 [.y.z.] x2 (right) ]
          // --> [ [(succ) x1.] y [.z.] x2 (right) ]
          cur->left = succ;
          cur->right = up->middle->left;
          LET (cur->x1, up->x1);
          cur->x2 = cur->x1;
          up->middle->left = up->middle->middle;
          LET (up->x1, up->middle->x1);
          LET (up->middle->x1, up->middle->x2);
          return 1;
        }
      }
    } else {
      // now up->middle == cur
      // up: [ (left) x1 cur:[(succ)] x2 (right) ]
      if (IS_2N(up->left)) {
        // up: [ [.s.] x1 cur:[(succ)] x2 (right) ]
        // --> [ [.s.x1 (succ)] x2 (right) ]
        LET (up->left->x2, up->x1);
        up->left->middle = up->left->right;
        up->left->right = succ;
        LET (up->x1, up->x2);
        free_node (cur, Extra_words);
        return 1;
      } else {
        // up: [ [.s.t.] x1 cur:[(succ)] x2 (right) ]
        // --> [ [.s.] t [.x1 (succ)] x2 (right) ]
        LET (cur->x1, up->x1);
        cur->x2 = cur->x1;
        cur->right = succ;
        cur->left = up->left->right;
        up->left->right = up->left->middle;
        LET (up->x1, up->left->x2);
        up->left->x2 = up->left->x1;
        return 1;
      }
    }
  }

  // If we come here, this means that `up` is the root
  // and we want to replace it with "1-node" [ (succ) ]
  // Instead, we decrease the depth by one, and make `succ` the new root

  free_node (up, Extra_words);
  R->root = succ;
  R->depth--;

  return 1;
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
