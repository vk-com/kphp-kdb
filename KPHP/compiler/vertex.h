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
#include "stage.h"
#include "data_ptr.h"
#include "graph.h"
#include "types.h"
#include "type-inferer-core.h"

size_t vertex_total_mem_used __attribute__ ((weak));

#define FOREACH_VERTEX(v, i) for (__typeof (all (v)) i = all (v); !i.empty(); i.next())


inline VertexPtr &get_empy_vertex() {
  static VertexPtr res;
  res = VertexPtr();
  return res;
}
VertexPtr clone_vertex (VertexPtr from);

template <>
class vertex_inner <meta_op_base> {
public:
  typedef VertexPtr value_type;
  typedef value_type *xiterator;
  typedef const value_type *const_xiterator;
  typedef std::reverse_iterator <xiterator> iterator;
  typedef std::reverse_iterator <const_xiterator> const_iterator;
  //typedef vector <VertexPtr>::iterator iterator;

  //Data
  int id;
private:
  Operation type_;
public:
  tinf::ExprNode tinf_node;
  VertexPtr type_rule;
  Location location;

  OperationExtra extra_type : 4;
  PrimitiveType type_help : 5;
  RLValueType rl_type : 2;
  RLValueType val_ref_flag : 2;
  ConstValueType const_type : 2;
  int ref_flag : 1;
  bool auto_flag : 1;
  bool varg_flag : 1;
  bool throws_flag : 1;
  bool parent_flag : 1;

  int n;

  vertex_inner():
    id (0),
    type_ (op_none),
    tinf_node (VertexPtr (this)),
    type_rule(),
    location(),
    extra_type (op_ex_none),
    type_help(),
    rl_type (val_error),
    val_ref_flag (val_none),
    const_type (cnst_error_),
    ref_flag (0),
    auto_flag(),
    varg_flag(),
    throws_flag(),
    parent_flag(),
    n (-1) {
  }

  vertex_inner (const vertex_inner <meta_op_base> &from) :
    id (from.id),
    type_ (from.type_),
    tinf_node (VertexPtr (this)),
    type_rule (from.type_rule),
    location (from.location),
    extra_type (from.extra_type),
    type_help (from.type_help),
    rl_type (from.rl_type),
    val_ref_flag (from.val_ref_flag),
    const_type (from.const_type),
    ref_flag (from.ref_flag),
    auto_flag (from.auto_flag),
    varg_flag (from.varg_flag),
    throws_flag (from.throws_flag),
    parent_flag (from.parent_flag),
    n (-1) {
  }
  virtual ~vertex_inner() {
  }


  VertexPtr *arr() const {
    return (VertexPtr *)this - 1;
  }
  void raw_init (int real_n) {
    assert (n == -1);
    n = real_n;
    for (int i = 0; i < n; i++) {
      new (&arr()[-i]) VertexPtr();
    }
  }
  void raw_copy (const vertex_inner <meta_op_base> &from) {
    assert (n == -1);
    n = from.size();
    for (int i = 0; i < n; i++) {
      new (&arr()[-i]) VertexPtr (clone_vertex (from.ith (i)));
    }
  }

  inline bool check_range (int i) const {
    return 0 <= i && i < size();
  }
  inline VertexPtr &operator[] (int i) {
    assert (check_range (i));
    return arr()[-i];
  }
  inline const VertexPtr &operator[] (int i) const {
    assert (check_range (i));
    return arr()[-i];
  }
  inline int size() const {
    return n;
  }
  VertexPtr &back() {
    return (*this)[size() - 1];
  }

  vector <VertexPtr> get_next() {
    vector <VertexPtr> res(begin(), end());
    return res;
  }

  bool empty() {
    return size() == 0;
  }
  inline VertexPtr &ith (int i) {
    return (*this)[i];
  }
  inline const VertexPtr &ith (int i) const {
    return (*this)[i];
  }

  inline iterator begin() {
    return iterator (arr() + 1);
  }
  inline iterator end() {
    return iterator (arr() - size() + 1);
  }
  inline const_iterator begin() const {
    return const_iterator (arr() + 1);
  }
  inline const_iterator end() const {
    return const_iterator (arr() - size() + 1);
  }

  const Location &get_location() {
    return location;
  }


  void init() {
  }

  static void init_properties (OpProperties *p) {
  }

  const Operation &type() const {return type_;}

  virtual const FunctionPtr &get_func_id() const {dl_fail ("get_func_id is not supported");}
  virtual void set_func_id (FunctionPtr func_ptr) {dl_fail ("set_func_id is not supported");}

  virtual const VarPtr &get_var_id() const {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}
  virtual void set_var_id (const VarPtr &) {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}

  virtual const DefinePtr &get_define_id() const {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}
  virtual void set_define_id (const DefinePtr &) {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}

  virtual const string &get_string() const {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}
  virtual void set_string (const string &) {dl_fail (dl_pstr ("not supported [%d:%s]", type_, OpInfo::str (type_).c_str()));}

  template <Operation Op> friend
    vertex_inner <Op> *raw_create_vertex_inner (int args_n, vertex_inner <Op> *from_ptr);
};

inline bool operator == (const VertexPtr &a, const VertexPtr &b) {
  return a->id == b->id;
}

inline bool operator < (const VertexPtr &a, const VertexPtr &b) {
  return a->id < b->id;
}

template <Operation Op>
int get_index (const VertexAdaptor <Op> &v) {
  return v->id;
}

template <Operation Op>
void set_index (VertexAdaptor <Op> *v, int id) {
  (*v)->id = id;
}

template <Operation Op>
class vertex_inner : public vertex_inner <meta_op_base> {
};

inline void set_location (VertexPtr v, const Location &location) {
  v->location = location;
}
typedef vertex_inner <meta_op_base> Vertex;
typedef Range <Vertex::iterator> VertexRange;
typedef Range <Vertex::const_iterator> ConstVertexRange;

inline VertexRange all (VertexPtr &v) {
  return all (*v);
}
inline ConstVertexRange all (const VertexPtr &v) {
  return all (const_cast <const Vertex &> (*v));
}
inline VertexRange all (VertexRange range) {
  return range;
}

template <Operation Op> size_t vertex_inner_size (int args_n) {
  return sizeof (vertex_inner <Op>) + sizeof (VertexPtr) * args_n;
}
template <Operation Op> size_t vertex_inner_shift (int args_n) {
  return sizeof (VertexPtr) * args_n;
}

template <Operation Op>
vertex_inner <Op> *raw_create_vertex_inner (int args_n, vertex_inner <Op> *from_ptr = NULL) {
  size_t size = vertex_inner_size <Op> (args_n);
  size_t shift = vertex_inner_shift <Op> (args_n);
  PROF (vertex_inner).alloc_memory (size);
  PROF (vertex_inner_data).alloc_memory (size - shift);

  vertex_inner <Op> *ptr = (vertex_inner <Op> *) (
      (char *)malloc (size) + shift
  );
  if (from_ptr == NULL) {
    new (ptr) vertex_inner <Op>();
  } else {
    new (ptr) vertex_inner <Op> (*from_ptr);
  }
  ptr->raw_init (args_n);
  ptr->type_ = Op;
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *raw_clone_vertex_inner (const vertex_inner <Op> &from) {
  size_t size = vertex_inner_size <Op> ((int)from.size());
  size_t shift = vertex_inner_shift <Op> ((int)from.size());
  PROF (vertex_inner).alloc_memory (size);
  PROF (vertex_inner_data).alloc_memory (size - shift);

  vertex_inner <Op> *ptr = (vertex_inner <Op> *) (
      (char *)malloc (size) + shift
  );
  new (ptr) vertex_inner <Op> (from);
  ptr->raw_copy (from);
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner(vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (0, (vertex_inner <Op> *)from_ptr);
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner (VertexPtr first, vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (1, (vertex_inner <Op> *)from_ptr);
  (*ptr)[0] = first;
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner (VertexPtr first, VertexPtr second, vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (2, (vertex_inner <Op> *)from_ptr);
  (*ptr)[0] = first;
  (*ptr)[1] = second;
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner (VertexPtr first, VertexPtr second,
    VertexPtr third, vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (3, (vertex_inner <Op> *)from_ptr);
  (*ptr)[0] = first;
  (*ptr)[1] = second;
  (*ptr)[2] = third;
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner (VertexPtr first, VertexPtr second, VertexPtr third,
    VertexPtr forth, vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (4, (vertex_inner <Op> *)from_ptr);
  (*ptr)[0] = first;
  (*ptr)[1] = second;
  (*ptr)[2] = third;
  (*ptr)[3] = forth;
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *create_vertex_inner (const vector <VertexPtr> &next,
    vertex_inner <meta_op_base> *from_ptr = NULL) {
  vertex_inner <Op> *ptr = raw_create_vertex_inner <Op> (
      (int)next.size(),
      (vertex_inner <Op> *)from_ptr
  );
  for (int i = 0, ni = (int)next.size(); i < ni; i++) {
    ptr->ith (i) = next[i];
  }
  return ptr;
}

template <Operation Op>
vertex_inner <Op> *clone_vertex_inner (const vertex_inner <Op> &from) {
  vertex_inner <Op> *ptr = raw_clone_vertex_inner <Op> (from);
  return ptr;
}

VertexPtr create_vertex (Operation op, VertexPtr first);
VertexPtr create_vertex (Operation op, VertexPtr first, VertexPtr second);

#define CLONE_VERTEX(name, op, from)\
  VertexAdaptor <op> name = reg_vertex (VertexPtr (clone_vertex_inner <op> (*from)))
#define CREATE_VERTEX(name, op, args...)\
  VertexAdaptor <op> name = reg_vertex (VertexPtr (create_vertex_inner <op> (args)))
#define COPY_CREATE_VERTEX(name, from, op, args...)\
  VertexAdaptor <op> name = reg_vertex (VertexPtr (create_vertex_inner <op> (args, &*from)))

#define CREATE_META_VERTEX_1(name, meta_op, op, first)\
  VertexAdaptor <meta_op> name = create_vertex (op, first)
#define CREATE_META_VERTEX_2(name, meta_op, op, first, second)\
  VertexAdaptor <meta_op> name = create_vertex (op, first, second)

#define VA_BEGIN(Op, BaseOp) \
  template<>\
  class vertex_inner <Op> : public vertex_inner <BaseOp> {\
  public:\
    typedef vertex_inner <BaseOp> super;\
    typedef vertex_inner <Op> self;
#define VA_END };

#define PROPERTIES_BEGIN\
  static void init_properties (OpProperties *p) {\
    super::init_properties (p);
#define PROPERTIES_END }
#define OPP(at, x) p->at = x

#define RAW_INIT_BEGIN\
  void raw_init (int args_n) {\
    super::raw_init (args_n);
#define RAW_INIT_END }
#define RAW_COPY_BEGIN\
  void raw_copy (const self &from) {\
    super::raw_copy (from);
#define RAW_COPY_END }

#define VA_BINARY_BEGIN(op) VA_BEGIN (op, meta_op_binary_op) PROPERTIES_BEGIN
#define VA_BINARY_END PROPERTIES_END VA_END
#define VA_UNARY_BEGIN(op) VA_BEGIN (op, meta_op_unary_op) PROPERTIES_BEGIN
#define VA_UNARY_END PROPERTIES_END VA_END

#define TMP_OP(Op, i) DL_ADD_SUFF (DL_ADD_SUFF (Op, tmp), i)
#define VA_BEGIN_1(Op, BaseOp, arg1)\
  VA_BEGIN (TMP_OP (Op, 0), BaseOp)\
    FIELD_BODY (arg1, Op)\
  VA_END\
  VA_BEGIN (Op, TMP_OP (Op, 0))\

#define VA_BEGIN_2(Op, BaseOp, arg1, arg2)\
  VA_BEGIN (TMP_OP (Op, 1), BaseOp)\
    FIELD_BODY (arg1, Op)\
  VA_END\
  VA_BEGIN_1 (Op, TMP_OP (Op, 1), arg2)\

#define FIELD_BODY(field_name, Op) DL_ADD_SUFF (FIELD_BODY, field_name) (Op)

#define FIELD_BODY_function(Op)\
  private:\
  FunctionPtr func_;\
  public:\
  const FunctionPtr &get_func_id() const {return func_;}\
  void set_func_id (FunctionPtr func_ptr) {func_ = func_ptr;}


#define FIELD_BODY_variable(Op)\
  private:\
  VarPtr var_;\
  public:\
  const VarPtr &get_var_id() const {return var_;}\
  void set_var_id (const VarPtr &var) {var_ = var;}\
  RAW_COPY_BEGIN\
    var_ = from.var_;\
  RAW_COPY_END

#define FIELD_BODY_define(Op)\
  private:\
  DefinePtr define_;\
  public:\
  const DefinePtr &get_define_id() const {return define_;}\
  void set_define_id (const DefinePtr &define) {define_ = define;}\
  RAW_COPY_BEGIN\
    define_ = from.define_;\
  RAW_COPY_END

#define FIELD_BODY_string(Op)\
  public:\
  string str_val;\
  virtual const string &get_string() const {return str_val;}\
  virtual void set_string (const string &s) {str_val = s;}\
  RAW_COPY_BEGIN\
    str_val = from.str_val;\
  RAW_COPY_END


#define FIELD_BODY_labels(Op)\
  public:\
  int break_label_id;\
  int continue_label_id;\
  RAW_INIT_BEGIN\
    break_label_id = 0;\
    continue_label_id = 0;\
  RAW_INIT_END\
  RAW_COPY_BEGIN\
    break_label_id = from.break_label_id;\
    continue_label_id = from.continue_label_id;\
  RAW_COPY_END

#define FIELD_BODY_int(Op)\
  public:\
  union {\
    int int_val;\
    int param_cnt;\
  };\
  RAW_INIT_BEGIN\
   int_val = 0;\
  RAW_INIT_END\
  RAW_COPY_BEGIN\
    int_val = 0;\
  RAW_COPY_END

VA_BEGIN (meta_op_unary_, meta_op_base)
  VertexPtr &expr() {return ith (0);}
VA_END

VA_BEGIN (meta_op_unary_op, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (main, expression_opp);
    OPP (minor, operator_opp);
    OPP (arity, unary_opp);
    OPP (fixity, left_opp); //may be redefined
  PROPERTIES_END
VA_END

VA_BEGIN (meta_op_binary_, meta_op_base)
  VertexPtr &lhs() {return ith (0);}
  VertexPtr &rhs() {return ith (1);}
VA_END

VA_BEGIN (meta_op_binary_op, meta_op_binary_)
  PROPERTIES_BEGIN
    OPP (main, expression_opp);
    OPP (minor, operator_opp);
    OPP (arity, binary_opp);
    OPP (fixity, left_opp); //may be redefined
  PROPERTIES_END
VA_END

VA_BEGIN (meta_op_varg_, meta_op_base)
  VertexRange args() {return all (*this);}
VA_END

//VA_BEGIN (op_func_call, meta_op_base)
  //void *func_;
  //RAW_INIT_BEGIN
    //func_ = NULL;
  //RAW_INIT_END
  //PROPERTIES_BEGIN
    //OPP (main, statement_opp);
  //PROPERTIES_END
  //void* &func() {return func_;}
//VA_END

//VA_BEGIN (op_int_const, meta_op_base)
  //string str_;
  //RAW_INIT_BEGIN
    //new (&str_) string();
  //RAW_INIT_END
  //PROPERTIES_BEGIN
    //OPP (main, expression_opp);
    //OPP (minor, term_opp);
  //PROPERTIES_END
  //string &value() {return str_;}
//VA_END

//VA_BINARY_BEGIN (op_add)
  //OPP (math, arithmetic_opp);
  //OPP (fixity, left_opp);
  //OPP (str, "+");
  //OPP (rl, rl_op);
  //OPP (cnst, cnst_const_func);
//VA_BINARY_END


VA_BINARY_BEGIN (op_set_shl)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "<<=");
  OPP (base_op, op_shl);
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_set_shl");
  }

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_add)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "+=");
  OPP (base_op, op_add);
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_set_add");
  }

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_sub)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "-=");
  OPP (base_op, op_sub);
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_set_sub");
  }

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_mul)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "*=");
  OPP (base_op, op_mul);
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_set_mul");
  }

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_shl)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<<");
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_shl");
  }
VA_BINARY_END

VA_BINARY_BEGIN (op_add)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "+");
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_add");
  }
VA_BINARY_END

VA_BINARY_BEGIN (op_sub)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "-");
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_sub");
  }
VA_BINARY_END

VA_BINARY_BEGIN (op_mul)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "*");
  if (use_safe_integer_arithmetic) {
    OPP (type, binary_func_op);
    OPP (str, "safe_mul");
  }
VA_BINARY_END


VA_UNARY_BEGIN (op_conv_int)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$intval");
  if (use_safe_integer_arithmetic) {
    OPP (str, "f$safe_intval");
  }
VA_UNARY_END


VA_BINARY_BEGIN (op_set_div)
  OPP (type, binary_func_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "divide_self");
  OPP (base_op, op_div);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_mod)
  OPP (type, binary_func_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "modulo_self");
  OPP (base_op, op_mod);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_div)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "divide");
VA_BINARY_END

VA_BINARY_BEGIN (op_mod)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "modulo");
VA_BINARY_END


VA_BINARY_BEGIN (op_set)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "=");

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_and)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "&=");
  OPP (base_op, op_and);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_or)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "|=");
  OPP (base_op, op_or);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_xor)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "^=");
  OPP (base_op, op_xor);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_shr)
  OPP (type, binary_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, ">>=");
  OPP (base_op, op_shr);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END

VA_BINARY_BEGIN (op_set_dot)
  OPP (type, binary_func_op);
  OPP (rl, rl_set);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "append");
  OPP (base_op, op_concat);

  OPP (fixity, right_opp); //redefined
VA_BINARY_END


VA_BINARY_BEGIN (op_log_or_let)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "||");
VA_BINARY_END

VA_BINARY_BEGIN (op_log_and_let)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "&&");
VA_BINARY_END

VA_BINARY_BEGIN (op_log_xor_let)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "^");
VA_BINARY_END


VA_BINARY_BEGIN (op_log_or)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "||");
VA_BINARY_END

VA_BINARY_BEGIN (op_log_and)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "&&");
VA_BINARY_END

VA_BINARY_BEGIN (op_or)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "|");
VA_BINARY_END

VA_BINARY_BEGIN (op_xor)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "^");
VA_BINARY_END

VA_BINARY_BEGIN (op_and)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "&");
VA_BINARY_END

VA_BINARY_BEGIN (op_eq2)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "eq2");
VA_BINARY_END

VA_BINARY_BEGIN (op_neq2)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "neq2");
VA_BINARY_END

VA_BINARY_BEGIN (op_lt)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<");
VA_BINARY_END

VA_BINARY_BEGIN (op_gt)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, ">");
VA_BINARY_END

VA_BINARY_BEGIN (op_le)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<=");
VA_BINARY_END

VA_BINARY_BEGIN (op_ge)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, ">=");
VA_BINARY_END

VA_BINARY_BEGIN (op_shr)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, ">>");
VA_BINARY_END

VA_BINARY_BEGIN (op_arrow)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "@op_arrow");
VA_BINARY_END

VA_BEGIN (op_double_arrow, meta_op_binary_op)
  PROPERTIES_BEGIN
    OPP (type, binary_op);
    OPP (rl, rl_op);
    OPP (cnst, cnst_const_func);
    OPP (str, "@op_double_arrow");
    OPP (fixity, right_opp); //redefined
  PROPERTIES_END
  VertexPtr &key() {return lhs();}
  VertexPtr &value() {return rhs();}

VA_END

VA_BINARY_BEGIN (op_eq3)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "equals");
VA_BINARY_END

VA_BINARY_BEGIN (op_neq3)
  OPP (type, binary_func_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "not_equals");
VA_BINARY_END

VA_BINARY_BEGIN (op_concat)
  OPP (type, binary_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<TODO: op_concat>");
VA_BINARY_END

VA_BEGIN (op_ternary, meta_op_base)
  PROPERTIES_BEGIN
    OPP (main, expression_opp);
    OPP (type, ternary_op);
    OPP (rl, rl_op);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: ternary>");
    OPP (description, "ternary operator");

    OPP (fixity, right_opp); //redefined
  PROPERTIES_END
  VertexPtr &cond() {return ith (0);}
  VertexPtr &true_expr() {return ith (1);}
  VertexPtr &false_expr() {return ith (2);}
VA_END

VA_UNARY_BEGIN (op_not)
  OPP (type, prefix_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "~");
VA_UNARY_END

VA_UNARY_BEGIN (op_log_not)
  OPP (type, prefix_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "!");
VA_UNARY_END

VA_UNARY_BEGIN (op_minus)
  OPP (type, prefix_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, " - ");
VA_UNARY_END

VA_UNARY_BEGIN (op_plus)
  OPP (type, prefix_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, " + ");
VA_UNARY_END

VA_UNARY_BEGIN (op_noerr)
  OPP (type, common_op);
  OPP (rl, rl_other);
  OPP (cnst, cnst_const_func);
  OPP (str, "NOERR");
VA_UNARY_END

VA_UNARY_BEGIN (op_prefix_inc)
  OPP (type, prefix_op);
  OPP (rl, rl_op_l);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "++");
  if (use_safe_integer_arithmetic) {
    OPP (type, conv_op);
    OPP (str, "safe_incr_pre");
  }
VA_UNARY_END

VA_UNARY_BEGIN (op_prefix_dec)
  OPP (type, prefix_op);
  OPP (rl, rl_op_l);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "--");
  if (use_safe_integer_arithmetic) {
    OPP (type, conv_op);
    OPP (str, "safe_decr_pre");
  }
VA_UNARY_END

VA_UNARY_BEGIN (op_addr)
  OPP (type, prefix_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<TODO: op_addr>");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_float)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$floatval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_string)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$strval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_array)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$arrayval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_array_l)
  OPP (type, common_op);
  OPP (rl, rl_other);
  OPP (cnst, cnst_const_func);
  OPP (str, "<TODO: op_conv_array_l>");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_int_l)
  OPP (type, common_op);
  OPP (rl, rl_other);
  OPP (cnst, cnst_const_func);
  OPP (str, "<TODO: op_conv_int_l>");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_object)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "<TODO: op_conv_object>");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_bool)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$boolval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_var)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "var");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_uint)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$uintval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_long)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$longval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_ulong)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "f$ulongval");
VA_UNARY_END

VA_UNARY_BEGIN (op_conv_regexp)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_const_func);
  OPP (str, "regexp");
VA_UNARY_END

VA_UNARY_BEGIN (op_postfix_inc)
  OPP (type, postfix_op);
  OPP (rl, rl_op_l);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "++");
  if (use_safe_integer_arithmetic) {
    OPP (type, conv_op);
    OPP (str, "safe_incr_post");
  }
VA_UNARY_END

VA_UNARY_BEGIN (op_postfix_dec)
  OPP (type, postfix_op);
  OPP (rl, rl_op_l);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "--");
  if (use_safe_integer_arithmetic) {
    OPP (type, conv_op);
    OPP (str, "safe_decr_post");
  }
VA_UNARY_END


VA_BEGIN_1 (meta_op_num, meta_op_base, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_const_func);
  PROPERTIES_END
VA_END
VA_BEGIN (op_int_const, meta_op_num)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_int>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_uint_const, meta_op_num)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_uint>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_long_const, meta_op_num)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_long>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_ulong_const, meta_op_num)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_ulong>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_float_const, meta_op_num)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_float>");
  PROPERTIES_END
VA_END
VA_BEGIN_1 (op_string, meta_op_base, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: op_string>");
  PROPERTIES_END
VA_END
VA_BEGIN_1 (op_func_name, meta_op_base, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: op_func_name>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_string_build, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_op);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: op_string_build>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_false, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: todo1>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_true, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: todo2>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_null, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: todo3>");
  PROPERTIES_END
VA_END
VA_BEGIN_2 (op_var, meta_op_base, variable, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_var);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: op_var>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_index, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_index);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: op_index>");
  PROPERTIES_END
  VertexPtr &array() {return ith (0);}
  bool has_key() {return check_range (1);}
  VertexPtr &key() {return ith (1);}
VA_END

VA_BEGIN (meta_op_push_back, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_error);
    OPP (cnst, cnst_error);
  PROPERTIES_END
  VertexPtr &array() {return ith (0);}
  VertexPtr &value() {return ith (1);}
VA_END
VA_BEGIN (op_push_back_return, meta_op_push_back)
  PROPERTIES_BEGIN
    OPP (str, "push_back_return");
  PROPERTIES_END
VA_END
VA_BEGIN (op_push_back, meta_op_push_back)
  PROPERTIES_BEGIN
    OPP (str, "push_back");
  PROPERTIES_END
VA_END

VA_BEGIN (op_set_value, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_error);
    OPP (cnst, cnst_error);
    OPP (str, "set_value");
  PROPERTIES_END
  VertexPtr &array() {return ith (0);}
  VertexPtr &key() {return ith (1);}
  VertexPtr &value() {return ith (2);}
VA_END

VA_BEGIN_2 (op_func_call, meta_op_varg_, function, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_func);
    OPP (str, "<TODO: op_func_call>");
  PROPERTIES_END
VA_END
VA_BEGIN_2 (op_func_ptr, meta_op_base, function, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_error);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: op_func_ptr>");
  PROPERTIES_END
VA_END
VA_BEGIN_1 (op_define_val, meta_op_base, define)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_const);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: op_define_val>");
  PROPERTIES_END
VA_END
VA_BEGIN (meta_op_define, meta_op_base)
  VertexPtr &name() {return ith(0);}
  VertexPtr &value() {return ith(1);}
VA_END
VA_BEGIN (op_define, meta_op_define)
VA_END
VA_BEGIN (op_define_raw, meta_op_define)
VA_END

VA_BEGIN_1 (meta_op_cycle, meta_op_base, labels)
  PROPERTIES_BEGIN
    OPP (type, cycle_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
  PROPERTIES_END
VA_END
VA_BEGIN (op_while, meta_op_cycle)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: while>");
  PROPERTIES_END
  VertexPtr &cond() {return ith (0);}
  VertexPtr &cmd() {return ith (1);}
VA_END
VA_BEGIN (op_do, meta_op_cycle)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: do>");
  PROPERTIES_END
  VertexPtr &cond() {return ith (0);}
  VertexPtr &cmd() {return ith (1);}
VA_END
VA_BEGIN (op_for, meta_op_cycle)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: for>");
  PROPERTIES_END
  VertexPtr &pre_cond() {return ith (0);}
  VertexPtr &cond() {return ith (1);}
  VertexPtr &post_cond() {return ith (2);}
  VertexPtr &cmd() {return ith (3);}
VA_END
VA_BEGIN (op_foreach, meta_op_cycle)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: foreach>");
  PROPERTIES_END
  VertexPtr &params() {return ith (0);}
  VertexPtr &cmd() {return ith (1);}
VA_END
VA_BEGIN (op_switch, meta_op_cycle)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: switch>");
  PROPERTIES_END
  VertexPtr &expr() {return ith (0);}
  VertexRange cases() {return VertexRange (begin() + 1, end());}
VA_END

VA_BEGIN (meta_op_require, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (cnst, cnst_not_func);
  PROPERTIES_END
VA_END
VA_BEGIN (op_require, meta_op_require)
  PROPERTIES_BEGIN
    OPP (rl, rl_func);
    OPP (str, "<TODO: require>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_require_once, meta_op_require)
  PROPERTIES_BEGIN
    OPP (rl, rl_common);
    OPP (str, "<TODO: require_once>");
  PROPERTIES_END
VA_END

VA_BEGIN (op_if, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: if>");
  PROPERTIES_END
  VertexPtr &cond() {return ith (0);}
  VertexPtr &true_cmd() {return ith (1);}
  bool has_false_cmd() {return check_range (2);}
  VertexPtr &false_cmd() {return ith (2);}
VA_END
VA_BEGIN (op_else, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: else>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_elseif, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: elseif>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_return, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: return>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_try, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: try>");
  PROPERTIES_END
  VertexPtr &try_cmd() {return ith (0);}
  VertexPtr &exception() {return ith (1);}
  VertexPtr &catch_cmd() {return ith (2);}
VA_END
VA_BEGIN (op_array, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: array>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_list, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: list>");
  PROPERTIES_END
  VertexPtr &array() {
    return ith (size() - 1);
  }
  VertexRange list() {
    return VertexRange (begin(), end() - 1);
  }
VA_END
VA_BEGIN (op_global, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: global>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_static, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: static>");
  PROPERTIES_END
VA_END
VA_BEGIN_1 (meta_op_goto, meta_op_unary_, int)
VA_END
VA_BEGIN (op_break, meta_op_goto)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: break>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_continue, meta_op_goto)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: continue>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_echo, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: echo>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_dbg_echo, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: dbg_echo>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_throw, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: throw>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_var_dump, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: var_dump>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_print, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: print>");
  PROPERTIES_END
VA_END
VA_BEGIN_2 (meta_op_function, meta_op_base, function, string)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
  PROPERTIES_END
  VertexPtr &name() {return ith (0);}
  VertexPtr &params() {return ith (1);}
VA_END
VA_BEGIN (op_function, meta_op_function)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: function>");
  PROPERTIES_END
  //virtual has_cmd() {return true;}
  VertexPtr &cmd() {return ith (2);}
VA_END
VA_BEGIN (op_extern_func, meta_op_function)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_extern_func>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_func_decl, meta_op_function)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: op_extern_func>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_func_param_list, meta_op_varg_)
  VertexRange params() {return args();}
  VertexPtr &ith_param (int i) {return ith (i);}
VA_END
VA_BEGIN (meta_op_func_param, meta_op_base)
  VertexPtr &var() {return ith (0);}
  bool has_default() {return size() > 1;}
  VertexPtr &default_value() {return ith (1);}
VA_END
VA_BEGIN_1 (op_func_param_callback, meta_op_func_param, int)
VA_END
VA_BEGIN (op_func_param, meta_op_func_param)
VA_END

VA_BEGIN (op_class, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: class>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_seq, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: seq>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_seq_comma, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_const_func);
    OPP (str, "<TODO: seq_comma>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_case, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: case>");
  PROPERTIES_END
  VertexPtr &expr() {return ith (0);}
  VertexPtr &cmd() {return ith (1);}
VA_END
VA_BEGIN (op_default, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: op_default>");
  PROPERTIES_END
  VertexPtr &cmd() {return ith (0);}
VA_END

VA_BEGIN (op_defined, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: defined>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_pack, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: pack>");
  PROPERTIES_END
VA_END
VA_BEGIN (meta_op_xprintf, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_nonconst_func);
  PROPERTIES_END
VA_END
VA_BEGIN (op_printf, meta_op_xprintf)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: printf>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_sprintf, meta_op_xprintf)
  PROPERTIES_BEGIN
    OPP (str, "<TODO: sprintf>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_store_many, meta_op_unary_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: store_many>");
  PROPERTIES_END
VA_END

VA_BEGIN (meta_op_builtin_func, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_func);
    OPP (cnst, cnst_const_func);
  PROPERTIES_END
VA_END

VA_BEGIN (op_min, meta_op_builtin_func)
  PROPERTIES_BEGIN
    OPP (str, "f$min");
  PROPERTIES_END
VA_END

VA_BEGIN (op_max, meta_op_builtin_func)
  PROPERTIES_BEGIN
    OPP (str, "f$max");
  PROPERTIES_END
VA_END

VA_BEGIN (op_empty, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: empty>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_foreach_param, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_common);
    OPP (cnst, cnst_not_func);
    OPP (str, "<TODO: op_foreach_param>");
  PROPERTIES_END
  VertexPtr &xs() {return ith (0);}
  VertexPtr &x() {return ith (1);}
  bool has_key() {return check_range (2);}
  VertexPtr &key() {return ith (2);}
VA_END
VA_UNARY_BEGIN (op_exit)
  OPP (type, conv_op);
  OPP (rl, rl_op);
  OPP (cnst, cnst_nonconst_func);
  OPP (str, "f$exit");
VA_UNARY_END
VA_BEGIN (meta_op_xset, meta_op_varg_)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (cnst, cnst_nonconst_func);
  PROPERTIES_END
VA_END
VA_BEGIN (op_isset, meta_op_xset)
  PROPERTIES_BEGIN
    OPP (rl, rl_op);
    OPP (str, "<TODO: isset>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_unset, meta_op_xset)
  PROPERTIES_BEGIN
    OPP (rl, rl_common);
    OPP (str, "<TODO: unset>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_lvalue_null, meta_op_base)
  PROPERTIES_BEGIN
    OPP (type, common_op);
    OPP (rl, rl_var);
    OPP (cnst, cnst_nonconst_func);
    OPP (str, "<TODO: op_lvalue_null>");
  PROPERTIES_END
VA_END
VA_BEGIN (op_varg, meta_op_unary_)
VA_END

VA_BEGIN (meta_op_type_rule, meta_op_unary_)
VA_END
VA_BEGIN (op_common_type_rule, meta_op_type_rule)
VA_END
VA_BEGIN (op_lt_type_rule, meta_op_type_rule)
VA_END
VA_BEGIN (op_gt_type_rule, meta_op_type_rule)
VA_END
VA_BEGIN (op_eq_type_rule, meta_op_type_rule)
VA_END
VA_BEGIN (op_type_rule, meta_op_varg_)
VA_END
VA_BEGIN_1 (op_type_rule_func, meta_op_varg_, string)
VA_END
VA_BEGIN_1 (op_arg_ref, meta_op_base, int)
VA_END

VA_BEGIN (op_new, meta_op_unary_)
VA_END


inline VertexRange get_function_params (VertexAdaptor <meta_op_function> func) {
  return func->params().as <op_func_param_list>()->params();
}
