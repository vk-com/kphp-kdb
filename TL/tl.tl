---types---
/////
//
// Serialized binary TL-schema in vkext format
//
/////

tls.schema_v2 version:int date:int types_num:# types:types_num*[tls.Type] constructor_num:# constructors:constructor_num*[tls.Combinator] functions_num:# functions:functions_num*[tls.Combinator] = tls.Schema;

tls.type name:int id:string constructors_num:int flags:int arity:int params_type:long = tls.Type;
tls.combinator name:int id:string type_name:int left:tls.CombinatorLeft right:tls.CombinatorRight = tls.Combinator;
tls.combinatorLeftBuiltin = tls.CombinatorLeft;
tls.combinatorLeft args_num:# args:args_num*[tls.Arg] = tls.CombinatorLeft;

tls.combinatorRight value:tls.TypeExpr = tls.CombinatorRight;

tls.arg id:string flags:# var_num:flags.1?int exist_var_num:flags.2?int exist_var_bit:flags.2?int type:tls.TypeExpr = tls.Arg;

tls.exprType _:tls.TypeExpr = tls.Expr;
tls.exprNat _:tls.NatExpr = tls.Expr;

tls.natConst value:int = tls.NatExpr;
tls.natVar dif:int var_num:int = tls.NatExpr;

tls.typeVar var_num:int flags:int = tls.TypeExpr;
tls.array multiplicity:tls.NatExpr args_num:# args:args_num*[tls.Arg] = tls.TypeExpr;
tls.typeExpr name:int flags:int children_num:# children:children_num*[tls.Expr] = tls.TypeExpr;
---functions---
