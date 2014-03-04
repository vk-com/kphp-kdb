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
              2012-2013 Anton Maydell
*/

#ifndef __TL_SERIALIZE_H__
#define __TL_SERIALIZE_H__

#include "tl-parser.h"
#include "tl-scheme.h"

//int#a8509bda ? = Int;
#define CODE_int 0xa8509bda
//long#22076cba ? = Long;
#define CODE_long 0x22076cba
//string#b5286e24 ? = String;
#define CODE_string 0xb5286e24
//double#2210c154 ? = Double;
#define CODE_double 0x2210c154

//vector # [ X ] = Vector X;
#define CODE_vector 0x389dd8fa

int tl_serialize (struct tl_compiler *C, struct tl_scheme_object *expressions, int *out, int olen);

/*
  Append to the buffer's end serialization of single expression from input
  section_mask: 1 - search in types section
  section_mask: 2 - search in functions section
  Returns number of ints read.
*/
int tl_unserialize (struct tl_compiler *C, struct tl_buffer *b, int *input, int ilen, int section_mask);

int tl_serialize_rpc_function_call (struct tl_compiler *C, const char *const text, int *out, int olen, char **result_typename);
int tl_expression_unserialize (struct tl_compiler *C, int *input, int ilen, int section_mask, const char *type_name, struct tl_scheme_object **R);
int tl_unserialize_rpc_function_result (struct tl_compiler *C, struct tl_buffer *b, int *input, int ilen, char *result_typename, int indentation);


#endif

