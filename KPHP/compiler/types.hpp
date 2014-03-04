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

/*** PrimitiveType ***/
#define PTYPE_NAME_(id) (#id + 3)
#define PTYPE_NAME(id) PTYPE_NAME_(id)

//define ptype_name specialization for each PrimitiveType

template <PrimitiveType T_ID> const char *ptype_name() {
  return NULL;
}

#define PTYPE_NAME_FUNC(id) template <> inline const char *ptype_name <id> () {return PTYPE_NAME (id);}
#define FOREACH_PTYPE(tp) PTYPE_NAME_FUNC (tp);
#include "foreach_ptype.h"
#undef PTYPE_NAME_FUNC

/*** Key ***/
inline bool operator < (const Key &a, const Key &b) {
  return a.id < b.id;
}
inline bool operator > (const Key &a, const Key &b) {
  return a.id > b.id;
}
inline bool operator <= (const Key &a, const Key &b) {
  return a.id <= b.id;
}
inline bool operator != (const Key &a, const Key &b) {
  return a.id != b.id;
}
inline bool operator == (const Key &a, const Key &b) {
  return a.id == b.id;
}

/*** TypeData ***/
bool operator < (const TypeData::KeyValue &a, const TypeData::KeyValue &b) {
  return a.first < b.first;
}
