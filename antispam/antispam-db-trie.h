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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 * Created: 16.03.2012
 */

#pragma once

#include "antispam-db.h"

// type-0 : ch != 0, next == 0  there are no edges
// type-1 : ch != 0, next != 0  there is only one edge "ch"
// type-2 : ch == 0, next != 0  there are many edges

typedef struct trie_node trie_node_t;
struct trie_node {
  void *next;
  trie_node_t *parent;
  unsigned long long used;
  int patterns_cnt;
  byte ch, p_ch, deg; // p_ch = char to parent, 0 <= deg <= 255
};
