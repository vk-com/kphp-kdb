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

#include "server-functions.h"
#include "search-index-layout.h"
#include "stemmer-new.h"
#include "word-split.h"
#include "listcomp.h"

int universal = 0, hashtags_enabled = 0, use_stemmer = 0, wordfreqs_enabled = 0, creation_date = 1, tag_owner = 0;

int get_cls_flags (void) {
  int r = 0;
  if (hashtags_enabled) { r |= FLAG_CLS_ENABLE_TAGS; }
  if (use_stemmer) { r |= FLAGS_CLS_USE_STEMMER; }
  if (universal) { r |= FLAGS_CLS_ENABLE_UNIVERSE; }
  if (wordfreqs_enabled) { r |= FLAGS_CLS_ENABLE_ITEM_WORDS_FREQS; }
  if (creation_date) { r |= FLAGS_CLS_CREATION_DATE; }
  if (word_split_utf8) { r |= FLAGS_CLS_WORD_SPLIT_UTF8; }
  if (tag_owner) { r |= FLAGS_CLS_TAG_OWNER; }
  return r;
}


int check_header (struct search_index_header *H) {
  if (H->command_line_switches_flags != get_cls_flags ()) {
    kprintf ("Index header command line switches not equal given command line switches. Index flags %08x, current flags %08x\n", H->command_line_switches_flags, get_cls_flags ());
    return 0;
  }
  if (H->stemmer_version != stemmer_version) {
    kprintf ("Header stemmer version = %d, stemmer_version = %d\n", H->stemmer_version, stemmer_version);
    return 0;
  }
  if (H->word_split_version != word_split_version) {
    kprintf ("Header word split version = %d, word split version = %d\n", H->word_split_version, word_split_version);
    return 0;
  }
  if (!check_listcomp_version (H->listcomp_version)) {
    kprintf ("Header listcomp version = %x, listcomp version = %x\n", H->listcomp_version, listcomp_version);
    return 0;
  }
  return 1;
}
