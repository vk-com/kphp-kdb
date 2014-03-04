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

template <typename T> Trie<T>::Trie () {
  memset (next, 0, sizeof (next));
  has_val = 0;
}

template <typename T> void Trie<T>::add (const string &s, const T &val) {
  Trie *cur = this;

  for (int i = 0; i < (int)s.size(); i++) {
    int c = (unsigned char)s[i];

    if (cur->next[c] == NULL) {
      cur->next[c] = new Trie();
    }
    cur = cur->next[c];
  }

  cur->val = val;
  assert (cur->has_val == 0);
  cur->has_val = 1;
}

template <typename T> T *Trie<T>::get_deepest (const char *s) {
  T *best = NULL;
  Trie <T> *cur = this;

  while (cur) {
    if (cur->has_val) {
      best = &cur->val;
    }
    if (*s == 0) {
      break;
    }
    cur = cur->next[(unsigned char)*s++];
  }

  return best;
}


template <typename T> void Trie<T>::clear() {
  for (int i = 0; i < 256; i++) {
    delete next[i];
  }
}

template <typename T> Trie<T>::~Trie() {
  clear();
}
