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

inline vector <string> expand_template_ (const string &s) {
  vector <string> res (1, "");

  int si = 0, sn = (int)s.size();

  while (si < sn) {
    string to_append;
    if (s[si] == '[') {
      si++;

      while (si < sn && s[si] != ']') {
        int l, r;
        if (si + 1 < sn && s[si + 1] == '-') {
          assert (si + 2 < sn);

          l = s[si], r = s[si + 2];
          assert (l < r);

          si += 3;
        } else {
          l = r = s[si];

          si += 1;
        }

        for (int c = l; c <= r; c++) {
          to_append += (char)c;
        }
      }
      assert (si < sn);
      si++;

    } else {
      to_append += s[si];
      si++;
    }

    int n = (int)res.size();
    for (int i = 0; i < n; i++) {
      for (int j = 1; j < (int)to_append.size(); j++) {
        res.push_back(res[i] + to_append[j]);
      }
      res[i] += to_append[0];
    }
  }

  return res;
}
inline vector <string> expand_template (const string &s) {
  vector <string> v;

  int sn = (int)s.size();

  string cur = "";
  for (int i = 0; i < sn; i++) {
    if (s[i] == '|') {
      v.push_back (cur);
      cur = "";
    } else {
      cur += s[i];
    }
  }
  v.push_back (cur);

  vector <string> res;
  int vn = (int)v.size();
  for (int i = 0; i < vn; i++) {
    vector <string> tmp = expand_template_ (v[i]);

    int tmpn = (int)tmp.size();
    for (int i = 0; i < tmpn; i++) {
      res.push_back (tmp[i]);
    }
  }

  return res;
}

template <typename T> Helper<T>::Helper (T* on_fail) : on_fail (on_fail) {
  assert (on_fail != NULL);
}

template <typename T> void Helper<T>::add_rule (const string &rule_template, T *val, bool need_expand) {
  vector <string> v = need_expand ? expand_template (rule_template) : vector <string> (1, rule_template);

  for (int i = 0; i < (int)v.size(); i++) {
    trie.add (v[i], val);
  }

}

template <typename T> void Helper<T>::add_simple_rule (const string &rule_template, T *val) {
  add_rule (rule_template, val, false);
}

template <typename T> T *Helper<T>::get_default() {
  return on_fail;
}

template <typename T> T *Helper<T>::get_help (const char *s) {
  T **best = trie.get_deepest (s);
  if (best == NULL) {
    return NULL;
  } else {
    return *best;
  }

}
