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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <aspell.h>
#include <regex.h>
#include "stemmer.h"
#include "word-split.h"
#include "server-functions.h"
#include "kdb-data-common.h"

int yo_hack = 0;
long long yo_hack_stat[2], check_word_stat[2];
struct speller {
  AspellConfig *config;
  AspellSpeller *spell_checker;
  struct speller *next;
  long long stat[2][2];
  char *name;
  char *code;
  char *jargon;
  int russian;
};
int use_aspell_suggestion = 0;
int spellers;
static struct speller **SC;
static struct speller **SS;
static int sort_idx;

static regex_t re_email, re_url;
static const char *email_regexp = "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[a-z]{2,4}";
static const char *url_regexp = "(ftp|https?)://([0-9a-zA-Z.-]+)\\.([a-z]{2,3})[/0-9A-Za-z._?;&=+-]*";

int spell_get_dicts (char *buf, int len) {
  int i;
  char *p = buf;
  char *e = buf + len - 2;
  for (i = 0; i < spellers; i++) {
    int o = e - p;
    if (o < 5) {
      break ;
    }
    int l = snprintf (p, o, "%s\t%s\t%lld\t%lld\t%lld\t%lld\n", SC[i]->code, SC[i]->jargon, SC[i]->stat[0][0], SC[i]->stat[0][1], SC[i]->stat[1][0], SC[i]->stat[1][1]);
    if (l < 0 || l >= o) {
      *p = 0;
      break;
    }
    p += l;
  }
  return p - buf;
}

static int cmp_spellers (const void *a, const void *b) {
  const struct speller **x = (const struct speller **) a, **y = (const struct speller **) b;
  long double u = (*x)->stat[sort_idx][0];
  u *= (*y)->stat[sort_idx][1];
  long double v = (*y)->stat[sort_idx][0];
  v *= (*x)->stat[sort_idx][1];
  if (u > v) {
    return -1;
  }
  if (u < v) {
    return 1;
  }
  return 0;
}

void spell_reoder_spellers (void) {
  sort_idx = 0;
  qsort (SC, spellers, sizeof (SC[0]), cmp_spellers);
  sort_idx = 1;
  qsort (SS, spellers, sizeof (SS[0]), cmp_spellers);
}

void spell_done (void) {
  int i;
  for (i = 0; i < spellers; i++) {
    delete_aspell_speller (SS[i]->spell_checker);
    delete_aspell_config (SS[i]->config);
  }
  regfree (&re_email);
  regfree (&re_url);
}

static int vk_aspell_config_replace (AspellConfig *c, const char *key, const char *value) {
  if (!aspell_config_replace (c, key, value)) {
    vkprintf (1, "aspell_config_replace (%p, %s, %s) fail.\n", c, key, value);
    vkprintf (1, "%s\n", aspell_config_error_message (c));
    return 0;
  }
  return 1;
}

static void filter_re (char *text, regex_t *re) {
  regmatch_t pmatch;
  size_t nmatch = 1;
  while (!regexec (re, text, nmatch, &pmatch, 0)) {
    int j;
    for (j = pmatch.rm_so; j < pmatch.rm_eo; j++) {
      text[j] = ' ';
    }
    text += pmatch.rm_eo;
  }
}

static int vk_regcomp (regex_t *preg, const char *regex, int cflags) {
  char err[16384];
  int r = regcomp (preg, regex, cflags);
  if (r) {
    int l = regerror (r, preg, err, sizeof (err) - 1);
    kprintf ("%.*s\n", l, err);
  }
  return r;
}
void spell_init (void) {
  int r = vk_regcomp (&re_email, email_regexp, REG_EXTENDED | REG_ICASE | REG_NEWLINE);
  if (r) {
    kprintf ("regcomp (%s) return error code %d\n.", email_regexp, r);
    exit (1);
  }
  r = vk_regcomp (&re_url, url_regexp, REG_EXTENDED | REG_ICASE | REG_NEWLINE);
  if (r) {
    kprintf ("regcomp (%s) return error code %d\n.", email_regexp, r);
    exit (1);
  }

  struct speller *head = NULL;
  spellers = 0;

  init_is_letter ();
  l_case[0xa8] = l_case[0xb8] = 0xb8;

  const AspellDictInfo *entry;
  AspellConfig *config = new_aspell_config ();
  AspellDictInfoList *dlist = get_aspell_dict_info_list (config);
  AspellDictInfoEnumeration *dels = aspell_dict_info_list_elements (dlist);
  while ((entry = aspell_dict_info_enumeration_next (dels)) != 0) {
    struct speller *w;
    for (w = head; w != NULL; w = w->next) {
      if (!strcmp (w->code, entry->code) && !strcmp (w->jargon, entry->jargon)) {
        break;
      }
    }
    if (w != NULL) {
      vkprintf (1, "skip duplicate dictionary (code:%s, jargon:%s)\n", entry->code, entry->jargon);
      continue;
    }

    AspellConfig *c = new_aspell_config ();
    if (!vk_aspell_config_replace (c, "lang", entry->code) ||
        !vk_aspell_config_replace (c, "jargon", entry->jargon) ||
        !vk_aspell_config_replace (c, "encoding", "CP1251")) {
      delete_aspell_config (c);
      continue;
    }
    if (use_aspell_suggestion && (!vk_aspell_config_replace (c, "filter", "url") ||
                                  !vk_aspell_config_replace (c, "sug-mode", "fast"))) {
      delete_aspell_config (c);
      continue;
    }
    AspellCanHaveError *possible_err = new_aspell_speller (c);
    if (aspell_error_number (possible_err) != 0) {
      kprintf ("%s\n", aspell_error_message (possible_err));
      delete_aspell_can_have_error (possible_err);
      delete_aspell_config (c);
      continue;
    } else {
      struct speller *P = zmalloc0 (sizeof (struct speller));
      P->config = c;
      P->spell_checker = to_aspell_speller (possible_err);
      P->name = zstrdup (entry->name);
      P->code = zstrdup (entry->code);
      P->jargon = zstrdup (entry->jargon);
      P->russian = !strncmp (entry->name, "ru", 2);
      P->next = head;
      head = P;
      spellers++;
      vkprintf (1, "user dict: %s\n", P->name);
    }
  }
  delete_aspell_dict_info_enumeration (dels);
  delete_aspell_config (config);
  SC = zmalloc (spellers * sizeof (struct speller));
  SS = zmalloc (spellers * sizeof (struct speller));
  int i = 0;
  while (head != NULL) {
    SC[i] = SS[i] = head;
    head = head->next;
    i++;
  }
}

int check_word (char *word, int len) {
  int i;
  for (i = 0; i < spellers; i++) {
    int r = aspell_speller_check (SC[i]->spell_checker, word, len);
    if (r >= 0) {
      SC[i]->stat[0][1]++;
      if (r) {
        SC[i]->stat[0][0]++;
        return 1;
      }
    }
  }
  
  if (yo_hack) {
    int j;
    yo_hack_stat[1]++;
    for (j = 0; j < len; j++) {
      if ((unsigned char) word[j] == 0xe5) {
        word[j] = 0xb8;
        for (i = 0; i < spellers; i++) {
          if (SC[i]->russian && aspell_speller_check (SC[i]->spell_checker, word, len) == 1) {            
            word[j] = 0xe5;
            yo_hack_stat[0]++;
            return 1;
          }
        }
        word[j] = 0xe5;
      }
    }
  }

  if (!use_aspell_suggestion) {
    return 0;
  }
  int class = get_str_class (word, len);
  for (i = 0; i < spellers; i++) {
    SS[i]->stat[1][1]++;
    const AspellWordList *suggestions = aspell_speller_suggest (SS[i]->spell_checker, word, len);
    if (suggestions) {
      AspellStringEnumeration *elements = aspell_word_list_elements (suggestions);
      const char *w;
      int words = 0;
      while ( (w = aspell_string_enumeration_next (elements)) != NULL ) {
        vkprintf (3, "suggest: %s, %.*s (%s)\n", w, len, word, SS[i]->name);
        if (class != get_str_class (w, strlen (w))) {
          continue;
        }
        words++;
        break;
      }
      delete_aspell_string_enumeration (elements);
      if (words) {
        SS[i]->stat[1][0]++;
        return 0;
      }
    } else {
      vkprintf (3, "suggetsions = NULL. %s\n", aspell_speller_error_message (SS[i]->spell_checker));
    }
  }
  return -1;
}

int spell_check2 (const char *text, int res[3]) {
  int l = strlen (text);
  res[0] = res[1] = res[2] = 0;
  dyn_mark_t m;
  dyn_mark (m);
  char *a = zmalloc (l + 3), *b = zmalloc (l + 3);
  strcpy (a, text);
  int i, words = 0;
  for (i = 0; i < spellers; i++) {
    words = 0;
    vkprintf (3, "%s (%s)\n",  a, SC[i]->name);
    char *p = b;
    AspellCanHaveError *ret = new_aspell_document_checker (SC[i]->spell_checker);
    if (aspell_error (ret) != 0) {
      vkprintf (1, "%s\n",aspell_error_message (ret));
      return -1;
    }
    AspellDocumentChecker *checker = to_aspell_document_checker (ret);
    aspell_document_checker_process (checker, a, -1);
    while (1) {
      AspellToken token = aspell_document_checker_next_misspelling (checker);
      if (!token.len) {
        break;
      }
      vkprintf (3, "%.*s (%s)\n", token.len, a + token.offset, SC[i]->name);
      memcpy (p, a + token.offset, token.len);
      p += token.len;
      *p++ = ' ';
      words++;
    }
    vkprintf (3, "words = %d\n", words);
    *p = 0;
    p = a; a = b; b = p;
    delete_aspell_document_checker (checker);
    if (!words) {
      break;
    }
  }
  dyn_release (m);
  res[0] = l;
  res[1] = words;
  return 0;
}

int spell_check (char *original_text, int res[3], int destroy_original) {
  static char buff[16384];
  memset (res, 0, sizeof (res));
  char *text;
  dyn_mark_t m;
  if (!destroy_original) {
    dyn_mark (m);
    text = zmalloc (strlen (original_text) + 1);
    strcpy (text, original_text);
  } else {
    text = original_text;
  }
  vkprintf (3, "Before filter: %s\n", text);
  filter_re (text, &re_email);
  filter_re (text, &re_url);
  vkprintf (3, "After filter: %s\n", text);

  while (*text) {
    int wl = get_notword (text);
    if (wl < 0) {
      break;
    }
    text += wl;
    wl = get_word (text);
    if (!wl) {
      continue;
    }
    assert (wl > 0 && wl < 16383);
    my_lc_str (buff, text, wl);
    if (wl >= 3) {
      int l = my_lc_str (buff, text, wl);
      int r = check_word (buff, l);
      vkprintf (3, "[%d] check_word: %.*s\n", r, l, buff);
      if (r >= 0) {
        check_word_stat[1]++;
        if (r) {
          res[0]++;
          check_word_stat[0]++;
        } else {
          res[1]++;
        }
      } else {
        res[2]++;
      }
    }
    text += wl;
  }
  if (!destroy_original) {
    dyn_release (m);
  }
  return 0;
}
