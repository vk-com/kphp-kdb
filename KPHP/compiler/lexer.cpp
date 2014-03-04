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

#include "lexer.h"
#include "stage.h"
#include "io.h"

#include "keywords_set.hpp"
#include "bicycle.h"

//FILE *F = fopen ("tok", "w");

template <class T> class Singleton {
public:
  static T *instance () {
    if (val == NULL) {
      assert (get_thread_id() == 0);
      val = (T *)(operator new (sizeof (T)));
      new (val) T();
    }
    return val;
  }
private:
  static T* val;
};
template <class T> T *Singleton<T>::val = (T *)NULL;

/***
  LexerData
 ***/
LexerData::LexerData ()
  : code(NULL), tokens(), in_gen_str (false) {
}

LexerData::~LexerData () {
}

void LexerData::new_line() {
  line_num++;
}

void LexerData::set_code (char *new_code, int new_code_len) {
  start = new_code;
  code = new_code;
  code_len = new_code_len;
  code_end = code + code_len;
  line_num = 0;
  new_line();
}

char *LexerData::get_code() {
  return code;
}

void LexerData::pass (int shift) {
  while (shift-- > 0) {
    int c = *code;
    code++;
    if (c == '\n') {
      new_line();
    }
  }
}

void LexerData::pass_raw (int shift) {
  code += shift;
}

void LexerData::add_token_ (Token *tok, int shift) {
  kphp_assert (code + shift <= code_end);
  tok->line_num = line_num;
  tok->debug_str = string_ref (code, code + shift);
  tokens.push_back (tok);
  //fprintf (stderr, "%.*s : %d\n", tok->debug_str.length(), tok->debug_str.begin(), line_num);
  pass (shift);
}

void LexerData::add_token (Token *tok, int shift) {
  kphp_assert (tok != NULL);
  flush_str();
  add_token_ (tok, shift);
}

void LexerData::start_str() {
  in_gen_str = true;
  str_begin = get_code();
  str_cur = (char *)get_code();
}

void LexerData::append_char (int c) {
  if (!in_gen_str) {
    start_str();
  }
  if (c == -1) {
    int c = *get_code();
    *str_cur++ = (char)c;
    if (c == '\n') {
      new_line();
    }
    pass_raw (1);
  } else {
    *str_cur++ = (char)c;
  }
}

void LexerData::flush_str() {
  if (in_gen_str) {
    add_token_ (new Token (tok_str, str_begin, str_cur), 0);
    while (str_cur != get_code()) {
      *str_cur++ = ' ';
    }
    in_gen_str = false;
  }
}

const map <string, string> &config_func() {
  static bool inited = false;
  static map <string, string> to;
  if (!inited) {
    to["attr_wrap"] = "attrWrap";
    to["logout_hash"] = "logoutHash";
    to["videocall_hash"] = "videocallHash";
    to["to_json"] = "convertToJSON";
    inited = true;
  }
  return to;
}

void LexerData::post_process (const string &main_func_name) {
  vector <Token *> oldtokens = tokens;
  tokens.clear();
  if (!main_func_name.empty()) {
    tokens.push_back (new Token (tok_function));
    tokens.push_back (new Token (tok_func_name, string_ref_dup (main_func_name)));
    tokens.push_back (new Token (tok_oppar));
    tokens.push_back (new Token (tok_clpar));
  }
  int n = (int)oldtokens.size();

  int i = 0;

  tokens.push_back (new Token (tok_opbrc));
  while (i < n) {
    TokenType tp = tok_empty;
    if (i + 2 < n && oldtokens[i]->type() == tok_oppar && oldtokens[i + 2]->type() == tok_clpar) {
      switch (oldtokens[i + 1]->type()) {
        case tok_int:
          tp = tok_conv_int;
          break;
        case tok_float:
          tp = tok_conv_float;
          break;
        case tok_string:
          tp = tok_conv_string;
          break;
        case tok_array:
          tp = tok_conv_array;
          break;
        case tok_object:
          tp = tok_conv_object;
          break;
        case tok_bool:
          tp = tok_conv_bool;
          break;
        case tok_var:
          tp = tok_conv_var;
        default:
          break;
      }
    }
    if (tp == tok_empty) {
      if (oldtokens[i]->type() == tok_elseif) {
        tokens.push_back (new Token (tok_else));
        tokens.push_back (new Token (tok_if));
        delete oldtokens[i];
        i++;
      } else if (i + 1 < n && oldtokens[i]->type() == tok_str_begin && oldtokens[i + 1]->type() == tok_str_end) {
        tokens.push_back(new Token (tok_str));
        delete oldtokens[i];
        delete oldtokens[i + 1];
        i += 2;
      } else if (i + 2 < n && oldtokens[i]->type() == tok_str_begin && oldtokens[i + 1]->type() == tok_str && oldtokens[i + 2]->type() == tok_str_end) {
        tokens.push_back(oldtokens[i + 1]);
        delete oldtokens[i];
        delete oldtokens[i + 2];
        i += 3;
      } else if (i + 1 < n && oldtokens[i]->type() == tok_new && oldtokens[i + 1]->type() == tok_func_name) {
        //new Memcached --> new_Memcached
        tokens.push_back (oldtokens[i]);
        tokens.back()->type() = tok_func_name;
        tokens.back()->str_val = string_ref_dup (string ("new_") + string (oldtokens[i + 1]->str_val));
        delete (oldtokens[i + 1]);
        i += 2;
        if (i == n || oldtokens[i]->type() != tok_oppar) {
          tokens.push_back (new Token (tok_oppar));
          tokens.push_back (new Token (tok_clpar));
        }
      } else if (oldtokens[i]->type() == tok_func_name && oldtokens[i]->str_val.eq ("static")) {
        i++;
      } else if (i + 2 < n && oldtokens[i]->type() == tok_new && oldtokens[i + 1]->type() == tok_Exception &&
                 oldtokens[i + 2]->type() == tok_oppar) {
        tokens.push_back (oldtokens[i + 1]);
        tokens.back()->type() = tok_func_name;
        tokens.back()->str_val = string_ref_dup ("new_Exception");
        tokens.push_back (oldtokens[i + 2]);
        tokens.push_back (new Token (tok_file_c));
        tokens.push_back (new Token (tok_comma));
        tokens.push_back (new Token (tok_line_c));
        tokens.back()->line_num = oldtokens[i]->line_num;
        if (i + 3 < n && oldtokens[i + 3]->type() != tok_clpar) {
          tokens.push_back (new Token (tok_comma));
        }
        i += 3;
      } if ((i == 0 || oldtokens[i - 1]->type() != tok_function) && oldtokens[i]->type() == tok_func_name && oldtokens[i + 1]->type() == tok_oppar && (!strncmp (oldtokens[i]->str_val.begin(), "fetch_", 6) || oldtokens[i]->str_val.eq ("err"))) {
        tokens.push_back (oldtokens[i]);
        tokens.push_back (oldtokens[i + 1]);
        tokens.push_back (new Token (tok_file_c));
        tokens.push_back (new Token (tok_comma));
        tokens.push_back (new Token (tok_line_c));
        if (i + 2 < n && oldtokens[i + 2]->type() != tok_clpar) {
          tokens.push_back (new Token (tok_comma));
        }
        i += 2;
      } else if (i + 3 < n && oldtokens[i]->type() == tok_var_name && oldtokens[i]->str_val.eq ("config") && oldtokens[i + 1]->type() == tok_opbrk &&
                 oldtokens[i + 2]->type() == tok_str && config_func().count (oldtokens[i + 2]->str_val) && oldtokens[i + 3]->type() == tok_clbrk &&
                 (i + 4 == n || oldtokens[i + 4]->type() != tok_eq1)) {
        tokens.push_back (new Token (tok_func_name));
        tokens.back()->str_val = string_ref_dup ((config_func().find (string (oldtokens[i + 2]->str_val)))->second);
        i += 4;
      } else if ((i == 0 || oldtokens[i - 1]->type() != tok_function) && oldtokens[i]->type() == tok_func_name &&
          oldtokens[i]->str_val.eq ("requireOnce")) {
        tokens.push_back (oldtokens[i]);
        tokens.back()->type() = tok_require_once;
        i++;
      } else {
        tokens.push_back (oldtokens[i]);
        i++;
      }
    } else {
      tokens.push_back (new Token (tp));
      delete oldtokens[i];
      delete oldtokens[i + 1];
      delete oldtokens[i + 2];
      i += 3;
    }
  }

  tokens.push_back (new Token (tok_clbrc));
  tokens.push_back (new Token (tok_end));
}

void LexerData::move_tokens (vector <Token *> *dest) {
  std::swap (tokens, *dest);
}

int LexerData::get_line_num() {
  return line_num;
}


/***
  TokenLexer
 ***/
//virtual int TokenLexer::parse (LexerData *lexer_data) const = 0;
TokenLexer::TokenLexer() {
}
TokenLexer::~TokenLexer() {
}

int parse_with_helper (LexerData *lexer_data, Helper <TokenLexer> *h) {
  const char *s = lexer_data->get_code();

  TokenLexer *fnd = h->get_help (s);

  int ret;
  if (fnd == NULL || (ret = fnd->parse (lexer_data)) != 0) {
    ret = h->get_default()->parse (lexer_data);
  }

  return ret;
}

TokenLexerError::TokenLexerError (string error_str)
 : error_str (error_str) {
}
TokenLexerError::~TokenLexerError () {
}

int TokenLexerError::parse (LexerData *lexer_data) const {
  stage::set_line (lexer_data->get_line_num());
  kphp_error (0, error_str.c_str());
  return 1;
}

void TokenLexerName::init_static() {
}

int TokenLexerName::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code(), *st = s;
  TokenType type;

  if (s[0] == '$') {
    type = tok_var_name;
    s++;
  } else {
    type = tok_func_name;
  }

  const char *t = s;
  if (is_alpha (t[0])) {
    t++;
    while (is_alphanum (t[0])) {
      t++;
    }
  }

  if (s == t) {
    return TokenLexerError ("Variable name expected").parse (lexer_data);
  }

  string_ref name(s, t);

  if (type == tok_func_name) {
    const KeywordType *tp = KeywordsSet::get_type (name.begin(), name.length());
    if (tp != NULL) {
      lexer_data->add_token (new Token (tp->type), (int)(t - st));
      return 0;
    }
  } else if (type == tok_var_name) {
    if (name.length() == 7 && !strncmp (name.begin(), "GLOBALS", 7)) {
      return TokenLexerError ("$GLOBALS is not supported").parse (lexer_data);
    }
  }

  lexer_data->add_token (new Token (type, name), (int)(t - st));
  return 0;
}

int TokenLexerNum::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code();

  const char *t = s;

  enum {before_dot, after_dot, after_e, after_e_and_sign,
    after_e_and_digit, finish, hex} state = before_dot;
  if (s[0] == '0' && s[1] == 'x') {
    t += 2;
    state = hex;
  }

  bool is_float = false;

  while (*t && state != finish) {
    switch (state) {
      case hex: {
        switch (*t) {
          case '0' ... '9':
          case 'A' ... 'F':
          case 'a' ... 'f':
            t++;
            break;
          default:
            state = finish;
            break;
        }
        break;
      }
      case before_dot: {
        switch (*t) {
          case '0' ... '9': {
            t++;
            break;
          }
          case '.': {
            t++;
            is_float = true;
            state = after_dot;
            break;
          }
          case 'e':
          case 'E': {
            t++;
            is_float = true;
            state = after_e;
            break;
          }
          default: {
            state = finish;
            break;
          }
        }
        break;
      }
      case after_dot: {
        switch (*t) {
          case '0' ... '9': {
            t++;
            break;
          }
          case 'e':
          case 'E': {
            t++;
            if (*t == '+' || *t == '-') {
              t++;
            }
            state = after_e;
            break;
          }
          default: {
            state = finish;
            break;
          }
        }
        break;
      }
      case after_e: {
        switch (*t) {
          case '-':
          case '+': {
            t++;
            state = after_e_and_sign;
            break;
          }
          case '0' ... '9': {
            t++;
            state = after_e_and_digit;
            break;
          }
          default: {
            return TokenLexerError("Bad exponent").parse (lexer_data);
          }
        }
        break;
      }
      case after_e_and_sign: {
        switch (*t) {
          case '0' ... '9': {
            t++;
            state = after_e_and_digit;
            break;
          }
          default: {
            return TokenLexerError("Bad exponent").parse (lexer_data);
          }
        }
        break;
      }
      case after_e_and_digit: {
        switch (*t) {
          case '0' ... '9': {
            t++;
            break;
          }
          default: {
            state = finish;
            break;
          }
        }
        break;
      }

      case finish: {
        assert (0);
      }
    }
  }

  assert (t != s);
  lexer_data->add_token (new Token (is_float ? tok_float_const : tok_int_const, s, t), (int)(t - s));

  return 0;
}


int TokenLexerSimpleString::parse (LexerData *lexer_data) const {
  string str;

  const char *s = lexer_data->get_code(),
    *t = s + 1;

  lexer_data->pass_raw (1);
  bool need = true;
  lexer_data->start_str();
  while (need) {
    switch (t[0]) {
      case 0:
        return TokenLexerError ("Unexpected end of file").parse (lexer_data);
        continue;

      case '\'':
        need = false;
        t++;
        continue;

      case '\\':
        if (t[1] == '\\') {
          t += 2;
          lexer_data->append_char ('\\');
          lexer_data->pass_raw (2);
          continue;
        } else if (t[1] == '\'') {
          t += 2;
          lexer_data->append_char ('\'');
          lexer_data->pass_raw (2);
          continue;
        }
        break;

      default:
        break;
    }
    lexer_data->append_char (-1);
    t++;
  }
  lexer_data->flush_str();
  lexer_data->pass_raw (1);
  return 0;
}

TokenLexerAppendChar::TokenLexerAppendChar (int c, int pass)
  : c(c), pass(pass) {
  }

int TokenLexerAppendChar::parse (LexerData *lexer_data) const {
  lexer_data->append_char (c);
  lexer_data->pass_raw (pass);
  return 0;
}

int TokenLexerOctChar::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code(), *t = s;
  int val = t[1] - '0';
  t += 2;

  int add = conv_oct_digit (*t);
  if (add != -1) {
    val = (val << 3) + add;

    add = conv_oct_digit (*++t);
    if (add != -1) {
      val = (val << 3) + add;
      t++;
    }
  }

  //TODO: \777
  lexer_data->append_char (val);
  lexer_data->pass_raw ((int)(t - s));
  return 0;
}

int TokenLexerHexChar::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code(), *t = s + 2;
  int val = conv_hex_digit (*t);
  if (val == -1) {
    return TokenLexerError("It is not hex char").parse (lexer_data);
  }

  int add = conv_hex_digit (*++t);
  if (add != -1) {
    val = (val << 4) + add;
    t++;
  }

  lexer_data->append_char (val);
  lexer_data->pass_raw ((int)(t - s));
  return 0;
}


Helper <TokenLexer> *TokenLexerStringExpr::gen_helper() {
  //fprintf (stderr, "Create TokenLexerStringExpr helper\n");
  Helper <TokenLexer> *h = new Helper <TokenLexer> (new TokenLexerError ("Can't parse"));

  h->add_rule ("\'", Singleton <TokenLexerSimpleString>::instance());
  h->add_rule ("\"", Singleton <TokenLexerString>::instance());
  h->add_rule ("[a-zA-Z_$]", Singleton <TokenLexerName>::instance());

  //TODO: double (?)
  h->add_rule ("[0-9]|.[0-9]", Singleton <TokenLexerNum>::instance());

  h->add_rule (" |\t|\n|\r", Singleton <TokenLexerSkip>::instance());
  h->add_rule ("", Singleton <TokenLexerCommon>::instance());

  return h;
}
TokenLexerStringExpr::TokenLexerStringExpr() : h (NULL) {
}
void TokenLexerStringExpr::init() {
  assert (h == NULL);
  h = gen_helper();
}
TokenLexerStringExpr::~TokenLexerStringExpr() {
  delete h;
}
int TokenLexerStringExpr::parse (LexerData *lexer_data) const {
  assert (h != NULL);
  const char *s = lexer_data->get_code();
  assert (*s == '{');
  lexer_data->add_token (new Token (tok_expr_begin), 1);

  int bal = 0;
  while (true) {
    const char *s = lexer_data->get_code();

    if (*s == 0) {
      return TokenLexerError("Unexpected end of file").parse (lexer_data);
    } else if (*s == '{') {
      bal++;
    } else if (*s == '}') {
      if (bal == 0) {
        lexer_data->add_token (new Token (tok_expr_end), 1);
        break;
      }
      bal--;
    }

    int res = parse_with_helper (lexer_data, h);
    if (res) {
      return res;
    }
  }
  return 0;
}


void TokenLexerString::add_esc (string s, char c) {
  h->add_simple_rule (s, new TokenLexerAppendChar (c, (int)s.size()));
}
void TokenLexerHeredocString::add_esc (string s, char c) {
  h->add_simple_rule (s, new TokenLexerAppendChar (c, (int)s.size()));
}

Helper <TokenLexer> *TokenLexerString::gen_helper() {
  h = new Helper <TokenLexer> (new TokenLexerAppendChar (-1, 0));

  add_esc ("\\f", '\f');
  add_esc ("\\n", '\n');
  add_esc ("\\r", '\r');
  add_esc ("\\t", '\t');
  add_esc ("\\v", '\v');
  add_esc ("\\$", '$');
  add_esc ("\\\\", '\\');
  add_esc ("\\\"", '\"');

  h->add_rule ("\\[0-7]", Singleton <TokenLexerOctChar>::instance());
  h->add_rule ("\\x[0-9A-Fa-f]", Singleton <TokenLexerHexChar>::instance());

  h->add_rule ("$[A-Za-z_]", Singleton <TokenLexerName>::instance());
  h->add_rule ("{$", Singleton <TokenLexerStringExpr>::instance());

  return h;
}

Helper <TokenLexer> *TokenLexerHeredocString::gen_helper() {
  h = new Helper <TokenLexer> (new TokenLexerAppendChar (-1, 0));

  add_esc ("\\f", '\f');
  add_esc ("\\n", '\n');
  add_esc ("\\r", '\r');
  add_esc ("\\t", '\t');
  add_esc ("\\v", '\v');
  add_esc ("\\$", '$');
  add_esc ("\\\\", '\\');

  h->add_rule ("\\[0-7]", Singleton <TokenLexerOctChar>::instance());
  h->add_rule ("\\x[0-9A-Fa-f]", Singleton <TokenLexerHexChar>::instance());

  h->add_rule ("$[A-Za-z]", Singleton <TokenLexerName>::instance());
  h->add_rule ("{$", Singleton <TokenLexerStringExpr>::instance());

  return h;
}

TokenLexerString::TokenLexerString() : h (NULL) {
}
void TokenLexerString::init() {
  assert (h == NULL);
  h = gen_helper();
}
TokenLexerString::~TokenLexerString() {
  delete h;
}

TokenLexerHeredocString::TokenLexerHeredocString() : h (NULL) {
}
void TokenLexerHeredocString::init() {
  assert (h == NULL);
  h = gen_helper();
}
TokenLexerHeredocString::~TokenLexerHeredocString() {
  delete h;
}

int TokenLexerString::parse (LexerData *lexer_data) const {
  assert (h != NULL);
  const char *s = lexer_data->get_code();
  int is_heredoc = s[0] == '<';
  assert (!is_heredoc);

  lexer_data->add_token (new Token (tok_str_begin), 1);

  while (true) {
    const char *s = lexer_data->get_code();
    if (*s == '\"') {
      lexer_data->add_token (new Token (tok_str_end), 1);
      break;
    }

    if (*s == 0) {
      return TokenLexerError("Unexpected end of file").parse (lexer_data);
    }

    int res = parse_with_helper (lexer_data, h);
    if (res) {
      return res;
    }
  }
  return 0;
}

int TokenLexerHeredocString::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code();
  int is_heredoc = s[0] == '<';
  assert (is_heredoc);

  string tag;
  const char *st = s;
  assert (s[1] == '<' && s[2] == '<');

  s += 3;

  while (s[0] == ' ') {
    s++;
  }

  while (is_alpha (s[0])) {
    tag += *s++;
  }

  if (tag.empty()) {
    return TokenLexerError ("TAG expected").parse (lexer_data);
  }
  if (s[0] != '\n') {
    return TokenLexerError ("'\\n' expected").parse (lexer_data);
  }
  s++;

  lexer_data->add_token (new Token (tok_str_begin), (int)(s - st));
  assert (s == lexer_data->get_code());

  bool first = true;
  while (true) {
    const char *s = lexer_data->get_code();
    const char *t = s, *st = s;
    if (t[0] == '\n' || first) {
      t += t[0] == '\n';
      if (!strncmp (t, tag.c_str(), tag.size())) {
        t += tag.size();

        int semicolon = 0;
        if (t[0] == ';') {
          t++;
          semicolon = 1;
        }
        if (t[0] == '\n' || t[0] == 0) {
          lexer_data->add_token (new Token (tok_str_end), (int)(t - st - semicolon));
          break;
        }
      }
    }

    if (*s == 0) {
      return TokenLexerError("Unexpected end of file").parse (lexer_data);
    }

    int res = parse_with_helper (lexer_data, h);
    if (res) {
      return res;
    }
    first = false;
  }
  return 0;
}

int TokenLexerComment::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code(),
   *st = s;

  assert (s[0] == '/' || s[0] == '#');
  if (!strncmp ("#break_file", s, 11)) {
    lexer_data->add_token (new Token (tok_break_file), 0);
  }
  if (s[0] == '#' || s[1] == '/') {
    while (s[0] && s[0] != '\n') {
      s++;
    }
  } else {
    s += 2;
    while (s[0] && (s[0] != '*' || s[1] != '/')) {
      s++;
    }
    if (s[0]) {
      s += 2;
    } else {
      return TokenLexerError ("Unclosed comment (*/ expected)").parse (lexer_data);
    }
  }

  lexer_data->pass ((int)(s - st));
  return 0;
}

int TokenLexerIfndefComment::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code(),
   *st = s;

  assert (strncmp (s, "#ifndef KittenPHP", 17) == 0);
  s += 17;
  while (s[0] && strncmp (s, "\n#endif", 7)) {
    s++;
  }
  if (s[0]) {
    s += 7;
  } else {
    return TokenLexerError ("Unclosed comment (#endif expected)").parse (lexer_data);
  }
  lexer_data->pass ((int)(s - st));
  return 0;
}

TokenLexerWithHelper::TokenLexerWithHelper() : h (NULL) {
}

TokenLexerWithHelper::~TokenLexerWithHelper() {
  delete h;
}

void TokenLexerWithHelper::init () {
  assert (h == NULL);
  h = gen_helper();
}

int TokenLexerWithHelper::parse (LexerData *lexer_data) const {
  assert (h != NULL);
  return parse_with_helper(lexer_data, h);
}

TokenLexerToken::TokenLexerToken (TokenType tp, int len)
  : tp (tp), len (len) {
}

int TokenLexerToken::parse (LexerData *lexer_data) const {
  lexer_data->add_token (new Token(tp), len);
  return 0;
}

void TokenLexerCommon::add_rule (Helper <TokenLexer> *h, string str, TokenType tp) {
  h->add_simple_rule (str, new TokenLexerToken (tp, (int)str.size()) );
}

Helper <TokenLexer> *TokenLexerCommon::gen_helper() {
  Helper <TokenLexer> *h = new Helper <TokenLexer> (new TokenLexerError ("No <common token> found"));

  add_rule (h, ":::", tok_triple_colon);
  add_rule (h, "::<", tok_triple_lt);
  add_rule (h, "::>", tok_triple_gt);
  add_rule (h, "::=", tok_triple_eq);

  add_rule (h, "=", tok_eq1);
  add_rule (h, "==", tok_eq2);
  add_rule (h, "===", tok_eq3);
  add_rule (h, "<>", tok_neq_lg);
  add_rule (h, "!=", tok_neq2);
  add_rule (h, "!==", tok_neq3);
  add_rule (h, "<", tok_lt);
  add_rule (h, "<=", tok_le);
  add_rule (h, ">", tok_gt);
  add_rule (h, ">=", tok_ge);

  add_rule (h, "(", tok_oppar);
  add_rule (h, ")", tok_clpar);
  add_rule (h, "[", tok_opbrk);
  add_rule (h, "]", tok_clbrk);
  add_rule (h, "{", tok_opbrc);
  add_rule (h, "}", tok_clbrc);
  add_rule (h, ":", tok_colon);
  add_rule (h, ";", tok_semicolon);
  add_rule (h, ".", tok_dot);
  add_rule (h, ",", tok_comma);

  add_rule (h, "++", tok_inc);
  add_rule (h, "--", tok_dec);
  add_rule (h, "+", tok_plus);
  add_rule (h, "-", tok_minus);
  add_rule (h, "*", tok_times);
  add_rule (h, "/", tok_divide);

  add_rule (h, "@", tok_at);

  add_rule (h, "%", tok_mod);
  add_rule (h, "&", tok_and);
  add_rule (h, "|", tok_or);
  add_rule (h, "^", tok_xor);
  add_rule (h, "~", tok_not);
  add_rule (h, "!", tok_log_not);
  add_rule (h, "?", tok_question);

  add_rule (h, "<<", tok_shl);
  add_rule (h, ">>", tok_shr);
  add_rule (h, "+=", tok_set_add);
  add_rule (h, "-=", tok_set_sub);
  add_rule (h, "*=", tok_set_mul);
  add_rule (h, "/=", tok_set_div);
  add_rule (h, "%=", tok_set_mod);
  add_rule (h, "&=", tok_set_and);
  add_rule (h, "&&", tok_log_and);
  add_rule (h, "|=", tok_set_or);
  add_rule (h, "||", tok_log_or);
  add_rule (h, "^=", tok_set_xor);
  add_rule (h, ".=", tok_set_dot);
  add_rule (h, ">>=", tok_set_shr);
  add_rule (h, "<<=", tok_set_shl);

  add_rule (h, "=>", tok_double_arrow);
  add_rule (h, "::", tok_double_colon);
  add_rule (h, "->", tok_arrow);
  add_rule (h, "...", tok_varg);

  return h;
}

TokenLexerCommon::TokenLexerCommon() {
}

TokenLexerSkip::TokenLexerSkip (int n)
  : n (n) {
}

int TokenLexerSkip::parse (LexerData *lexer_data) const {
  lexer_data->pass (n);
  return 0;
}

Helper <TokenLexer> *TokenLexerPHP::gen_helper() {
  Helper <TokenLexer> *h = new Helper <TokenLexer> (new TokenLexerError ("Can't parse"));

  h->add_rule ("/*|//|#", Singleton <TokenLexerComment>::instance());
  h->add_rule ("#ifndef KittenPHP", Singleton <TokenLexerIfndefComment>::instance());
  h->add_rule ("\'", Singleton <TokenLexerSimpleString>::instance());
  h->add_rule ("\"", Singleton <TokenLexerString>::instance());
  h->add_rule ("<<<", Singleton <TokenLexerHeredocString>::instance());
  h->add_rule ("[a-zA-Z_$]", Singleton <TokenLexerName>::instance());

  h->add_rule ("[0-9]|.[0-9]", Singleton <TokenLexerNum>::instance());

  h->add_rule (" |\t|\n|\r", Singleton <TokenLexerSkip>::instance());
  h->add_rule ("", Singleton <TokenLexerCommon>::instance());

  return h;
}

TokenLexerPHP::TokenLexerPHP() {
}

TokenLexerGlobal::TokenLexerGlobal() {
  php_lexer = Singleton <TokenLexerPHP>::instance();
  assert (php_lexer != NULL);
}

int TokenLexerGlobal::parse (LexerData *lexer_data) const {
  const char *s = lexer_data->get_code();
  const char *t = s;
  while (*t && strncmp (t, "<?", 2)) {
     t++;
  }

  if (s != t) {
    lexer_data->add_token (new Token (tok_inline_html, s, t), (int)(t - s));
    return 0;
  }

  s = t;
  if (*s == 0) {
    return TokenLexerError("End of file").parse (lexer_data);
  }

  int d = 2;
  if (!strncmp (s + d, "php", 3)) {
    d += 3;
  }
  lexer_data->pass (d);


  while (true) {
    char *s = lexer_data->get_code();
    char *t = s;
    while (t[0] == ' ' || t[0] == '\t') {
      t++;
    }
    lexer_data->pass_raw ((int)(t - s));
    s = t;
    if (s[0] == 0 || (s[0] == '?' && s[1] == '>')) {
      break;
    }
    int ret = php_lexer->parse (lexer_data);
    if (ret != 0) {
      return ret;
    }
  }
  lexer_data->add_token (new Token (tok_semicolon), 0);
  if (*lexer_data->get_code()) {
    lexer_data->pass (2);
  }
  char c = *lexer_data->get_code();
  if (c == '\n') {
    lexer_data->pass (1);
  } else if (c == ' ') {
    while (*lexer_data->get_code() == ' ') {
      lexer_data->pass (1);
    }
  }

  return 0;
}

TokenLexerGlobal::~TokenLexerGlobal() {
}

void lexer_init() {
  Singleton <TokenLexerCommon>::instance()->init();
  Singleton <TokenLexerStringExpr>::instance()->init();
  Singleton <TokenLexerString>::instance()->init();
  Singleton <TokenLexerHeredocString>::instance()->init();
  Singleton <TokenLexerPHP>::instance()->init();
  config_func();
}

int php_text_to_tokens (char *text, int text_length, const string &main_func_name, vector <Token *> *result) {
  static TokenLexerGlobal lexer;

  LexerData lexer_data;
  lexer_data.set_code (text, text_length);

  while (true) {
    char c = *lexer_data.get_code();
    if (c == 0) {
      break;
    }
    int ret = lexer.parse (&lexer_data);
    kphp_error_act (ret == 0, "failed to parse", return ret);
  }

  lexer_data.post_process (main_func_name);
  lexer_data.move_tokens (result);
  return 0;
}

