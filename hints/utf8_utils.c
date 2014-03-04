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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#include "utf8_utils.h"

#define HASH_MUL 999983
const int _s_1_[] = {97, 0};
const long long _h_1_ = 999983ll, _a_1_ = 1072ll;
const int _s_2_[] = {98, 0};
const long long _h_2_ = 999983ll, _a_2_ = 1073ll;
const int _s_3_[] = {99, 0};
const long long _h_3_ = 999983ll, _a_3_ = 1082ll;
const int _s_4_[] = {99, 104, 0};
const long long _h_4_ = 999983ll, _a_4_ = 1095ll;
const int _s_5_[] = {100, 0};
const long long _h_5_ = 999983ll, _a_5_ = 1076ll;
const int _s_6_[] = {101, 0};
const long long _h_6_ = 999983ll, _a_6_ = 1077ll;
const int _s_7_[] = {101, 105, 0};
const long long _h_7_ = 999966000289ll, _a_7_ = 1076982772ll;
const int _s_8_[] = {101, 121, 0};
const long long _h_8_ = 999966000289ll, _a_8_ = 1076982772ll;
const int _s_9_[] = {102, 0};
const long long _h_9_ = 999983ll, _a_9_ = 1092ll;
const int _s_10_[] = {103, 0};
const long long _h_10_ = 999983ll, _a_10_ = 1075ll;
const int _s_11_[] = {104, 0};
const long long _h_11_ = 999983ll, _a_11_ = 1093ll;
const int _s_12_[] = {105, 0};
const long long _h_12_ = 999983ll, _a_12_ = 1080ll;
const int _s_13_[] = {105, 97, 0};
const long long _h_13_ = 999966000289ll, _a_13_ = 1079982743ll;
const int _s_14_[] = {105, 121, 0};
const long long _h_14_ = 999966000289ll, _a_14_ = 1079982721ll;
const int _s_15_[] = {106, 0};
const long long _h_15_ = 999983ll, _a_15_ = 1081ll;
const int _s_16_[] = {106, 111, 0};
const long long _h_16_ = 999983ll, _a_16_ = 1105ll;
const int _s_17_[] = {106, 117, 0};
const long long _h_17_ = 999983ll, _a_17_ = 1102ll;
const int _s_18_[] = {106, 97, 0};
const long long _h_18_ = 999983ll, _a_18_ = 1103ll;
const int _s_19_[] = {107, 0};
const long long _h_19_ = 999983ll, _a_19_ = 1082ll;
const int _s_20_[] = {107, 104, 0};
const long long _h_20_ = 999983ll, _a_20_ = 1093ll;
const int _s_21_[] = {108, 0};
const long long _h_21_ = 999983ll, _a_21_ = 1083ll;
const int _s_22_[] = {109, 0};
const long long _h_22_ = 999983ll, _a_22_ = 1084ll;
const int _s_23_[] = {110, 0};
const long long _h_23_ = 999983ll, _a_23_ = 1085ll;
const int _s_24_[] = {111, 0};
const long long _h_24_ = 999983ll, _a_24_ = 1086ll;
const int _s_25_[] = {112, 0};
const long long _h_25_ = 999983ll, _a_25_ = 1087ll;
const int _s_26_[] = {113, 0};
const long long _h_26_ = 999983ll, _a_26_ = 1082ll;
const int _s_27_[] = {114, 0};
const long long _h_27_ = 999983ll, _a_27_ = 1088ll;
const int _s_28_[] = {115, 0};
const long long _h_28_ = 999983ll, _a_28_ = 1089ll;
const int _s_29_[] = {115, 104, 0};
const long long _h_29_ = 999983ll, _a_29_ = 1096ll;
const int _s_30_[] = {115, 104, 99, 104, 0};
const long long _h_30_ = 999983ll, _a_30_ = 1097ll;
const int _s_31_[] = {115, 99, 104, 0};
const long long _h_31_ = 999983ll, _a_31_ = 1097ll;
const int _s_32_[] = {116, 0};
const long long _h_32_ = 999983ll, _a_32_ = 1090ll;
const int _s_33_[] = {116, 115, 0};
const long long _h_33_ = 999983ll, _a_33_ = 1094ll;
const int _s_34_[] = {117, 0};
const long long _h_34_ = 999983ll, _a_34_ = 1091ll;
const int _s_35_[] = {118, 0};
const long long _h_35_ = 999983ll, _a_35_ = 1074ll;
const int _s_36_[] = {119, 0};
const long long _h_36_ = 999983ll, _a_36_ = 1074ll;
const int _s_37_[] = {120, 0};
const long long _h_37_ = 999966000289ll, _a_37_ = 1081982695ll;
const int _s_38_[] = {121, 0};
const long long _h_38_ = 999983ll, _a_38_ = 1080ll;
const int _s_39_[] = {121, 111, 0};
const long long _h_39_ = 999983ll, _a_39_ = 1105ll;
const int _s_40_[] = {121, 117, 0};
const long long _h_40_ = 999983ll, _a_40_ = 1102ll;
const int _s_41_[] = {121, 97, 0};
const long long _h_41_ = 999983ll, _a_41_ = 1103ll;
const int _s_42_[] = {122, 0};
const long long _h_42_ = 999983ll, _a_42_ = 1079ll;
const int _s_43_[] = {122, 104, 0};
const long long _h_43_ = 999983ll, _a_43_ = 1078ll;
const int _s_44_[] = {1072, 0};
const long long _h_44_ = 999983ll, _a_44_ = 97ll;
const int _s_45_[] = {1073, 0};
const long long _h_45_ = 999983ll, _a_45_ = 98ll;
const int _s_46_[] = {1074, 0};
const long long _h_46_ = 999983ll, _a_46_ = 118ll;
const int _s_47_[] = {1075, 0};
const long long _h_47_ = 999983ll, _a_47_ = 103ll;
const int _s_48_[] = {1076, 0};
const long long _h_48_ = 999983ll, _a_48_ = 100ll;
const int _s_49_[] = {1077, 0};
const long long _h_49_ = 999983ll, _a_49_ = 101ll;
const int _s_50_[] = {1105, 0};
const long long _h_50_ = 999966000289ll, _a_50_ = 120998054ll;
const int _s_51_[] = {1078, 0};
const long long _h_51_ = 999966000289ll, _a_51_ = 121998030ll;
const int _s_52_[] = {1079, 0};
const long long _h_52_ = 999983ll, _a_52_ = 122ll;
const int _s_53_[] = {1080, 0};
const long long _h_53_ = 999983ll, _a_53_ = 105ll;
const int _s_54_[] = {1080, 1081, 0};
const long long _h_54_ = 999983ll, _a_54_ = 121ll;
const int _s_55_[] = {1080, 1103, 0};
const long long _h_55_ = 999966000289ll, _a_55_ = 104998312ll;
const int _s_56_[] = {1081, 0};
const long long _h_56_ = 999983ll, _a_56_ = 121ll;
const int _s_57_[] = {1082, 0};
const long long _h_57_ = 999983ll, _a_57_ = 107ll;
const int _s_58_[] = {1082, 1089, 0};
const long long _h_58_ = 999983ll, _a_58_ = 120ll;
const int _s_59_[] = {1083, 0};
const long long _h_59_ = 999983ll, _a_59_ = 108ll;
const int _s_60_[] = {1084, 0};
const long long _h_60_ = 999983ll, _a_60_ = 109ll;
const int _s_61_[] = {1085, 0};
const long long _h_61_ = 999983ll, _a_61_ = 110ll;
const int _s_62_[] = {1086, 0};
const long long _h_62_ = 999983ll, _a_62_ = 111ll;
const int _s_63_[] = {1087, 0};
const long long _h_63_ = 999983ll, _a_63_ = 112ll;
const int _s_64_[] = {1088, 0};
const long long _h_64_ = 999983ll, _a_64_ = 114ll;
const int _s_65_[] = {1089, 0};
const long long _h_65_ = 999983ll, _a_65_ = 115ll;
const int _s_66_[] = {1090, 0};
const long long _h_66_ = 999983ll, _a_66_ = 116ll;
const int _s_67_[] = {1091, 0};
const long long _h_67_ = 999983ll, _a_67_ = 117ll;
const int _s_68_[] = {1092, 0};
const long long _h_68_ = 999983ll, _a_68_ = 102ll;
const int _s_69_[] = {1093, 0};
const long long _h_69_ = 999966000289ll, _a_69_ = 106998285ll;
const int _s_70_[] = {1094, 0};
const long long _h_70_ = 999966000289ll, _a_70_ = 115998143ll;
const int _s_71_[] = {1095, 0};
const long long _h_71_ = 999966000289ll, _a_71_ = 98998421ll;
const int _s_72_[] = {1096, 0};
const long long _h_72_ = 999966000289ll, _a_72_ = 114998149ll;
const int _s_73_[] = {1097, 0};
const long long _h_73_ = 7792474480393186625ll, _a_73_ = 4313774654010153786ll;
const int _s_74_[] = {1098, 0};
const long long _h_74_ = 1ll, _a_74_ = 0ll;
const int _s_75_[] = {1099, 0};
const long long _h_75_ = 999983ll, _a_75_ = 121ll;
const int _s_76_[] = {1100, 0};
const long long _h_76_ = 1ll, _a_76_ = 0ll;
const int _s_77_[] = {1101, 0};
const long long _h_77_ = 999983ll, _a_77_ = 101ll;
const int _s_78_[] = {1102, 0};
const long long _h_78_ = 999966000289ll, _a_78_ = 120998060ll;
const int _s_79_[] = {1103, 0};
const long long _h_79_ = 999966000289ll, _a_79_ = 120998040ll;
void translit_from_en_to_ru (int *s, long long *rh, int *hn) {

#define TEST(a, b, hm, ha) \
  i = 1;                         \
  while (a[i] && a[i] == b[i]) { \
    i++;                         \
  }                              \
  if (!a[i] || !b[i]) {          \
    nh = h * hm + ha;            \
    ns = a + i;                  \
  }                              \
  if (!*ns) {                    \
    rh[(*hn)++] = nh;            \
  }


#define PASS(a)                   \
  nh = h * HASH_MUL + a[0];       \
  ns = a + 1;                     \
  if (!*ns) {                     \
    rh[(*hn)++] = nh;             \
  }

  *hn = 0;
  int *ns = s, i;
  long long h = 0, nh = 0;

  while (*s) {
    switch (*s) {
    case 97://a
      //a --> а
      TEST(s, _s_1_, _h_1_, _a_1_);
      break;
    case 98://b
      //b --> б
      TEST(s, _s_2_, _h_2_, _a_2_);
      break;
    case 99://c
      //c --> к
      TEST(s, _s_3_, _h_3_, _a_3_);
      //ch --> ч
      TEST(s, _s_4_, _h_4_, _a_4_);
      break;
    case 100://d
      //d --> д
      TEST(s, _s_5_, _h_5_, _a_5_);
      break;
    case 101://e
      //e --> е
      TEST(s, _s_6_, _h_6_, _a_6_);
      //ei --> ей
      TEST(s, _s_7_, _h_7_, _a_7_);
      //ey --> ей
      TEST(s, _s_8_, _h_8_, _a_8_);
      break;
    case 102://f
      //f --> ф
      TEST(s, _s_9_, _h_9_, _a_9_);
      break;
    case 103://g
      //g --> г
      TEST(s, _s_10_, _h_10_, _a_10_);
      break;
    case 104://h
      //h --> х
      TEST(s, _s_11_, _h_11_, _a_11_);
      break;
    case 105://i
      //i --> и
      TEST(s, _s_12_, _h_12_, _a_12_);
      //ia --> ия
      TEST(s, _s_13_, _h_13_, _a_13_);
      //iy --> ий
      TEST(s, _s_14_, _h_14_, _a_14_);
      break;
    case 106://j
      //j --> й
      TEST(s, _s_15_, _h_15_, _a_15_);
      //jo --> ё
      TEST(s, _s_16_, _h_16_, _a_16_);
      //ju --> ю
      TEST(s, _s_17_, _h_17_, _a_17_);
      //ja --> я
      TEST(s, _s_18_, _h_18_, _a_18_);
      break;
    case 107://k
      //k --> к
      TEST(s, _s_19_, _h_19_, _a_19_);
      //kh --> х
      TEST(s, _s_20_, _h_20_, _a_20_);
      break;
    case 108://l
      //l --> л
      TEST(s, _s_21_, _h_21_, _a_21_);
      break;
    case 109://m
      //m --> м
      TEST(s, _s_22_, _h_22_, _a_22_);
      break;
    case 110://n
      //n --> н
      TEST(s, _s_23_, _h_23_, _a_23_);
      break;
    case 111://o
      //o --> о
      TEST(s, _s_24_, _h_24_, _a_24_);
      break;
    case 112://p
      //p --> п
      TEST(s, _s_25_, _h_25_, _a_25_);
      break;
    case 113://q
      //q --> к
      TEST(s, _s_26_, _h_26_, _a_26_);
      break;
    case 114://r
      //r --> р
      TEST(s, _s_27_, _h_27_, _a_27_);
      break;
    case 115://s
      //s --> с
      TEST(s, _s_28_, _h_28_, _a_28_);
      //sh --> ш
      TEST(s, _s_29_, _h_29_, _a_29_);
      //shch --> щ
      TEST(s, _s_30_, _h_30_, _a_30_);
      //sch --> щ
      TEST(s, _s_31_, _h_31_, _a_31_);
      break;
    case 116://t
      //t --> т
      TEST(s, _s_32_, _h_32_, _a_32_);
      //ts --> ц
      TEST(s, _s_33_, _h_33_, _a_33_);
      break;
    case 117://u
      //u --> у
      TEST(s, _s_34_, _h_34_, _a_34_);
      break;
    case 118://v
      //v --> в
      TEST(s, _s_35_, _h_35_, _a_35_);
      break;
    case 119://w
      //w --> в
      TEST(s, _s_36_, _h_36_, _a_36_);
      break;
    case 120://x
      //x --> кс
      TEST(s, _s_37_, _h_37_, _a_37_);
      break;
    case 121://y
      //y --> и
      TEST(s, _s_38_, _h_38_, _a_38_);
      //yo --> ё
      TEST(s, _s_39_, _h_39_, _a_39_);
      //yu --> ю
      TEST(s, _s_40_, _h_40_, _a_40_);
      //ya --> я
      TEST(s, _s_41_, _h_41_, _a_41_);
      break;
    case 122://z
      //z --> з
      TEST(s, _s_42_, _h_42_, _a_42_);
      //zh --> ж
      TEST(s, _s_43_, _h_43_, _a_43_);
      break;
    default:
      PASS(s);
    }
    s = ns++;
    h = nh;
  }
#undef TEST
#undef PASS
}
void translit_from_ru_to_en (int *s, long long *rh, int *hn) {

#define TEST(a, b, hm, ha) \
  i = 1;                         \
  while (a[i] && a[i] == b[i]) { \
    i++;                         \
  }                              \
  if (!a[i] || !b[i]) {          \
    nh = h * hm + ha;            \
    ns = a + i;                  \
  }                              \
  if (!*ns) {                    \
    rh[(*hn)++] = nh;            \
  }


#define PASS(a)                   \
  nh = h * HASH_MUL + a[0];       \
  ns = a + 1;                     \
  if (!*ns) {                     \
    rh[(*hn)++] = nh;             \
  }

  *hn = 0;
  int *ns = s, i;
  long long h = 0, nh = 0;

  while (*s) {
    switch (*s) {
    case 1072://а
      //а --> a
      TEST(s, _s_44_, _h_44_, _a_44_);
      break;
    case 1073://б
      //б --> b
      TEST(s, _s_45_, _h_45_, _a_45_);
      break;
    case 1074://в
      //в --> v
      TEST(s, _s_46_, _h_46_, _a_46_);
      break;
    case 1075://г
      //г --> g
      TEST(s, _s_47_, _h_47_, _a_47_);
      break;
    case 1076://д
      //д --> d
      TEST(s, _s_48_, _h_48_, _a_48_);
      break;
    case 1077://е
      //е --> e
      TEST(s, _s_49_, _h_49_, _a_49_);
      break;
    case 1105://ё
      //ё --> yo
      TEST(s, _s_50_, _h_50_, _a_50_);
      break;
    case 1078://ж
      //ж --> zh
      TEST(s, _s_51_, _h_51_, _a_51_);
      break;
    case 1079://з
      //з --> z
      TEST(s, _s_52_, _h_52_, _a_52_);
      break;
    case 1080://и
      //и --> i
      TEST(s, _s_53_, _h_53_, _a_53_);
      //ий --> y
      TEST(s, _s_54_, _h_54_, _a_54_);
      //ия --> ia
      TEST(s, _s_55_, _h_55_, _a_55_);
      break;
    case 1081://й
      //й --> y
      TEST(s, _s_56_, _h_56_, _a_56_);
      break;
    case 1082://к
      //к --> k
      TEST(s, _s_57_, _h_57_, _a_57_);
      //кс --> x
      TEST(s, _s_58_, _h_58_, _a_58_);
      break;
    case 1083://л
      //л --> l
      TEST(s, _s_59_, _h_59_, _a_59_);
      break;
    case 1084://м
      //м --> m
      TEST(s, _s_60_, _h_60_, _a_60_);
      break;
    case 1085://н
      //н --> n
      TEST(s, _s_61_, _h_61_, _a_61_);
      break;
    case 1086://о
      //о --> o
      TEST(s, _s_62_, _h_62_, _a_62_);
      break;
    case 1087://п
      //п --> p
      TEST(s, _s_63_, _h_63_, _a_63_);
      break;
    case 1088://р
      //р --> r
      TEST(s, _s_64_, _h_64_, _a_64_);
      break;
    case 1089://с
      //с --> s
      TEST(s, _s_65_, _h_65_, _a_65_);
      break;
    case 1090://т
      //т --> t
      TEST(s, _s_66_, _h_66_, _a_66_);
      break;
    case 1091://у
      //у --> u
      TEST(s, _s_67_, _h_67_, _a_67_);
      break;
    case 1092://ф
      //ф --> f
      TEST(s, _s_68_, _h_68_, _a_68_);
      break;
    case 1093://х
      //х --> kh
      TEST(s, _s_69_, _h_69_, _a_69_);
      break;
    case 1094://ц
      //ц --> ts
      TEST(s, _s_70_, _h_70_, _a_70_);
      break;
    case 1095://ч
      //ч --> ch
      TEST(s, _s_71_, _h_71_, _a_71_);
      break;
    case 1096://ш
      //ш --> sh
      TEST(s, _s_72_, _h_72_, _a_72_);
      break;
    case 1097://щ
      //щ --> shch
      TEST(s, _s_73_, _h_73_, _a_73_);
      break;
    case 1098://ъ
      //ъ -->
      if (s == ns) {
        return;
      }
      TEST(s, _s_74_, _h_74_, _a_74_);
      break;
    case 1099://ы
      //ы --> y
      TEST(s, _s_75_, _h_75_, _a_75_);
      break;
    case 1100://ь
      //ь -->
      if (s == ns) {
        return;
      }
      TEST(s, _s_76_, _h_76_, _a_76_);
      break;
    case 1101://э
      //э --> e
      TEST(s, _s_77_, _h_77_, _a_77_);
      break;
    case 1102://ю
      //ю --> yu
      TEST(s, _s_78_, _h_78_, _a_78_);
      break;
    case 1103://я
      //я --> ya
      TEST(s, _s_79_, _h_79_, _a_79_);
      break;
    default:
      PASS(s);
    }
    s = ns++;
    h = nh;
  }
#undef TEST
#undef PASS
}

unsigned int convert_language (unsigned int x) {
  switch (x) {
  case 113://q->й
    return 1081;
  case 119://w->ц
    return 1094;
  case 101://e->у
    return 1091;
  case 114://r->к
    return 1082;
  case 116://t->е
    return 1077;
  case 121://y->н
    return 1085;
  case 117://u->г
    return 1075;
  case 105://i->ш
    return 1096;
  case 111://o->щ
    return 1097;
  case 112://p->з
    return 1079;
  case 91://[->х
    return 1093;
  case 93://]->ъ
    return 1098;
  case 97://a->ф
    return 1092;
  case 115://s->ы
    return 1099;
  case 100://d->в
    return 1074;
  case 102://f->а
    return 1072;
  case 103://g->п
    return 1087;
  case 104://h->р
    return 1088;
  case 106://j->о
    return 1086;
  case 107://k->л
    return 1083;
  case 108://l->д
    return 1076;
  case 59://;->ж
    return 1078;
  case 39://'->э
    return 1101;
  case 122://z->я
    return 1103;
  case 120://x->ч
    return 1095;
  case 99://c->с
    return 1089;
  case 118://v->м
    return 1084;
  case 98://b->и
    return 1080;
  case 110://n->т
    return 1090;
  case 109://m->ь
    return 1100;
  case 44://,->б
    return 1073;
  case 46://.->ю
    return 1102;
  case 96://`->ё
    return 1105;
  case 81://Q->Й
    return 1049;
  case 87://W->Ц
    return 1062;
  case 69://E->У
    return 1059;
  case 82://R->К
    return 1050;
  case 84://T->Е
    return 1045;
  case 89://Y->Н
    return 1053;
  case 85://U->Г
    return 1043;
  case 73://I->Ш
    return 1064;
  case 79://O->Щ
    return 1065;
  case 80://P->З
    return 1047;
  case 123://{->Х
    return 1061;
  case 125://}->Ъ
    return 1066;
  case 65://A->Ф
    return 1060;
  case 83://S->Ы
    return 1067;
  case 68://D->В
    return 1042;
  case 70://F->А
    return 1040;
  case 71://G->П
    return 1055;
  case 72://H->Р
    return 1056;
  case 74://J->О
    return 1054;
  case 75://K->Л
    return 1051;
  case 76://L->Д
    return 1044;
  case 58://:->Ж
    return 1046;
  case 34://"->Э
    return 1069;
  case 90://Z->Я
    return 1071;
  case 88://X->Ч
    return 1063;
  case 67://C->С
    return 1057;
  case 86://V->М
    return 1052;
  case 66://B->И
    return 1048;
  case 78://N->Т
    return 1058;
  case 77://M->Ь
    return 1068;
  case 60://<->Б
    return 1041;
  case 62://>->Ю
    return 1070;
  case 126://~->Ё
    return 1025;
  case 1081://й->q
    return 113;
  case 1094://ц->w
    return 119;
  case 1091://у->e
    return 101;
  case 1082://к->r
    return 114;
  case 1077://е->t
    return 116;
  case 1085://н->y
    return 121;
  case 1075://г->u
    return 117;
  case 1096://ш->i
    return 105;
  case 1097://щ->o
    return 111;
  case 1079://з->p
    return 112;
  case 1093://х->[
    return 91;
  case 1098://ъ->]
    return 93;
  case 1092://ф->a
    return 97;
  case 1099://ы->s
    return 115;
  case 1074://в->d
    return 100;
  case 1072://а->f
    return 102;
  case 1087://п->g
    return 103;
  case 1088://р->h
    return 104;
  case 1086://о->j
    return 106;
  case 1083://л->k
    return 107;
  case 1076://д->l
    return 108;
  case 1078://ж->;
    return 59;
  case 1101://э->'
    return 39;
  case 1103://я->z
    return 122;
  case 1095://ч->x
    return 120;
  case 1089://с->c
    return 99;
  case 1084://м->v
    return 118;
  case 1080://и->b
    return 98;
  case 1090://т->n
    return 110;
  case 1100://ь->m
    return 109;
  case 1073://б->,
    return 44;
  case 1102://ю->.
    return 46;
  case 1105://ё->`
    return 96;
  case 1049://Й->Q
    return 81;
  case 1062://Ц->W
    return 87;
  case 1059://У->E
    return 69;
  case 1050://К->R
    return 82;
  case 1045://Е->T
    return 84;
  case 1053://Н->Y
    return 89;
  case 1043://Г->U
    return 85;
  case 1064://Ш->I
    return 73;
  case 1065://Щ->O
    return 79;
  case 1047://З->P
    return 80;
  case 1061://Х->{
    return 123;
  case 1066://Ъ->}
    return 125;
  case 1060://Ф->A
    return 65;
  case 1067://Ы->S
    return 83;
  case 1042://В->D
    return 68;
  case 1040://А->F
    return 70;
  case 1055://П->G
    return 71;
  case 1056://Р->H
    return 72;
  case 1054://О->J
    return 74;
  case 1051://Л->K
    return 75;
  case 1044://Д->L
    return 76;
  case 1046://Ж->:
    return 58;
  case 1069://Э->"
    return 34;
  case 1071://Я->Z
    return 90;
  case 1063://Ч->X
    return 88;
  case 1057://С->C
    return 67;
  case 1052://М->V
    return 86;
  case 1048://И->B
    return 66;
  case 1058://Т->N
    return 78;
  case 1068://Ь->M
    return 77;
  case 1041://Б-><
    return 60;
  case 1070://Ю->>
    return 62;
  case 1025://Ё->~
    return 126;
  case 48://0->0
    return 48;
  case 49://1->1
    return 49;
  case 50://2->2
    return 50;
  case 51://3->3
    return 51;
  case 52://4->4
    return 52;
  case 53://5->5
    return 53;
  case 54://6->6
    return 54;
  case 55://7->7
    return 55;
  case 56://8->8
    return 56;
  case 57://9->9
    return 57;
  default:
    if (x >= 128) {
      return x;
    }
    return 32;
  }
}
