/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define	_FILE_OFFSET_BITS	64

#include "utf8_utils.h"

void string_to_utf8 (const unsigned char *s, int *v) {
  int *tv = v;
#define CHECK(x) if (!(x)) {v = tv; break;}

  int a, b, c, d;

  while (*s) {
    a = *s++;
    if ((a & 0x80) == 0) {
      *v++ = a;
    } else if ((a & 0x40) == 0) {
      CHECK(0);
    } else if ((a & 0x20) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      CHECK((a & 0x1e) > 0);
      *v++ = ((a & 0x1f) << 6) | (b & 0x3f);
    } else if ((a & 0x10) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      c = *s++;
      CHECK((c & 0xc0) == 0x80);
      CHECK(((a & 0x0f) | (b & 0x20)) > 0);
      *v++ = ((a & 0x0f) << 12) | ((b & 0x3f) << 6) | (c & 0x3f);
    } else if ((a & 0x08) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      c = *s++;
      CHECK((c & 0xc0) == 0x80);
      d = *s++;
      CHECK((d & 0xc0) == 0x80);
      CHECK(((a & 0x07) | (b & 0x30)) > 0);
      *v++ = ((a & 0x07) << 18) | ((b & 0x3f) << 12) | ((c & 0x3f) << 6) | (d & 0x3f);
    } else {
      CHECK(0);
    }
  }
  *v = 0;
#undef CHECK
}

void good_string_to_utf8 (const unsigned char *s, int *v) {
  string_to_utf8 (s, v);

  int i, j;
  for (i = j = 0; v[i]; i++) {
    if (v[i] == '&') {
      if (v[i + 1] == 'a' && v[i + 2] == 'm' && v[i + 3] == 'p' && v[i + 4] == ';') {
        i += 4, v[j++] = '&';
      } else if (v[i + 1] == 'l' && v[i + 2] == 't' && v[i + 3] == ';') {
        i += 3, v[j++] = '<';
      } else if (v[i + 1] == 'g' && v[i + 2] == 't' && v[i + 3] == ';') {
        i += 3, v[j++] = '>';
      } else if (v[i + 1] == 'q' && v[i + 2] == 'u' && v[i + 3] == 'o' && v[i + 4] == 't' && v[i + 5] == ';') {
        i += 5, v[j++] = '"';
      } else {
        v[j++] = '&';
      }
    } else {
      v[j++] = v[i];
    }
  }
  v[j++] = 0;

  for (i = j = 0; v[i]; i++) {
    if (v[i] == '&') {
      if (v[i + 1] == 'a' && v[i + 2] == 'm' && v[i + 3] == 'p' && v[i + 4] == ';') {
        i += 4, v[j++] = '&';
      } else if (v[i + 1] == '#') {
        int r = 0, ti = i;
        for (i += 2; v[i]!=';' && v[i]; i++) {
          if ('0' <= v[i] && v[i] <= '9') {
            r = r * 10 + v[i] - '0';
          } else {
            break;
          }
        }
        if (v[i] != ';') {
          v[j++] = v[i = ti];
        } else {
          v[j++] = r;
        }
      } else {
        v[j++] = v[i];
      }
    } else {
      v[j++] = v[i];
    }
  }
  v[j++] = 0;

  for (i = j = 0; v[i]; i++) {
    if (v[i] !=   173 && (v[i] < 65024 || v[i] > 65062) && (v[i] < 7627 || v[i] > 7654) &&
        v[i] !=  8288 && (v[i] <  8202 || v[i] >  8207) && (v[i] < 8400 || v[i] > 8433) &&
        v[i] !=  8228 && (v[i] <  8298 || v[i] >  8303) &&
        v[i] != 65279 && (v[i] <   768 || v[i] >   879)) {
      v[j++] = v[i];
    }
  }
  v[j++] = 0;
}

void put_string_utf8 (const int *v, char *s) {
  while (*v) {
    s += put_char_utf8 (*v++, s);
  }
  *s = 0;
}

unsigned int convert_prep (unsigned int x) {
  switch (x) {
  case 65://A->a
    return 97;
  case 66://B->b
    return 98;
  case 67://C->c
    return 99;
  case 68://D->d
    return 100;
  case 69://E->e
    return 101;
  case 70://F->f
    return 102;
  case 71://G->g
    return 103;
  case 72://H->h
    return 104;
  case 73://I->i
    return 105;
  case 74://J->j
    return 106;
  case 75://K->k
    return 107;
  case 76://L->l
    return 108;
  case 77://M->m
    return 109;
  case 78://N->n
    return 110;
  case 79://O->o
    return 111;
  case 80://P->p
    return 112;
  case 81://Q->q
    return 113;
  case 82://R->r
    return 114;
  case 83://S->s
    return 115;
  case 84://T->t
    return 116;
  case 85://U->u
    return 117;
  case 86://V->v
    return 118;
  case 87://W->w
    return 119;
  case 88://X->x
    return 120;
  case 89://Y->y
    return 121;
  case 90://Z->z
    return 122;
  case 1049://Й->й
    return 1081;
  case 1062://Ц->ц
    return 1094;
  case 1059://У->у
    return 1091;
  case 1050://К->к
    return 1082;
  case 1045://Е->е
    return 1077;
  case 1053://Н->н
    return 1085;
  case 1043://Г->г
    return 1075;
  case 1064://Ш->ш
    return 1096;
  case 1065://Щ->щ
    return 1097;
  case 1047://З->з
    return 1079;
  case 1061://Х->х
    return 1093;
  case 1066://Ъ->ъ
    return 1098;
  case 1060://Ф->ф
    return 1092;
  case 1067://Ы->ы
    return 1099;
  case 1042://В->в
    return 1074;
  case 1040://А->а
    return 1072;
  case 1055://П->п
    return 1087;
  case 1056://Р->р
    return 1088;
  case 1054://О->о
    return 1086;
  case 1051://Л->л
    return 1083;
  case 1044://Д->д
    return 1076;
  case 1046://Ж->ж
    return 1078;
  case 1069://Э->э
    return 1101;
  case 1071://Я->я
    return 1103;
  case 1063://Ч->ч
    return 1095;
  case 1057://С->с
    return 1089;
  case 1052://М->м
    return 1084;
  case 1048://И->и
    return 1080;
  case 1058://Т->т
    return 1090;
  case 1068://Ь->ь
    return 1100;
  case 1041://Б->б
    return 1073;
  case 1070://Ю->ю
    return 1102;
  case 1025://Ё->ё
    return 1105;
  case 97://a->a
    return 97;
  case 98://b->b
    return 98;
  case 99://c->c
    return 99;
  case 100://d->d
    return 100;
  case 101://e->e
    return 101;
  case 102://f->f
    return 102;
  case 103://g->g
    return 103;
  case 104://h->h
    return 104;
  case 105://i->i
    return 105;
  case 106://j->j
    return 106;
  case 107://k->k
    return 107;
  case 108://l->l
    return 108;
  case 109://m->m
    return 109;
  case 110://n->n
    return 110;
  case 111://o->o
    return 111;
  case 112://p->p
    return 112;
  case 113://q->q
    return 113;
  case 114://r->r
    return 114;
  case 115://s->s
    return 115;
  case 116://t->t
    return 116;
  case 117://u->u
    return 117;
  case 118://v->v
    return 118;
  case 119://w->w
    return 119;
  case 120://x->x
    return 120;
  case 121://y->y
    return 121;
  case 122://z->z
    return 122;
  case 1081://й->й
    return 1081;
  case 1094://ц->ц
    return 1094;
  case 1091://у->у
    return 1091;
  case 1082://к->к
    return 1082;
  case 1077://е->е
    return 1077;
  case 1085://н->н
    return 1085;
  case 1075://г->г
    return 1075;
  case 1096://ш->ш
    return 1096;
  case 1097://щ->щ
    return 1097;
  case 1079://з->з
    return 1079;
  case 1093://х->х
    return 1093;
  case 1098://ъ->ъ
    return 1098;
  case 1092://ф->ф
    return 1092;
  case 1099://ы->ы
    return 1099;
  case 1074://в->в
    return 1074;
  case 1072://а->а
    return 1072;
  case 1087://п->п
    return 1087;
  case 1088://р->р
    return 1088;
  case 1086://о->о
    return 1086;
  case 1083://л->л
    return 1083;
  case 1076://д->д
    return 1076;
  case 1078://ж->ж
    return 1078;
  case 1101://э->э
    return 1101;
  case 1103://я->я
    return 1103;
  case 1095://ч->ч
    return 1095;
  case 1089://с->с
    return 1089;
  case 1084://м->м
    return 1084;
  case 1080://и->и
    return 1080;
  case 1090://т->т
    return 1090;
  case 1100://ь->ь
    return 1100;
  case 1073://б->б
    return 1073;
  case 1102://ю->ю
    return 1102;
  case 1105://ё->ё
    return 1105;
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

int remove_diacritics (int c) {
  switch (c) {
    case 193:
    case 258:
    case 7854:
    case 7862:
    case 7856:
    case 7858:
    case 7860:
    case 461:
    case 194:
    case 7844:
    case 7852:
    case 7846:
    case 7848:
    case 7850:
    case 196:
    case 7840:
    case 192:
    case 7842:
    case 256:
    case 260:
    case 197:
    case 506:
    case 195:
    case 198:
    case 508:
      return 65;
    case 7684:
    case 385:
    case 666:
    case 606:
      return 66;
    case 262:
    case 268:
    case 199:
    case 264:
    case 266:
    case 390:
    case 663:
      return 67;
    case 270:
    case 7698:
    case 7692:
    case 394:
    case 7694:
    case 498:
    case 453:
    case 272:
    case 208:
    case 497:
    case 452:
      return 68;
    case 201:
    case 276:
    case 282:
    case 202:
    case 7870:
    case 7878:
    case 7872:
    case 7874:
    case 7876:
    case 203:
    case 278:
    case 7864:
    case 200:
    case 7866:
    case 274:
    case 280:
    case 7868:
    case 400:
    case 399:
      return 69;
    case 401:
      return 70;
    case 500:
    case 286:
    case 486:
    case 290:
    case 284:
    case 288:
    case 7712:
    case 667:
      return 71;
    case 7722:
    case 292:
    case 7716:
    case 294:
      return 72;
    case 205:
    case 300:
    case 463:
    case 206:
    case 207:
    case 304:
    case 7882:
    case 204:
    case 7880:
    case 298:
    case 302:
    case 296:
    case 306:
      return 73;
    case 308:
      return 74;
    case 310:
    case 7730:
    case 408:
    case 7732:
      return 75;
    case 313:
    case 573:
    case 317:
    case 315:
    case 7740:
    case 7734:
    case 7736:
    case 7738:
    case 319:
    case 456:
    case 321:
    case 455:
      return 76;
    case 7742:
    case 7744:
    case 7746:
      return 77;
    case 323:
    case 327:
    case 325:
    case 7754:
    case 7748:
    case 7750:
    case 504:
    case 413:
    case 7752:
    case 459:
    case 209:
    case 458:
      return 78;
    case 211:
    case 334:
    case 465:
    case 212:
    case 7888:
    case 7896:
    case 7890:
    case 7892:
    case 7894:
    case 214:
    case 7884:
    case 336:
    case 210:
    case 7886:
    case 416:
    case 7898:
    case 7906:
    case 7900:
    case 7902:
    case 7904:
    case 332:
    case 415:
    case 490:
    case 216:
    case 510:
    case 213:
    case 338:
    case 630:
      return 79;
    case 222:
      return 80;
      return 81;
    case 340:
    case 344:
    case 342:
    case 7768:
    case 7770:
    case 7772:
    case 7774:
    case 641:
      return 82;
    case 346:
    case 352:
    case 350:
    case 348:
    case 536:
    case 7776:
    case 7778:
    case 7838:
      return 83;
    case 356:
    case 354:
    case 7792:
    case 538:
    case 7788:
    case 7790:
    case 358:
      return 84;
    case 218:
    case 364:
    case 467:
    case 219:
    case 220:
    case 471:
    case 473:
    case 475:
    case 469:
    case 7908:
    case 368:
    case 217:
    case 7910:
    case 431:
    case 7912:
    case 7920:
    case 7914:
    case 7916:
    case 7918:
    case 362:
    case 370:
    case 366:
    case 360:
      return 85;
      return 86;
    case 7810:
    case 372:
    case 7812:
    case 7808:
    case 684:
      return 87;
      return 88;
    case 221:
    case 374:
    case 376:
    case 7822:
    case 7924:
    case 7922:
    case 435:
    case 7926:
    case 562:
    case 7928:
      return 89;
    case 377:
    case 381:
    case 379:
    case 7826:
    case 7828:
    case 437:
      return 90;
    case 225:
    case 259:
    case 7855:
    case 7863:
    case 7857:
    case 7859:
    case 7861:
    case 462:
    case 226:
    case 7845:
    case 7853:
    case 7847:
    case 7849:
    case 7851:
    case 228:
    case 7841:
    case 224:
    case 7843:
    case 257:
    case 261:
    case 229:
    case 507:
    case 227:
    case 230:
    case 509:
    case 593:
    case 592:
    case 594:
      return 97;
    case 7685:
    case 595:
    case 223:
      return 98;
    case 263:
    case 269:
    case 231:
    case 265:
    case 597:
    case 267:
      return 99;
    case 271:
    case 7699:
    case 7693:
    case 599:
    case 7695:
    case 273:
    case 598:
    case 676:
    case 499:
    case 675:
    case 677:
    case 454:
    case 240:
      return 100;
    case 233:
    case 277:
    case 283:
    case 234:
    case 7871:
    case 7879:
    case 7873:
    case 7875:
    case 7877:
    case 235:
    case 279:
    case 7865:
    case 232:
    case 7867:
    case 275:
    case 281:
    case 7869:
    case 658:
    case 495:
    case 659:
    case 600:
    case 604:
    case 605:
    case 601:
    case 602:
      return 101;
    case 402:
    case 383:
    case 681:
    case 64257:
    case 64258:
    case 643:
    case 646:
    case 645:
    case 644:
      return 102;
    case 501:
    case 287:
    case 487:
    case 291:
    case 285:
    case 289:
    case 608:
    case 7713:
    case 609:
    case 611:
      return 103;
    case 7723:
    case 293:
    case 7717:
    case 614:
    case 7830:
    case 295:
    case 615:
    case 613:
    case 686:
    case 687:
      return 104;
    case 237:
    case 301:
    case 464:
    case 238:
    case 239:
    case 7883:
    case 236:
    case 7881:
    case 299:
    case 303:
    case 616:
    case 297:
    case 617:
    case 305:
    case 307:
      return 105;
    case 496:
    case 309:
    case 669:
    case 567:
    case 607:
      return 106;
    case 311:
    case 7731:
    case 409:
    case 7733:
    case 312:
    case 670:
      return 107;
    case 314:
    case 410:
    case 620:
    case 318:
    case 316:
    case 7741:
    case 7735:
    case 7737:
    case 7739:
    case 320:
    case 619:
    case 621:
    case 322:
    case 411:
    case 622:
    case 457:
    case 682:
    case 683:
      return 108;
    case 7743:
    case 7745:
    case 7747:
    case 625:
    case 623:
    case 624:
      return 109;
    case 329:
    case 324:
    case 328:
    case 326:
    case 7755:
    case 7749:
    case 7751:
    case 505:
    case 626:
    case 7753:
    case 627:
    case 241:
    case 460:
    case 331:
    case 330:
      return 110;
    case 243:
    case 335:
    case 466:
    case 244:
    case 7889:
    case 7897:
    case 7891:
    case 7893:
    case 7895:
    case 246:
    case 7885:
    case 337:
    case 242:
    case 7887:
    case 417:
    case 7899:
    case 7907:
    case 7901:
    case 7903:
    case 7905:
    case 333:
    case 491:
    case 248:
    case 511:
    case 245:
    case 603:
    case 596:
    case 629:
    case 664:
    case 339:
      return 111;
    case 632:
    case 254:
      return 112;
    case 672:
      return 113;
    case 341:
    case 345:
    case 343:
    case 7769:
    case 7771:
    case 7773:
    case 638:
    case 7775:
    case 636:
    case 637:
    case 639:
    case 633:
    case 635:
    case 634:
      return 114;
    case 347:
    case 353:
    case 351:
    case 349:
    case 537:
    case 7777:
    case 7779:
    case 642:
      return 115;
    case 357:
    case 355:
    case 7793:
    case 539:
    case 7831:
    case 7789:
    case 7791:
    case 648:
    case 359:
    case 680:
    case 679:
    case 678:
    case 647:
      return 116;
    case 649:
    case 250:
    case 365:
    case 468:
    case 251:
    case 252:
    case 472:
    case 474:
    case 476:
    case 470:
    case 7909:
    case 369:
    case 249:
    case 7911:
    case 432:
    case 7913:
    case 7921:
    case 7915:
    case 7917:
    case 7919:
    case 363:
    case 371:
    case 367:
    case 361:
    case 650:
      return 117;
    case 651:
    case 652:
      return 118;
    case 7811:
    case 373:
    case 7813:
    case 7809:
    case 653:
      return 119;
      return 120;
    case 253:
    case 375:
    case 255:
    case 7823:
    case 7925:
    case 7923:
    case 436:
    case 7927:
    case 563:
    case 7929:
    case 654:
      return 121;
    case 378:
    case 382:
    case 657:
    case 380:
    case 7827:
    case 7829:
    case 656:
    case 438:
      return 122;

    case 9398 ... 9423:
      return c - 9398 + 'a';
    case 9424 ... 9449:
      return c - 9424 + 'a';
    case 9372 ... 9397:
      return c - 9372 + 'a';
    case 65313 ... 65338:
      return c - 65313 + 'a';
    case 65345 ... 65370:
      return c - 65345 + 'a';
    case 120250 ... 120275:
      return c - 120250 + 'a';
    case 9312 ... 9320:
      return c - 9312 + '1';
    case 9332 ... 9340:
      return c - 9332 + '1';
    case 9352 ... 9360:
      return c - 9352 + '1';
    case 65296 ... 65305:
      return c - 65296 + '0';

    case 170:      return 'a';
    case 178:      return '2';
    case 179:      return '3';
    case 185:      return '1';
    case 186:      return 'o';
    case 688:      return 'h';
    case 690:      return 'j';
    case 691:      return 'r';
    case 695:      return 'w';
    case 696:      return 'y';
    case 737:      return 'l';
    case 738:      return 's';
    case 739:      return 'x';
    case 7468:      return 'a';
    case 7470:      return 'b';
    case 7472:      return 'd';
    case 7473:      return 'e';
    case 7475:      return 'g';
    case 7476:      return 'h';
    case 7477:      return 'i';
    case 7478:      return 'j';
    case 7479:      return 'k';
    case 7480:      return 'l';
    case 7481:      return 'm';
    case 7482:      return 'n';
    case 7484:      return 'o';
    case 7486:      return 'p';
    case 7487:      return 'r';
    case 7488:      return 't';
    case 7489:      return 'u';
    case 7490:      return 'w';
    case 7491:      return 'a';
    case 7495:      return 'b';
    case 7496:      return 'd';
    case 7497:      return 'e';
    case 7501:      return 'g';
    case 7503:      return 'k';
    case 7504:      return 'm';
    case 7506:      return 'o';
    case 7510:      return 'p';
    case 7511:      return 't';
    case 7512:      return 'u';
    case 7515:      return 'v';
    case 7522:      return 'i';
    case 7523:      return 'r';
    case 7524:      return 'u';
    case 7525:      return 'v';
    case 8304:      return '0';
    case 8305:      return 'i';
    case 8308:      return '4';
    case 8309:      return '5';
    case 8310:      return '6';
    case 8311:      return '7';
    case 8312:      return '8';
    case 8313:      return '9';
    case 8319:      return 'n';
    case 8320:      return '0';
    case 8321:      return '1';
    case 8322:      return '2';
    case 8323:      return '3';
    case 8324:      return '4';
    case 8325:      return '5';
    case 8326:      return '6';
    case 8327:      return '7';
    case 8328:      return '8';
    case 8329:      return '9';
    case 8336:      return 'a';
    case 8337:      return 'e';
    case 8338:      return 'o';
    case 8339:      return 'x';
    case 8450:      return 'c';
    case 8458:      return 'g';
    case 8459:      return 'h';
    case 8460:      return 'h';
    case 8461:      return 'h';
    case 8462:      return 'h';
    case 8464:      return 'i';
    case 8465:      return 'i';
    case 8466:      return 'l';
    case 8467:      return 'l';
    case 8469:      return 'n';
    case 8473:      return 'p';
    case 8474:      return 'q';
    case 8475:      return 'r';
    case 8476:      return 'r';
    case 8477:      return 'r';
    case 8484:      return 'z';
    case 8488:      return 'z';
    case 8490:      return 'k';
    case 8492:      return 'b';
    case 8493:      return 'c';
    case 8495:      return 'e';
    case 8496:      return 'e';
    case 8497:      return 'f';
    case 8499:      return 'm';
    case 8500:      return 'o';
    case 8505:      return 'i';
    case 8517:      return 'd';
    case 8518:      return 'd';
    case 8519:      return 'e';
    case 8520:      return 'i';
    case 8521:      return 'j';
    case 8544:      return 'i';
    case 8548:      return 'v';
    case 8553:      return 'x';
    case 8556:      return 'l';
    case 8557:      return 'c';
    case 8558:      return 'd';
    case 8559:      return 'm';
    case 8560:      return 'i';
    case 8564:      return 'v';
    case 8569:      return 'x';
    case 8572:      return 'l';
    case 8573:      return 'c';
    case 8574:      return 'd';
    case 8575:      return 'm';
    case 9450:      return '0';

    default:
      return c;
  }
}
