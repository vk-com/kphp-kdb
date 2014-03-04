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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/


#include <assert.h>
int am_choose_hash_prime (int x) {
  static const int p[] = {
  //23,29,37,41,47,53,59,67,79,89,101,113,127,149,167,191,211,233,257,283,313,347,383,431,479,541,599,659,727,809,907,
  1103,1217,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,
  4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16673,18341,20177,22229,
  24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,
  112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143, 389561, 428531,
  471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,
  1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,
  4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047,
  14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,
  41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,
  118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,
  307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,
  798252779,878078057,965885863,1062474559};
  const int lp = sizeof (p) / sizeof (p[0]);
  int a = -1, b = lp;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (p[c] <= x) { a = c; } else { b = c; }
  }
  if (a < 0) { a++; }
  while (a < lp && p[a] < x) {
    a++;
  }
  assert (a < lp-1);
  assert (x <= p[a]);
  return p[a];
}
