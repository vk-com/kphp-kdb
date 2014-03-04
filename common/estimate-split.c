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

    Copyright 2010 Vkontakte Ltd
              2010 Nikolai Durov
              2010 Andrei Lopatin
*/

#include "estimate-split.h"
#include <math.h>

/*
    N objects are randomly put into K buckets
    find N' such that each bucket contains at most N' objects with probability > 1 - 1e-9
  USE:
    if we want to find N most rated people by distributing query to K servers,
    ask only N' top results from each of them

  ESTIMATE:
    N/K + 7*sqrt(N/K) leads to error probability <= 3e-12 * K, use it if N >= 100, N/K >= 20
  EXACT:
    sum(i=0..N', N!/(i!*(N-i)!)*p^(N-i)*q^i) > 1-eps, where p=1/K, q=1-p
*/
int estimate_split (int N, int K) {
  double x, a, tp, ta;
  int i;
  if (N <= 0 || K <= 0) {
    return 0;
  }
  if (K == 1) {
    return N;
  }
  if (N >= 100 && N >= 100*K) {	// if necessary, N/K >= 100 may be replaced by any value <= 500
    x = (double) N / K;
    return (int) (x + 7*sqrt(x) + 1);
  }
  a = exp (N*log (1 - 1.0/K));
  x = a;
  i = 0;
  tp = 1 - 1e-9/K;
  ta = 1e-9/K/N;
  while (x < tp && i < N && !(a < ta && i*K > N)) {
    a *= N-i;
    a /= (K-1)*++i;
    x += a;
  }
  return i;
}
