#!/usr/bin/python
"""
Script computing correclation statistics between different amortization counters
"""

import sys, os, math

def get_sum(c, ra, rb):
  r = 0
  for i in range(ra[0], ra[1]):
    for j in range(rb[0], rb[1]):
      r += c[i][j]
  return r

def calc(dir, i, j):
  outfilename = os.path.join(dir, str(i) + str(j) + '.cor')
  transpose = False
  if i > j:
    transpose = True
    i, j = j, i
  f = open (os.path.join(dir, str(i) + str(j)), 'r')
  c = []
  for s in f:
    c.append([int(z) for z in s.rstrip('\n').split('\t')])
    assert(len(c[-1])==100)
  f.close()
  if transpose:
    for i in range(100):
      for j in range(i+1, 100):
        c[i][j],c[j][i] = c[j][i], c[i][j]
  f = open(outfilename, 'w')
  assert(len(c)==100)
  d = [0] * 4
  t = get_sum(c, (0, 100), (0, 100))
  for a in range(1, 100):
    m = 0
    best_b = -1
    for b in range(1, 100):
      d[0] = get_sum(c, (0, a), (0, b))
      d[1] = get_sum(c, (a, 100), (0, b))
      d[2] = get_sum(c, (0, a), (b, 100))
      d[3] = get_sum(c, (a, 100), (b, 100))
      for k in range(4): d[k] = float(d[k]) / float(t)
      EXY = d[3]
      EX = d[1] + d[3]
      EY = d[2] + d[3]
      cov = EXY - EX * EY
      correlation = cov / math.sqrt((1.0 - EX * EX) * (1.0 - EY * EY))
      assert (-1.0 - 1e-6 < correlation < 1.0 + 1e6)
      if correlation > m:
        m, bestb = correlation, b
    f.write(str(a) + '\t' + str(bestb) + '\t' + str(m) + '\n')
  f.close()

if len(sys.argv)<2:
  sys.stderr.write('Usage: ' + sys.argv[0] + ' <directory>\m')
  sys.exit(1)

for i in range(4):
  for j in range(4):
    if i != j:
      calc(sys.argv[1], i, j)


