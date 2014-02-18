import os, sys, logging, subprocess, filecmp, shutil, string, random, time, getopt, stat, signal, cPickle

def _rmdir (dir):
  if os.path.lexists (dir) and os.path.isdir (dir):
    shutil.rmtree (dir)

def _emptydir(dir):
  _rmdir (dir)
  os.mkdir (dir, 0700)

def init():
  global BIN, ICPLC, TMP_DIR, HOME, PROG_ID
  if os.getenv ('m') == '32': mode = 32
  else: mode = 64
  HOME = os.path.expanduser ('~')
  fmt = '%(asctime)s %(levelname)s %(message)s'
  logging.basicConfig (level=logging.DEBUG, format=fmt, filename=os.path.join (HOME, 'run.log'), filemode='w')
  console = logging.StreamHandler (sys.stdout)
  formatter = logging.Formatter (fmt)
  console.setFormatter (formatter)
  logging.getLogger ('').addHandler (console)
  if mode == 64: suffix = ''
  elif mode == 32: suffix = '32'
  else: assert (0)
  if os.getenv ('p') == '1': suffix += 'p'
  BIN = os.path.join(HOME, 'engine/src/objs' + suffix + '/bin')
  ICPLC = os.path.join(BIN, 'icplc')
  TMP_DIR = os.path.join(HOME, 'tmp', '.icpl')
  _emptydir (TMP_DIR)
  PROG_ID = 0

def test_prog(input, output):
  global PROG_ID
  PROG_ID += 1
  filename = os.path.join(TMP_DIR, str(PROG_ID) + '.icpl')
  f = open(filename, 'w')
  f.write(input)
  f.close()
  subprocess.check_call([ICPLC, filename] + ['-v'] * VERBOSITY)

def run (cmd):
  logging.info ('run (%s)' % ' '.join (cmd))
  subprocess.check_call(cmd)

def test_manual():
  test_prog('''Fact 0 = 1;
Fact n = * n (Fact (- n 1));
Fact 6;
''', '24')
  
  test_prog('''Nil/0; Cons/2;
Rev x = Rev1 Nil x;
Rev1 x Nil = x;
Rev1 x (Cons a y) = Rev1 (Cons a x) y;
Rev1 (Cons 2 (Cons 3 (Cons 9 Nil)));''', 'Cons 9 (Cons 3 (Cons 2 Nil))')

  test_prog('''N/0; C/2;
Map f N = N;
Map f (C a x) = C (f a) (Map f x);
Map (* 2) (C 2 (C 3 (C 9 N)));''', 'C 4 (C 6 (C 18 N))')

  test_prog('''N/0; C/2;
L = C 2 (C 3 (C 9 N));
Foldr f a N = a;
Foldr f a (C b x) = f b (Foldr f a x);
Foldr (+) 0 L;''', '14')

  test_prog('''N/0; C/2;
From n = C n (From (+ n 1));
Nat = From 1;
First 0 _ = N;
First n (C a x) = C a (First (- n 1) x);
First 5 Nat;''', 'C 1 (C 2 (C 3 (C 4 (C 5 N))))')

  test_prog('''N/0; C/2;
AddL (C a x) (C b y) = C (+ a b) (AddL x y);
Fib = C 1 (AddL Fib (C 1 Fib));
First 7 Fib;''', 'C 1 (C 2 (C 3 (C 5 (C 8 (C 13 (C 21 N))))))')

optlist, args = getopt.getopt (sys.argv[1:], 'v')
VERBOSITY = 0

for o, a in optlist:
  if o == '-v': VERBOSITY += 1

init()
test_manual()

