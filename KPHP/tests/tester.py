#!/usr/bin/python
import sys, os, random, logging, httplib, urllib, string, subprocess, shlex, time, re, shutil
from subprocess import call, check_call
from getopt import getopt, gnu_getopt
from glob import glob
import stat

kphp_path = os.environ.get('KPHP_PATH', None);
if not kphp_path:
  kphp_path = os.environ.get('VK_PATH', None);
if not kphp_path:
  print "No KPHP_PATH or VK_PATH found, default is used"
  kphp_path = os.path.expanduser ("~/engine/src.vk")
root_path = kphp_path + "/"

def run_make (args = []):
  check_call ("make -C %s -sj30" % root_path + " " + " ".join (args), shell = True)

def benchmark_prefix (f):
  if f:
    return "time "
  return ""

def run_php (dest, benchmark = False):
  cmd = benchmark_prefix (benchmark) + "php -n -d memory_limit=3072M %s > %s.php_out" % (dest, dest)
  check_call (cmd, shell = True)

def run_dl (dest, benchmark = False):
#  print "rm "
  print "make"
  make_cmd = '%sKPHP/tests/kphp.py -I tmp/ -o main %s 2> %s.parser_err ' % (root_path, dest, dest)
  check_call (make_cmd, shell = True)

  print "run"
  run_cmd = benchmark_prefix (benchmark) + "./main > %s.dl_out" % dest
  check_call (run_cmd, shell = True)

def prepare_test (src, dest, init_code = ""):
  src_f = open (src, "r")
  dest_f = open (dest, "w")

  dest_f.write (init_code)

  cur = src_f.readline()
  if cur and cur[0] != '@':
    dest_f.write (cur)
  while True:
    cur = src_f.readline()
    if not cur:
      break
    dest_f.write (cur)


  os.fchmod (dest_f.fileno(), stat.S_IRUSR)
  src_f.close()
  dest_f.close()

def run_test (test, init_code = ""):
  src_path = test.path
  src_dir, src_name = string.rsplit (src_path, '/', 1);
  dest = "tmp/" + src_name

  benchmark = "benchmark" in test.tags
  no_php = "no_php" in test.tags

  if no_php:
    init_code = ""

  prepare_test (src_path, dest, init_code)
  for x in test.to_copy:
    prepare_test (src_dir + "/" + x, "tmp/" + x)


  if not no_php:
    run_php (dest, benchmark)
    ans_suff = "php_out"

  run_dl (dest, benchmark);

  if no_php:
    if not os.path.isfile (src_path + ".ans"):
      print "  .ANS created!"
      cp_cmd = "cp %s.dl_out %s.ans" % (dest, src_path)
      check_call (cp_cmd, shell = True)
    else:
      print "  .ANS found"

    cp_cmd = "cp %s.ans %s.old_out" % (src_path, dest)
    check_call (cp_cmd, shell = True)
    ans_suff = "old_out"

  print "DIFF"
  diff_cmd = "diff %s.dl_out %s.%s" % (dest, dest, ans_suff);
  check_call (diff_cmd, shell = True)
  open(src_path + ".ok", 'w').close()

all_tags = set()
class Test:
  def __init__ (self, path, tags = set()):
    global all_tags

    self.path = path
    f = open (path, "r")
    header = f.readline()
    f.close()
    if header[0] == '@':
      self.tags = set(header[1:].split())
    else:
      self.tags = set (["none"])
    self.tags |= tags
    all_tags |= self.tags

    prefix = string.rsplit (path, '.', 1)[0]
    #print "----------"
    #print path
    #print prefix
    self.to_copy = glob (prefix + "*.php")
    #print self.tags
    #print self.to_copy;
    self.to_copy.remove (path)
    self.to_copy = map (lambda x: string.rsplit (x, '/', 1)[1], self.to_copy)
    #print self.to_copy;

def get_tests (tests, tags = []):
  tests = sorted (tests)
  tests = [Test (x, set(tags)) for x in tests]
  return tests

def get_tests_by_mask (mask, tags = []):
  return get_tests (glob (mask), tags);

def remove_ok_tests (tests, force = False, save_files = False):
  tmp_tests = []
  for x in tests:
    was = x.path + ".ok";
    if os.path.exists (was):
      if force:
        if not save_files:
          os.remove (was)
        tmp_tests.append (x)

    else:
      tmp_tests.append (x)
  return tmp_tests

def usage():
  print "DL_KPHP tester"
  print "usage: python tester.py [-t <tag>] [-l]"
  print "\t-a<tag> --- test must have given <tag>"
  print "\t-d<tag> --- test mustn't have given <tag>"
  print "\t-c<code> ---  <code> will be added to the beginning of each test"
  print "\t-l --- just print test names"
  print "\t-i --- test till infinity"

print_tests = False

try:
  opts, args = getopt(sys.argv[1:], "a:c:d:lhfi")
except:
  usage()
  exit(0)

need_tags = set()
bad_tags = set()

init_code = ""
force = False;
inf_flag = False;
for o, a in opts:
  if o == "-a":
    need_tags.add (a)
  elif o == '-c':
    init_code += "<?php " + a + " ?>\n"
  elif o == "-d":
    bad_tags.add (a)
  elif o == "-l":
    print_tests = True;
  elif o == "-h":
    usage()
    exit(0)
  elif o == "-f":
    force = True;
  elif o == "-i":
    inf_flag = True;
  else:
    usage()
    exit(0)

manual_tests = []
if args:
  manual_tests = get_tests (args, ["manual"])
  need_tags.add ("manual")

#REGISTER TESTS
dl_tests = get_tests_by_mask (root_path + "KPHP/tests/kphpt/*.php", ["dl"])
tests = []
tests.extend (dl_tests)
tests.extend (manual_tests)

print "All tags: ", all_tags

tests = filter (lambda x: need_tags <= x.tags and (not (bad_tags & x.tags)), tests)
tests = remove_ok_tests (tests, force, print_tests)
used_tags = reduce (lambda x, y: x | y.tags, tests, set())

print "Used tags: ", used_tags

if print_tests:
  for x in tests:
    print x.path
  print len (tests)
  exit(0)

run_make()

check_call ("rm -rf tmp", shell = True)
check_call ("mkdir tmp", shell = True)

run_flag = True
while run_flag:
  for test in tests:
    print "----------------------------------------------------------------------"
    print "Run: %s" % (test.path)
    check_call ("rm -rf tmp/*", shell = True)
    run_test (test, init_code)
  if not inf_flag:
    run_flag = False

