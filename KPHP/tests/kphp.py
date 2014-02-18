#!/usr/bin/python
import sys, os, random, logging, httplib, urllib, string, subprocess, shlex, time, re, shutil
from subprocess import call, check_call
from getopt import getopt, gnu_getopt
from glob import glob

kphp_path = os.environ.get('KPHP_PATH', None);
if not kphp_path:
  kphp_path = os.environ.get('VK_PATH', None);
if not kphp_path:
  print "No KPHP_PATH or VK_PATH found, default is used"
  kphp_path = os.path.expanduser ("~/engine/src.vk")

objs = "objs"
bin = "bin"

#TODO: add p=1 support
p = os.environ.get('p', None);
if p:
  objs += 'p'
  bin += 'p'

root_path = kphp_path + "/"
root_objs_path = root_path + objs + "/"
root_bin_path = root_objs_path + 'bin' + "/"
parser_path = root_bin_path + "kphp2cpp"
tmp_base_dir = root_path + "KPHP/tests/kphp_tmp/"

functions_txt_path = root_path + "KPHP/functions.txt"
kphp_makefile_path = root_path + "KPHP/tests/Makefile"
dir_list = []
main_php = None
main_php_name = None
dest = None
tmp_dir = None
use_net = False
verbosity = 0

def main():
  dbg ("Parse args")
  parse_args()
  dbg ("Main php file [%s]" % (main_php))

  dbg ("Run make kphp")
  run_make(["kphp"])

  dbg ("Run parser")
  run_parser()

  dbg ("Compile c++")
  run_cpp (use_net)

def dbg (s):
  if verbosity > 0:
    print s

def parse_args():
  try:
    opts, args = gnu_getopt (sys.argv[1:], "I:no:v")
  except:
    usage()
    exit (0)


  global use_net
  global dest
  global verbosity
  for code, arg in opts:
    if code == "-I" :
      dir_list.append (arg)
    elif code == "-n":
      use_net = True
    elif code == "-o":
      dest = arg
    elif code == "-v":
      verbosity += 1
    else:
      usage()
      exit(0)

  if args:
    global main_php
    global main_php_full
    global main_php_name

    main_php = []
    main_php_full = parser_path
    for entry in reversed (args):
      main_php.append (entry)

      #dir_list is not used, so path must be strict
      entry_full = os.path.abspath(entry)
      main_php_full += entry_full
      main_php_name = entry_full.split('/')[-1].rsplit('.', 1)[0].replace('.', '_').replace('-', '_')

    main_php.reverse()
  else:
    usage()
    exit(0)

def get_tmp_dir():
  global tmp_dir
  if not tmp_dir:
    h = 0
    for x in main_php_full:
      h = h * 31 + ord(x)
      h &= (1 << 31) - 1

    tmp_dir = "%s.%s--%x" % (tmp_base_dir, main_php_name, h)

  return tmp_dir

def get_dest():
  global dest
  if not dest:
    dest = main_php_name
  return dest

def run_make (args = []):
  check_call ("make -C %s -sj30" % root_path + " " + " ".join (args), shell = True)

def run_parser():
  run_parser_cmd = " ".join ([
                     #"valgrind --tool=callgrind",
                     parser_path,
                     " ".join (main_php),
                     "-f", functions_txt_path,
                     "-d", get_tmp_dir(),
                     "-r",
#                     "-a",
#                    "-i", "kphp_index",
                     "-I " + " -I ".join (dir_list) if dir_list else ""
                 ])
  print run_parser_cmd
  check_call (run_parser_cmd, shell = True)


def run_cpp(use_net = False):
  run_cpp_cmd = " ".join ([
                    "nice -n19 make",
                    "-sj30",
                    "-k",
                    "-f", kphp_makefile_path,
                    "-C", get_tmp_dir(),
                    "engine" if use_net else ""
                    ])
  print run_cpp_cmd
  check_call (run_cpp_cmd, shell = True)

  if use_net:
    link_engine()
  else:
    link_simple()

def link_engine():
  copy_cmd = " ".join ([
                 "cp",
                 get_tmp_dir() + "/" + bin + "/engine",
                 get_dest()
                 ])
  print copy_cmd
  check_call (copy_cmd, shell = True)

def link_simple():
  copy_cmd = " ".join ([
                 "cp",
                 get_tmp_dir() + "/" + bin + "/main",
                 get_dest()
                 ])
  print copy_cmd
  check_call (copy_cmd, shell = True)

def link_as_server():
  global p;
  link_cmd = " ".join ([
                 "g++",
                 "-pg" if p else "",
                 root_objs_path + "KPHP/php-engine_.o",
                 "-lm -lz -lpthread -lrt -lcrypto -ggdb -rdynamic -lpcre -lre2",
                 get_tmp_dir() + "/" + objs +"/script.o",
                 "-o", get_dest()])
  print link_cmd
  check_call (link_cmd, shell = True)

def usage():
  print "Kitten compiler for PHP"
  print "usage: kphp <main.php> [-I <include_dir>] [-o <dest>] [-n] [-v]"
  print "-v\tverbosity"
  print "-n\tcompile as server"


main()
