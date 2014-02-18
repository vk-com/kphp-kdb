/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 * Created: 16.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef _WIN32
#  include <direct.h>
#  define getcwd _getcwd
#else
#  include <unistd.h>
#endif

#include "../antispam-common.h"
#include "../antispam-db.h"
#include "../kdb-antispam-binlog.h"
#include "string-processing.h"

#define TESTDIR_ANTISPAM "/var/www/pics2/engine-testers/antispam"
#define FILE_WITH_PATTERNS TESTDIR_ANTISPAM "/recent_patterns2.csv"

#define MAX_N 50000
#define MAX_STR_LEN 10000

extern vec_int_t antispam_db_request; // externed from "antispam-db.c"

static double start;

static char buffer[MAX_STR_LEN];

static char s[MAX_STR_LEN];
static char *parts[MAX_STR_LEN];
static int pn;

static ip_t _ip[MAX_N];
static uahash_t _uahash[MAX_N];
static int _id[MAX_N];
static int _flag[MAX_N];

static char *str[MAX_N];
static int K = 100, N = 0;

/**
 * Utility
 */

static bool separator (char c) {
  return c == 0xA || c == 0xD || c == '\t';
}

static void split (char *s) {
  pn = 0;
  while (TRUE) {
    int end = 0;
    while (!end && *s && separator (*s)) {
      if (*s == '\t') {
        end = 1;
      }
      *s++ = 0;
    }
    if (!*s) {
      break;
    }
    parts[pn++] = s;
    while (*s && !separator (*s)) {
      s++;
    }
  }
}

static double current_time () {
  return (clock() - start) / CLOCKS_PER_SEC;
}

static void memory_statistic () {
  st_printf ("[%6.2f] Used z-memory = $1%ld$^, Used dl-memory = $1%lld$^\n", current_time(), dyn_used_memory(), dl_get_memory_used());
}

/**
 * Main test
 */

static void answer_queries (const char *fname) {
  FILE *f_queries = fopen (TESTDIR_ANTISPAM "/input.txt", "rt");
  assert (f_queries);

  st_printf ("[%6.2f] answer_queries: start [save answer to $1'$3%s$1'$^]\n", current_time(), fname);

  FILE *f_answers = fopen (fname, "wt");
  assert ("fatal: couldn't open output file, maybe permissions error?" && f_answers);

  int cnt = 0;
  while (fgets (s, MAX_STR_LEN, f_queries)) {
    int ip, uahash;
    assert (strlen (s) > 0);
    s[strlen (s) - 1] = 0; // cut '\n'
    assert (fscanf (f_queries, "%d %d ", &ip, &uahash) == 2);

    fprintf (f_answers, "[%3d] search for \"%s\" with ip=%u and uhash=%u\n", ++cnt, s, ip, uahash);

    int i, sign, num = antispam_get_matches (ip, uahash, s, ANTISPAM_DB_FIELDS_IDS, 0); // just calculate number of matchings
    fprintf (f_answers, "[%3d] there are %d matchings\n", cnt, num);
    for (sign = -1; sign <= 1; sign += 2) { // test decreasing and increasing orders
      antispam_get_matches (ip, uahash, s, ANTISPAM_DB_FIELDS_IDS, K * sign);

      int vn = st_vec_size (antispam_db_request);
      const int *id = st_vec_to_array (antispam_db_request);
      //memory_statistic();
      for (i = 0; i < vn; i++) {
        int pos = 0;
        while (pos < N && _id[pos] != id[i]) {
          pos++;
        }
        assert (pos < N);

        fprintf (f_answers, "[%3d, i=%d] : pos=%7d, id=%10d, ip=%10u, uahash=%10u, flags=%d] \"%s\"\n", cnt, i, pos, _id[pos], _ip[pos], _uahash[pos], _flag[pos], str[pos]);
        antispam_serialize_pattern (_id[pos], s);
        sprintf (buffer, "%d,%u,%u,%u,%s", _id[pos], _ip[pos], _uahash[pos], _flag[pos], str[pos]);
        if ((strcmp (s, buffer) != 0)) {
          st_printf ("$1FAIL$^ [query = %d, i = %d, pos = %d]\nantispam_serialize_pattern = '%s'\nstring in the database     = '%s'\n", cnt, i, pos, s, buffer);
          assert (0);
        }
      }
    }
  }
  fclose (f_queries);
  fclose (f_answers);

  fprintf (stderr, "[%6.2f] answer_queries: OK\n", current_time());
}

/* Just add all read patterns to the trie */
static void add_patterns (void) {
  int i;
  fprintf (stderr, "[%6.2f] Add all patterns... [free bytes = %ld] ", current_time(), dyn_free_bytes());
  for (i = 0; i < N; i++) {
    antispam_pattern_t p;
    p.id = _id[i];
    p.ip = _ip[i];
    p.flags = _flag[i];
    p.uahash = _uahash[i];
    antispam_add (p, str[i], FALSE);
  }
  fprintf (stderr, "OK [free bytes = %ld]\n", dyn_free_bytes());
  memory_statistic();
}

/* Just del all read patterns to the trie */
static void del_patterns (void) {
  int i;
  fprintf (stderr, "[%6.2f] Del all patterns... [free bytes = %ld] ", current_time(), dyn_free_bytes());
  for (i = 0; i < N; i++) {
    antispam_del (_id[i]);
  }

  // Try to del inexisting pattern
  // Suppose that there is no such id in trie
  antispam_del (178537843);

  fprintf (stderr, "OK [free bytes = %ld]\n", dyn_free_bytes());
  memory_statistic();
}

int main (int argc, char *argv[]) {
  FILE *f_index;
  int i;

  start = clock();
  st_printf ("Reading from $1'$3%s$1'$^\n", TESTDIR_ANTISPAM);
  assert (f_index = fopen (FILE_WITH_PATTERNS, "rt"));  // recent_patterns.txt
  for (i = 1; i < argc; ) {
    if (!strcmp (argv[i], "-k")) { // how many matches to output
      assert (++i < argc);
      assert (sscanf (argv[i++], "%d", &K) == 1);
    }
  }

  init_dyn_data();
  srand48 (239);
  assert (fgets (s, MAX_STR_LEN, f_index)); // skip header
  while (fgets (s, MAX_STR_LEN, f_index)) {
    //fprintf (stderr, "s = '%s'\n", s);
    split (s);
    //fprintf (stderr, "pn = %d\n", pn);
    //int tt;
    //for (tt = 0; tt < pn; tt++)
    //  fprintf (stderr, "%d: %s\n", tt, parts[tt]);
    assert (pn == 5); // {0=id, 1=ip, 2=uahash, 3=flag, 4=string}
    assert (N < MAX_N);
    assert (sscanf (parts[0], "%u", &_id[N]) == 1);
    assert (sscanf (parts[1], "%u", &_ip[N]) == 1);
    assert (sscanf (parts[2], "%u", &_uahash[N]) == 1);
    assert (sscanf (parts[3], "%u", &_flag[N]) == 1);
    if (_flag[N] & 16) {
      _flag[N] = 1;
       str[N++] = strdup (sp_simplify (parts[4]));
    } else if (_flag[N] & 32) {
      _flag[N] = 2;
       str[N++] = strdup (sp_full_simplify (parts[4]));
    } else {
      _flag[N] = 0;
       str[N++] = strdup (parts[4]);
    }
  }
  fclose (f_index);
  fprintf (stderr, "[%6.2f] Data is read. N = %d\n", current_time(), N);

  int first = 1;
  for (i = 0; i < 2; ++i) {

    static char current_path[FILENAME_MAX];

    //getcwd (current_path, sizeof (current_path) / sizeof (char));
    sprintf (current_path, "%s", TESTDIR_ANTISPAM);
    st_printf ("Output to $1'$3%s$1'$^\n", current_path);

    if (!first) {
      st_printf ("[copy]\n");
      assert (!system ("cp "TESTDIR_ANTISPAM"/output1.txt "TESTDIR_ANTISPAM"/output1.txt.backup"));
    }

    antispam_init();
    add_patterns();
    answer_queries (TESTDIR_ANTISPAM"/output1.txt");
    //antispam_out_all();
    //TODO: fprintf (stderr, "[%6.2f] max len = %d\n", current_time(), antispam_stat_max_list_len());
    del_patterns();
    add_patterns();
    answer_queries (TESTDIR_ANTISPAM"/output2.txt");
    del_patterns();
    antispam_finish();
    memory_statistic();

    st_printf ("[diff-1]\n");
    assert (!system ("diff "TESTDIR_ANTISPAM"/output1.txt "TESTDIR_ANTISPAM"/output2.txt"));
    if (!first) {
      st_printf ("[diff-2]\n");
      assert (!system ("diff "TESTDIR_ANTISPAM"/output1.txt "TESTDIR_ANTISPAM"/output1.txt.backup"));
    }

    first = 0;
  }
  return 0;
}
