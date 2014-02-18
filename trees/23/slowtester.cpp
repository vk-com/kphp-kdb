#include <cstdio>
#include <set>

using namespace std;

set <int> s;

int main () {
  int n;
  scanf ("%d", &n);
  for (int i = 0; i < n; i++) {
    int t, p;
    scanf ("%d", &t);
    switch (t) {
      case 1:
        scanf ("%d", &p);
        s.insert (p);
        break;
      case 2:
        scanf ("%d", &p);
        puts (s.find (p) != s.end () ? "YES" : "NO");
        break;
      case 3:
        scanf ("%d", &p);
        for (set <int>::iterator it = s.begin (); it != s.end (); it++)
          printf ("%d\n", *it);
        break;
      case 4:
        scanf ("%d", &p);
        s.erase (p);
        break;
    }
  }
}
