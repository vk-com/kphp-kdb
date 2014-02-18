#!/bin/sh
SCHEME=/tmp/.scheme
pushd ~/engine/src 1>/dev/null
objs/bin/tlc-new -w 2 -e objs/bin/combined.tlo objs/bin/combined.tl -E 2>${SCHEME} || (echo "tlc failed"; exit 1)

awk '{ 
  if (split ($1, a, "#") == 2) { 
    gsub (/[A-Z]/, "_&", a[1]); 
    gsub (/[.]/, "_", a[1]); 
    print "#define", "TL_" toupper(a[1]), "0x" a[2];
  }
}' <${SCHEME}

rm ${SCHEME}
popd 1>/dev/null


