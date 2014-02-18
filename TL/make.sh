#!/bin/sh
#Arguments 
#$1 - combined.tl [input]
#$2 - combined2.tl (generated with magics) [output]
#$3 - combined.tlo [output]
#$4 - TL/constants.tl  [output]
[ -z $1 ] && exit 1
[ -z $2 ] && exit 1
[ -z $3 ] && exit 1
[ -z $4 ] && exit 1

objs/bin/tlc-new -w 2 -e $3 $1 -E 2>$2 || (echo "tlc failed"; exit 1)

awk '{ 
  if (split ($1, a, "#") == 2) { 
    gsub (/[A-Z]/, "_&", a[1]); 
    gsub (/[.]/, "_", a[1]); 
    print "#define", "TL_" toupper(a[1]), "0x" a[2];
  }
}' <$2 >$4



