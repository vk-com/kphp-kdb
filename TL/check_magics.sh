pushd ~/engine/src 1>/dev/null
SCHEME=/tmp/.scheme
objs/bin/tlc-new -w 2 -e objs/bin/combined.tlo objs/bin/combined.tl -E 2>${SCHEME} || (echo "tlc failed"; exit 1)

function check () {
  echo "Checking " $1
  h=$1/$1-tl.h
  if [ $1 == "friend" ] ; then
    h='friend/friends-tl.h'
  elif [ $1 == "common" ] ; then
    h='common/rpc-const.h'
  fi  
  a=`awk '/#define/ {if (substr($3,1,2)=="0x") {print substr($3,3);} }' < ${h} `
  for s in $a; do
    grep -q \#$s ${SCHEME} || ( echo -n $s "not found - "; grep --color 0x$s $h;  )
  done
}

#check "common"
check "friend"
check "lists"
check "news"
check "search"
check "text"
check "photo"
check "cache"

rm ${SCHEME}
popd 1>/dev/null
