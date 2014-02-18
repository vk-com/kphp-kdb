#!/bin/bash

#
# author: Oleg Davydov
#

function die() {
    echo "fatal: $*"
    exit 1
}

function write_int32() {
# usage: write_int32 <int>
    x="$(($1))"
    for i in {1..4}; do
        echo -ne '\x'`printf '%02x' $(($x % 256))`
        x=$(($x/256))
    done
}

[ $# -eq 3 ] || [ $# -eq 5 ] || die "usage: $0 <engine-code> <cluster-size> <index> [<extra-length> <extra>]"
code="$1"
total="$2"
index="$3"
if [ $# -ge 4 ]; then
    extra_length="$4"
    extra="$5"
else
    extra_length=0
fi

write_int32 0x044c644b
write_int32 $code
write_int32 $extra_length
write_int32 $total
write_int32 $index
write_int32 $(($index+1))
if [ $extra_length -gt 0 ]; then
    echo -ne "$extra"
fi

