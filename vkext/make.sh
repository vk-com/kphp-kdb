#!/bin/bash
phpize
CFLAGS="-O3 -Wall" ./configure
make
