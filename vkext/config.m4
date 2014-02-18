CFLAGS="-O3"
LDFLAGS="-lrt"
PHP_ARG_ENABLE(vkext, whether to enable kitten-extension support)
if test "$PHP_VKEXT" = "yes"; then
  AC_DEFINE(HAVE_VKEXT, 1, [Whether you have test-kitten-php])
  PHP_NEW_EXTENSION(vkext, vkext.c vkext_iconv.c vkext_flex.c vkext_rpc.c crc32.c vkext_tl_parse.c vkext_tl_memcache.c vkext_schema_memcache.c, $ext_shared,,-Wall -O3 -march=core2 -mfpmath=sse -mssse3 -ggdb -fno-strict-aliasing -fno-strict-overflow -Werror)
fi
