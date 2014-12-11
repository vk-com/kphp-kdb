ifeq ($m, 32)
OBJ	=	objs32
DEP	=	dep32
MF	=	-m32
GM_LIBS_FLAGS = -L/usr/local/lib32 -L/usr/lib32 -L/lib32
FUSE_LIBS_FLAGS = -L/usr/lib32
else
OBJ	=	objs
DEP	=	dep
MF	=	-m64
GM_LIBS_FLAGS = -L/usr/local/lib -L/usr/lib
FUSE_LIBS_FLAGS = -L/usr/lib
endif

ifeq ($p, 1)
OBJ	:=	${OBJ}p
DEP	:=	${DEP}p
PROF	=	-pg
PROFC	=	-pg -DDEBUG=1
OPT	=
else
PROF	=
OPT	=	-O3
endif

# CWARN = -Wall -Werror
# CWARN = -Wall -Werror -Wno-error=unused-result -Wno-error=unused-but-set-variable
CWARN = -Wall

CFLAGS = ${CWARN} ${OPT} ${MF} -march=core2 -mfpmath=sse -mssse3 -ggdb -fno-strict-aliasing -fno-strict-overflow -fwrapv ${PROFC}

ifeq (${fp}, 1)
CFLAGS := ${CFLAGS} -fno-omit-frame-pointer
endif

CXXFLAGS = ${CFLAGS} -fno-omit-frame-pointer

DFLAGS = -M
LDFLAGS = ${MF} -ggdb -rdynamic ${PROF} -lm -lrt -lcrypto -lz -lpthread
LDFLAGSSSL	= ${LDFLAGS} -lcrypto

LDFLAGSGM := ${GM_LIBS_FLAGS} ${LDFLAGS} -l:libGraphicsMagick.a -l:libtiff.a -l:libfreetype.a -l:libjasper.a -l:libjpeg.a -l:libpng.a -l:libbz2.a -lz -lm -l:libgomp.a -lpthread
ifneq ($m, 32)
LDFLAGSGM += -l:libwebp.a
endif

INCLUDEGM = -I/usr/local/include/GraphicsMagick -I/usr/include/GraphicsMagick

EXE	=	${OBJ}/bin
CINCLUDE	= -I common -I binlog -I net -I kfs -I drinkless -I skat -I vv -I ${OBJ} -I .

COMMIT := $(shell git log -1 --pretty=format:"%H")
CFLAGS := ${CFLAGS} -DCOMMIT=\"${COMMIT}\"

ifeq ($(shell whoami),root)
  SUPERUSER_FLAGS:=-DSUPERUSER
endif

PROJECTS = binlog cache common copyexec db-proxy friend kfs \
		lists mc-proxy money monitor msg-search net news \
		search statsx targ text util crypto \
		memcached pmemcached hints bayes isearch logs magus mutual-friends \
		drinkless queue poll watchcat image filesys random storage weights dns dhcp \
		skat antispam KPHP KPHP/compiler KPHP/runtime \
		spell letters photo copyfast support geoip \
		TL vv rpc-proxy vkext seqmap

OBJDIRS := ${OBJ} $(addprefix ${OBJ}/,${PROJECTS}) ${EXE}
DEPDIRS := ${DEP} $(addprefix ${DEP}/,${PROJECTS})
ALLDIRS := ${DEPDIRS} ${OBJDIRS}


.PHONY:	all clean dist lists-x lists-y lists-z lists-w search image filesys kphp storage spell statsx dirs create_dirs_and_headers tl binlog

EXELIST	:= \
	${EXE}/check-binlog ${EXE}/fix-rotateto ${EXE}/tag-binlog \
	${EXE}/targ-engine ${EXE}/targ-import-dump ${EXE}/targ-log-merge ${EXE}/targ-log-split \
	${EXE}/statsx-engine ${EXE}/statsx-binlog ${EXE}/statsx-log-split \
	${EXE}/search-import-dump ${EXE}/search-log-split \
	${EXE}/friend-import-dump ${EXE}/friend-engine ${EXE}/friend-log-merge ${EXE}/friend-log-split \
	${EXE}/news-engine ${EXE}/news-binlog ${EXE}/news-log-split ${EXE}/news-import-dump \
	${EXE}/lists-import-dump ${EXE}/lists-engine ${EXE}/lists-log-merge \
	${EXE}/lists-x-engine ${EXE}/lists-y-engine ${EXE}/lists-z-engine ${EXE}/lists-w-engine \
	${EXE}/lists-binlog ${EXE}/lists-x-binlog ${EXE}/lists-y-binlog ${EXE}/lists-z-binlog ${EXE}/lists-log-split \
	${EXE}/mc-proxy ${EXE}/mc-proxy-search ${EXE}/rpc-proxy ${EXE}/db-proxy \
	${EXE}/text-import-dump ${EXE}/text-index ${EXE}/text-engine ${EXE}/text-binlog \
	${EXE}/text-log-merge ${EXE}/text-log-split \
	${EXE}/money-engine ${EXE}/money-import-dump \
	${EXE}/memcached \
	${EXE}/pmemcached-ram ${EXE}/pmemcached-disk ${EXE}/pmemcached-import-dump \
	${EXE}/pmemcached-binlog ${EXE}/pmemcached-log-split \
	${EXE}/targ-recover \
	${EXE}/replicator ${EXE}/backup-engine \
	${EXE}/hints-engine ${EXE}/hints-log-split ${EXE}/rating-engine ${EXE}/bayes-engine \
	${EXE}/mf-prepare-file ${EXE}/mf-merge-files ${EXE}/mf-xor \
	${EXE}/mf-engine ${EXE}/mf-process-file \
	${EXE}/isearch-engine ${EXE}/isearch-x-engine ${EXE}/isearch-interests-engine \
	${EXE}/logs-engine ${EXE}/logs-merge-dumps ${EXE}/logs-merge-stats \
	${EXE}/lists-binlog ${EXE}/queue-engine ${EXE}/poll-engine ${EXE}/watchcat-engine \
	${EXE}/magus-precalc ${EXE}/magus-engine \
	${EXE}/search-engine ${EXE}/search-index ${EXE}/search-binlog ${EXE}/search-y-engine ${EXE}/search-y-index \
	${EXE}/search-x-index ${EXE}/search-x-engine \
	${EXE}/truncate ${EXE}/crc32 \
	${EXE}/weights-engine \
	${EXE}/dns-engine ${EXE}/dns-binlog-diff ${EXE}/tftp ${EXE}/dhcp-engine \
	${EXE}/filesys-commit-changes ${EXE}/filesys-xfs-engine \
	${EXE}/cache-engine ${EXE}/cache-simulator ${EXE}/cache-binlog ${EXE}/cache-log-split \
	${EXE}/storage-engine ${EXE}/storage-import ${EXE}/storage-append ${EXE}/storage-binlog-check ${EXE}/storage-binlog \
	${EXE}/letters-engine \
	${EXE}/photo-engine ${EXE}/photo-import-dump ${EXE}/photo-log-split \
	${EXE}/copyfast-server ${EXE}/copyfast-engine \
	${EXE}/copyexec-commit ${EXE}/copyexec-engine ${EXE}/copyexec-binlog ${EXE}/copyexec-results-engine \
	${EXE}/tlc-new ${EXE}/icplc \
	${EXE}/random-engine \
	${EXE}/support-engine \
	${EXE}/antispam-engine \
	${EXE}/antispam-import-dump \
	${EXE}/geoip ${EXE}/geoip_v6 \
	${EXE}/seqmap-engine \
	${EXE}/rpc-proxy-delete-old

DISTRDIR =	${HOME}/engine-inst
KFSOBJS = ${OBJ}/kfs/kfs.o ${OBJ}/common/xz_dec.o ${OBJ}/crypto/aesni256.o ${OBJ}/common/sha1.o
SRVOBJS	=	${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o ${OBJ}/common/pid.o \
			${OBJ}/common/crc32.o ${OBJ}/common/md5.o ${OBJ}/common/sha1.o ${OBJ}/common/common-data.o \
			${OBJ}/binlog/kdb-binlog-common.o ${KFSOBJS} \
			${OBJ}/net/net-events.o ${OBJ}/net/net-buffers.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-msg-buffers.o \
			${OBJ}/net/net-crypto-aes.o
RPCOBJS	=	${OBJ}/net/net-connections.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o \
			${OBJ}/net/net-rpc-common.o ${OBJ}/net/net-memcache-server.o
RPC_PROXY_OBJS	=	${OBJ}/rpc-proxy/rpc-proxy-merge.o ${OBJ}/rpc-proxy/rpc-proxy-merge-diagonal.o ${OBJ}/news/rpc-proxy-merge-news.o ${OBJ}/news/rpc-proxy-merge-news-r.o ${OBJ}/rpc-proxy/rpc-proxy-double-send.o ${OBJ}/rpc-proxy/rpc-proxy-string-forward.o ${OBJ}/rpc-proxy/rpc-proxy-points.o ${OBJ}/lists/rpc-proxy-lists.o ${OBJ}/friend/rpc-proxy-friend.o ${OBJ}/memcached/rpc-proxy-memcached.o ${OBJ}/news/rpc-proxy-news.o ${OBJ}/search/rpc-proxy-search.o ${OBJ}/seqmap/rpc-proxy-seqmap.o ${OBJ}/statsx/rpc-proxy-statsx.o ${OBJ}/hints/rpc-proxy-hints.o ${OBJ}/hints/rpc-proxy-merge-hints.o ${OBJ}/photo/rpc-proxy-photo.o ${OBJ}/text/rpc-proxy-text.o ${OBJ}/rpc-proxy/rpc-proxy-any.o ${OBJ}/rpc-proxy/rpc-proxy-kitten-php.o ${OBJ}/rpc-proxy/rpc-proxy-secure-send.o ${OBJ}/rpc-proxy/rpc-proxy-binlog.o ${OBJ}/targ/rpc-proxy-targ.o ${OBJ}/weights/rpc-proxy-weights.o 
DLSTD		=	${OBJ}/drinkless/dl-utils.o ${OBJ}/drinkless/dl-crypto.o ${OBJ}/drinkless/dl-aho.o ${OBJ}/drinkless/dl-utils-lite.o ${OBJ}/common/crc32.o
DLDEF		=	${OBJ}/drinkless/dl-utils.o ${OBJ}/drinkless/dl-crypto.o ${OBJ}/drinkless/dl-aho.o ${OBJ}/drinkless/dl-utils-lite.o ${OBJ}/drinkless/dl-perm.o ${OBJ}/common/crc32.o
SKATOBJS	=	${OBJ}/skat/st-utils.o ${OBJ}/skat/st-hash.o ${OBJ}/skat/st-hash-set.o ${OBJ}/skat/st-memtest.o ${OBJ}/skat/st-numeric.o
KPHPOBJS	=	${OBJ}/KPHP/runtime/allocator.o ${OBJ}/KPHP/runtime/array_functions.o ${OBJ}/KPHP/runtime/bcmath.o ${OBJ}/KPHP/runtime/datetime.o ${OBJ}/KPHP/runtime/drivers.o ${OBJ}/KPHP/runtime/exception.o ${OBJ}/KPHP/runtime/files.o ${OBJ}/KPHP/runtime/interface.o ${OBJ}/KPHP/runtime/math_functions.o ${OBJ}/KPHP/runtime/mbstring.o ${OBJ}/KPHP/runtime/misc.o ${OBJ}/KPHP/runtime/openssl.o ${OBJ}/KPHP/runtime/regexp.o ${OBJ}/KPHP/runtime/rpc.o ${OBJ}/KPHP/runtime/string_functions.o ${OBJ}/KPHP/runtime/url.o ${OBJ}/KPHP/runtime/zlib.o ${OBJ}/KPHP/runtime/gost_hash.o
KPHP2CPPOBJS = ${OBJ}/KPHP/kphp2cpp.o ${OBJ}/KPHP/compiler/bicycle.o ${OBJ}/KPHP/compiler/cfg.o ${OBJ}/KPHP/compiler/compiler.o ${OBJ}/KPHP/compiler/data.o ${OBJ}/KPHP/compiler/gentree.o ${OBJ}/KPHP/compiler/io.o ${OBJ}/KPHP/compiler/lexer.o ${OBJ}/KPHP/compiler/name-gen.o ${OBJ}/KPHP/compiler/operation.o ${OBJ}/KPHP/compiler/stage.o ${OBJ}/KPHP/compiler/token.o ${OBJ}/KPHP/compiler/types.o ${OBJ}/KPHP/compiler/vertex.o ${OBJ}/KPHP/compiler/type-inferer.o ${OBJ}/KPHP/compiler/type-inferer-core.o ${OBJ}/KPHP/compiler/compiler-core.o ${OBJ}/KPHP/compiler/pass-rl.o ${OBJ}/KPHP/compiler/pass-ub.o

OBJECTS_CXX	= \
    ${OBJ}/KPHP/runtime/allocator.o ${OBJ}/KPHP/runtime/array_functions.o ${OBJ}/KPHP/runtime/bcmath.o ${OBJ}/KPHP/runtime/datetime.o \
    ${OBJ}/KPHP/runtime/drivers.o ${OBJ}/KPHP/runtime/exception.o ${OBJ}/KPHP/runtime/files.o ${OBJ}/KPHP/runtime/interface.o \
    ${OBJ}/KPHP/runtime/math_functions.o ${OBJ}/KPHP/runtime/mbstring.o ${OBJ}/KPHP/runtime/misc.o ${OBJ}/KPHP/runtime/openssl.o \
    ${OBJ}/KPHP/runtime/regexp.o ${OBJ}/KPHP/runtime/rpc.o ${OBJ}/KPHP/runtime/string_functions.o ${OBJ}/KPHP/runtime/url.o \
    ${OBJ}/KPHP/runtime/zlib.o ${OBJ}/KPHP/runtime/gost_hash.o \
    ${OBJ}/KPHP/php-runner.o ${OBJ}/KPHP/php-queries.o ${OBJ}/KPHP/php_script.o ${OBJ}/KPHP/php-master.o\
    ${KPHP2CPPOBJS}

OBJECTS	=	\
    ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/binlog/check-binlog.o ${OBJ}/binlog/pack-binlog.o ${OBJ}/binlog/fix-rotateto.o ${OBJ}/binlog/tag-binlog.o \
    ${OBJ}/common/base64.o ${OBJ}/common/crc32c.o ${OBJ}/common/md5.o ${OBJ}/common/sha1.o ${OBJ}/common/xz_dec.o \
    ${OBJ}/common/estimate-split.o \
    ${OBJ}/common/listcomp.o ${OBJ}/common/suffix-array.o ${OBJ}/common/diff-patch.o \
    ${OBJ}/common/fast-backtrace.o ${OBJ}/common/resolver.o \
    ${OBJ}/common/stemmer.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/translit.o ${OBJ}/common/word-split.o ${OBJ}/common/utf8_utils.o \
    ${OBJ}/common/string-processing.o \
    ${OBJ}/common/common-data.o \
    ${OBJ}/common/unicode-utils.o \
    ${OBJ}/common/aho-kmp.o \
    ${OBJ}/monitor/monitor-common.o \
    ${OBJ}/drinkless/dl-aho.o ${OBJ}/drinkless/dl-perm.o ${OBJ}/drinkless/dl-crypto.o ${OBJ}/drinkless/dl-utils.o ${OBJ}/drinkless/dl-utils-lite.o \
    ${OBJ}/kfs/kfs.o \
    ${OBJ}/net/net-aio.o ${OBJ}/net/net-buffers.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-crypto-aes.o ${OBJ}/net/net-crypto-rsa.o \
    ${OBJ}/net/net-events.o ${OBJ}/net/net-http-server.o ${OBJ}/net/net-http-client.o ${OBJ}/net/net-parse.o \
    ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-udp.o \
    ${OBJ}/net/net-rpc-common.o ${OBJ}/common/pid.o \
    ${OBJ}/net/net-rpc-targets.o \
    ${OBJ}/util/backup-engine.o ${OBJ}/util/replicator.o ${OBJ}/util/truncate.o ${OBJ}/util/crc32.o \
    ${OBJ}/bayes/bayes-data.o ${OBJ}/bayes/bayes-engine.o ${OBJ}/bayes/hash_table.o ${OBJ}/bayes/utils.o \
    ${OBJ}/db-proxy/db-proxy.o \
    ${OBJ}/friend/friend-data.o ${OBJ}/friend/friend-engine.o ${OBJ}/friend/friend-import-dump.o ${OBJ}/friend/friend-log-merge.o ${OBJ}/friend/friend-log-split.o \
    ${OBJ}/hints/hints-data.o ${OBJ}/hints/hints-engine.o ${OBJ}/hints/hints-log-split.o \
    ${OBJ}/hints/hash_table.o ${OBJ}/hints/maccub.o ${OBJ}/hints/perfect-hashing.o ${OBJ}/hints/treap.o ${OBJ}/hints/utf8_utils.o ${OBJ}/hints/utils.o \
    ${OBJ}/isearch/dl.o ${OBJ}/isearch/utf8_utils.o \
    ${OBJ}/letters/letters-data.o ${OBJ}/letters/letters-engine.o \
    ${OBJ}/lists/lists-binlog.o ${OBJ}/lists/lists-data.o ${OBJ}/lists/lists-engine.o \
    ${OBJ}/lists/lists-import-dump.o ${OBJ}/lists/lists-log-merge.o ${OBJ}/lists/lists-log-split.o \
    ${OBJ}/logs/dl.o ${OBJ}/logs/logs-data.o ${OBJ}/logs/logs-engine.o ${OBJ}/logs/logs-merge-dumps.o ${OBJ}/logs/logs-merge-stats.o \
    ${OBJ}/mc-proxy/mc-proxy.o ${OBJ}/mc-proxy/mc-proxy-merge-extension.o ${OBJ}/mc-proxy/mc-proxy-news-extension.o ${OBJ}/mc-proxy/mc-proxy-news-recommend-extension.o ${OBJ}/mc-proxy/mc-proxy-random-extension.o ${OBJ}/mc-proxy/mc-proxy-search-extension.o ${OBJ}/mc-proxy/mc-proxy-statsx-extension.o ${OBJ}/mc-proxy/mc-proxy-friends-extension.o ${OBJ}/mc-proxy/mc-proxy-targ-extension.o \
    ${OBJ}/memcached/memcached-data.o ${OBJ}/memcached/memcached-engine.o \
    ${OBJ}/money/money-data.o ${OBJ}/money/money-engine.o ${OBJ}/money/money-import-dump.o \
    ${OBJ}/magus/dl.o ${OBJ}/magus/magus-data.o ${OBJ}/magus/magus-engine.o ${OBJ}/magus/magus-precalc.o \
    ${OBJ}/mutual-friends/mf-data.o ${OBJ}/mutual-friends/mf-engine.o \
    ${OBJ}/mutual-friends/mf-merge-files.o ${OBJ}/mutual-friends/mf-prepare-file.o \
    ${OBJ}/mutual-friends/mf-process-file.o ${OBJ}/mutual-friends/mf-xor.o \
    ${OBJ}/mutual-friends/hash_table.o ${OBJ}/mutual-friends/maccub.o ${OBJ}/mutual-friends/treap.o ${OBJ}/mutual-friends/utils.o \
    ${OBJ}/news/news-binlog.o ${OBJ}/news/news-data.o ${OBJ}/news/news-engine.o ${OBJ}/news/news-log-split.o ${OBJ}/news/news-import-dump.o \
    ${OBJ}/photo/photo-engine.o ${OBJ}/photo/photo-data.o ${OBJ}/photo/utils.o ${OBJ}/photo/dl.o ${OBJ}/photo/photo-import-dump.o ${OBJ}/photo/photo-log-split.o \
    ${OBJ}/pmemcached/pmemcached-binlog.o ${OBJ}/pmemcached/pmemcached-data.o ${OBJ}/pmemcached/pmemcached-engine.o \
    ${OBJ}/pmemcached/pmemcached-index-disk.o ${OBJ}/pmemcached/pmemcached-index-ram.o ${OBJ}/pmemcached/pmemcached-import-dump.o \
    ${OBJ}/pmemcached/pmemcached-log-split.o \
    ${OBJ}/queue/queue-data.o ${OBJ}/queue/queue-engine.o ${OBJ}/queue/utils.o \
    ${OBJ}/poll/poll-data.o ${OBJ}/poll/poll-engine.o ${OBJ}/poll/utils.o \
    ${OBJ}/search/search-binlog.o ${OBJ}/search/search-data.o ${OBJ}/search/search-engine.o \
    ${OBJ}/search/search-index.o ${OBJ}/search/search-index-layout.o ${OBJ}/search/utils.o ${OBJ}/search/search-value-buffer.o \
    ${OBJ}/search/search-x-data.o ${OBJ}/search/search-x-engine.o ${OBJ}/search/search-common.o ${OBJ}/search/search-profile.o \
    ${OBJ}/search/search-y-index.o ${OBJ}/search/search-y-data.o ${OBJ}/search/search-y-engine.o ${OBJ}/common/search-y-parse.o \
    ${OBJ}/search/search-import-dump.o ${OBJ}/search/search-log-split.o \
    ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${OBJ}/vv/am-server-functions.o ${OBJ}/vv/am-amortization.o \
    ${OBJ}/statsx/statsx-binlog.o ${OBJ}/statsx/statsx-data.o ${OBJ}/statsx/statsx-engine.o ${OBJ}/statsx/statsx-log-split.o \
    ${OBJ}/support/support-data.o ${OBJ}/support/support-engine.o \
    ${OBJ}/skat/st-utils.o ${OBJ}/skat/st-hash.o ${OBJ}/skat/st-hash-set.o ${OBJ}/skat/st-memtest.o \
    ${OBJ}/skat/st-numeric.o \
    ${OBJ}/antispam/antispam-engine.o ${OBJ}/antispam/antispam-import-dump.o ${OBJ}/antispam/antispam-engine-impl.o ${OBJ}/antispam/antispam-data.o ${OBJ}/antispam/antispam-db.o \
    ${OBJ}/targ/targ-data.o ${OBJ}/targ/targ-index.o ${OBJ}/targ/targ-search.o ${OBJ}/targ/targ-engine.o ${OBJ}/targ/targ-weights.o \
    ${OBJ}/targ/targ-import-dump.o ${OBJ}/targ/targ-log-merge.o ${OBJ}/targ/targ-log-split.o \
    ${OBJ}/targ/targ-recover.o ${OBJ}/targ/targ-trees.o \
    ${OBJ}/text/text-data.o ${OBJ}/text/text-engine.o ${OBJ}/text/text-index.o ${OBJ}/text/text-binlog.o \
    ${OBJ}/text/text-import-dump.o ${OBJ}/text/text-log-merge.o ${OBJ}/text/text-log-split.o \
    ${OBJ}/watchcat/watchcat-data.o ${OBJ}/watchcat/utils.o ${OBJ}/watchcat/watchcat-engine.o \
    ${OBJ}/filesys/filesys-engine.o ${OBJ}/filesys/filesys-data.o ${OBJ}/filesys/filesys-memcache.o ${OBJ}/filesys/filesys-commit-changes.o ${OBJ}/filesys/filesys-utils.o ${OBJ}/filesys/filesys-xfs-engine.o ${OBJ}/filesys/filesys-pending-operations.o \
    ${OBJ}/cache/cache-engine.o ${OBJ}/cache/cache-data.o ${OBJ}/cache/cache-heap.o ${OBJ}/cache/cache-simulator.o ${OBJ}/cache/cache-binlog.o ${OBJ}/cache/cache-log-split.o \
    ${OBJ}/copyexec/copyexec-commit.o ${OBJ}/copyexec/copyexec-engine.o ${OBJ}/copyexec/copyexec-binlog.o ${OBJ}/copyexec/copyexec-err.o ${OBJ}/copyexec/copyexec-results-data.o ${OBJ}/copyexec/copyexec-results-engine.o ${OBJ}/copyexec/copyexec-rpc.o ${OBJ}/copyexec/copyexec-results-client.o \
    ${OBJ}/random/random-engine.o ${OBJ}/random/random-data.o \
    ${OBJ}/dns/dns-data.o ${OBJ}/dns/dns-engine.o ${OBJ}/dns/dns-binlog-diff.o ${OBJ}/util/tftp.o \
    ${OBJ}/dhcp/dhcp-engine.o ${OBJ}/dhcp/dhcp-data.o ${OBJ}/dhcp/dhcp-proto.o \
    ${OBJ}/weights/weights-engine.o ${OBJ}/weights/weights-data.o \
    ${OBJ}/storage/storage-data.o ${OBJ}/storage/storage-engine.o ${OBJ}/storage/storage-rpc.o ${OBJ}/storage/storage-import.o ${OBJ}/storage/storage-content.o ${OBJ}/storage/storage-binlog-check.o ${OBJ}/storage/storage-append.o ${OBJ}/storage/storage-binlog.o \
    ${OBJ}/KPHP/php-engine.o ${OBJ}/KPHP/php-engine-vars.o \
    ${OBJ}/TL/tlc.o ${OBJ}/TL/tl-parser.o ${OBJ}/TL/tl-scheme.o ${OBJ}/TL/tl-serialize.o ${OBJ}/TL/tl-utils.o ${OBJ}/TL/tlclient.o \
    ${OBJ}/TL/icplc.o ${OBJ}/TL/icpl-data.o \
    ${OBJ}/spell/spell-data.o ${OBJ}/spell/spell-engine.o \
    ${OBJ}/copyfast/copyfast-server.o ${OBJ}/copyfast/copyfast-engine.o ${OBJ}/copyfast/copyfast-common.o ${OBJ}/copyfast/copyfast-engine-data.o \
    ${OBJ}/image/image-engine.o \
    ${OBJ}/geoip/geoip.o ${OBJ}/geoip/geoip_v6.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/rpc-proxy/rpc-proxy.o ${RPC_PROXY_OBJS} \
    ${OBJ}/seqmap/seqmap-engine.o ${OBJ}/seqmap/seqmap-data.o ${OBJ}/rpc-proxy/rpc-proxy-delete-old.o ${OBJ}/net/net-udp-targets.o ${OBJ}/net/net-tcp-connections.o ${OBJ}/net/net-tcp-rpc-server.o ${OBJ}/net/net-tcp-rpc-common.o ${OBJ}/net/net-tcp-rpc-client.o

OBJECTS_NORM	=	${OBJECTS} \
    ${OBJ}/net/net-memcache-client.o ${OBJ}/net/net-memcache-server.o \
    ${OBJ}/net/net-mysql-client.o ${OBJ}/net/net-mysql-server.o \
    ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-server.o \
    ${OBJ}/crypto/aesni256.o \
    ${OBJ}/filesys/filesys-mount.o \
    ${OBJ}/copyexec/copyexec-data.o
OBJECTS_STRANGE	= \
    ${OBJ}/TL/tlc-new.o ${OBJ}/TL/tl-parser-new.o ${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o \
    ${OBJ}/hints/rating-data.o ${OBJ}/hints/rating-engine.o \
    ${OBJ}/hints/rating-hash_table.o ${OBJ}/hints/rating-utils.o ${OBJ}/hints/rating-maccub.o \
    ${OBJ}/isearch/isearch-data.o ${OBJ}/isearch/isearch-engine.o \
    ${OBJ}/isearch/isearch-interests-data.o ${OBJ}/isearch/isearch-interests-engine.o \
    ${OBJ}/isearch/isearch-x-data.o ${OBJ}/isearch/isearch-x-engine.o \
    ${OBJ}/lists/lists-x-binlog.o ${OBJ}/lists/lists-x-data.o ${OBJ}/lists/lists-x-engine.o \
    ${OBJ}/lists/lists-y-binlog.o ${OBJ}/lists/lists-y-data.o ${OBJ}/lists/lists-y-engine.o \
    ${OBJ}/lists/lists-z-data.o ${OBJ}/lists/lists-z-engine.o ${OBJ}/lists/lists-z-binlog.o \
    ${OBJ}/lists/lists-w-data.o ${OBJ}/lists/lists-w-engine.o \
    ${OBJ}/mc-proxy/mc-proxy-search.o \
    ${OBJ}/mutual-friends/maccub-x.o \
    ${OBJ}/search/search-x-index.o

OBJECTS_ALL		:=	${OBJECTS_NORM} ${OBJECTS_STRANGE}
DEPENDENCE_CXX		:=	$(subst ${OBJ}/,${DEP}/,$(patsubst %.o,%.d,${OBJECTS_CXX}))
DEPENDENCE_STRANGE	:=	$(subst ${OBJ}/,${DEP}/,$(patsubst %.o,%.d,${OBJECTS_STRANGE}))
DEPENDENCE_NORM	:=	$(subst ${OBJ}/,${DEP}/,$(patsubst %.o,%.d,${OBJECTS_NORM}))
DEPENDENCE_ALL		:=	${DEPENDENCE_NORM} ${DEPENDENCE_STRANGE} ${DEPENDENCE_CXX}

TL_OBJS := ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${OBJ}/net/net-rpc-client.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-connections.o ${OBJ}/vv/am-stats.o ${OBJ}/net/net-rpc-targets.o ${OBJ}/net/net-udp.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/common/crc32c.o ${OBJ}/net/net-udp.o ${OBJ}/net/net-udp-targets.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-tcp-connections.o ${OBJ}/net/net-tcp-rpc-server.o ${OBJ}/net/net-tcp-rpc-common.o ${OBJ}/net/net-tcp-rpc-client.o
TL_ENGINE_OBJS := ${TL_OBJS} ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-aio.o
TL_SCHEMA_LIST := TL/common.tl TL/tl.tl rpc-proxy/rpc-proxy.tl \
  memcached/memcache.tl seqmap/seqmap.tl lists/lists.tl search/search.tl statsx/statsx.tl text/text.tl \
  cache/cache.tl random/random.tl storage/storage.tl weights/weights.tl \
  friend/friends.tl news/news.tl hints/hints.tl isearch/isearch.tl photo/photo.tl targ/targ.tl net/net-udp-packet.tl 
TL_LIST := ${EXE}/combined.tl ${EXE}/combined.tlo

all:	dirs ${EXELIST} ${TL_LIST}
image: ${EXE}/image-engine 
filesys: ${EXE}/filesys-engine ${EXE}/filesys-commit-changes
spell: ${EXE}/spell-engine
dirs: ${ALLDIRS}
kphp: dirs ${OBJ}/KPHP/php-engine_.o ${EXE}/kphp2cpp
create_dirs_and_headers: dirs ${OBJ}/TL/constants.h
tl: ${EXE}/tlc ${EXE}/tlclient
binlog: ${EXE}/pack-binlog

${ALLDIRS}::	
	@test -d $@ || mkdir -p $@

-include ${DEPENDENCE_ALL}

${OBJECTS_CXX}: ${OBJ}/%.o: %.cpp | create_dirs_and_headers
	${CXX} ${CXXFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${OBJECTS}: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${EXE}/kphp2cpp: ${KPHP2CPPOBJS} ${OBJ}/common/base64.o ${SRVOBJS} ${OBJ}/drinkless/dl-utils-lite.o
	${CXX} -o $@ $^ ${LDFLAGS}

${OBJ}/KPHP/php-engine_.o: ${OBJ}/KPHP/php-engine.o ${OBJ}/KPHP/php-master.o ${OBJ}/net/net-mysql-client.o ${OBJ}/KPHP/php_script.o ${OBJ}/KPHP/php-queries.o ${OBJ}/KPHP/php-runner.o ${OBJ}/KPHP/php-engine-vars.o ${OBJ}/common/base64.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-client.o ${OBJ}/common/fast-backtrace.o ${OBJ}/common/resolver.o ${OBJ}/net/net-http-server.o ${KPHPOBJS} ${SRVOBJS} ${OBJ}/drinkless/dl-utils-lite.o ${OBJ}/common/unicode-utils.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/net/net-memcache-server.o ${TL_ENGINE_OBJS} | create_dirs_and_headers
	rm -f $@ && ar -crs -o $@ $^
${EXE}/php-engine: ${OBJ}/KPHP/php-engine_.o -lpcre -lre2
	${CXX} -o $@ $^ ${LDFLAGS}

${OBJ}/TL/tlc-new.o: ${OBJ}/%.o: %.c | dirs
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/TL/tl-parser-new.o: ${OBJ}/%.o: %.c | dirs
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/common/kdb-data-common.o: ${OBJ}/%.o: %.c | dirs
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/common/server-functions.o: ${OBJ}/%.o: %.c | dirs
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/common/crc32.o: ${OBJ}/%.o: %.c | dirs
	${CC} ${CFLAGS} ${CINCLUDE} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${OBJ}/net/net-memcache-server.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/net/net-memcache-client.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/net/net-mysql-server.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/net/net-mysql-client.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/net/net-rpc-server.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/net/net-rpc-client.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/image/image-data.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} ${INCLUDEGM} -pthread -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/filesys/filesys-mount.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -I/user/include/fuse -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/copyexec/copyexec-data.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} ${SUPERUSER_FLAGS} -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/crypto/aesni256.o: ${OBJ}/%.o: %.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -maes -DAES=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${EXE}/targ-import-dump:	${OBJ}/targ/targ-import-dump.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/targ-log-merge:	${OBJ}/targ/targ-log-merge.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/targ-log-split:	${OBJ}/targ/targ-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/targ-engine:	${OBJ}/targ/targ-engine.o ${OBJ}/targ/targ-data.o ${OBJ}/targ/targ-weights.o ${OBJ}/vv/am-amortization.o ${OBJ}/targ/targ-index.o ${OBJ}/targ/targ-search.o ${OBJ}/targ/targ-trees.o ${SRVOBJS} ${OBJ}/net/net-aio.o ${OBJ}/common/word-split.o ${OBJ}/common/translit.o ${OBJ}/common/stemmer.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/listcomp.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/targ-merge:	${OBJ}/targ/targ-merge.o ${OBJ}/common/server-functions.o ${OBJ}/net/net-events.o ${OBJ}/net/net-buffers.o ${OBJ}/common/estimate-split.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/check-binlog: ${OBJ}/binlog/check-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/tag-binlog: ${OBJ}/binlog/tag-binlog.o ${OBJ}/net/net-crypto-rsa.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/fix-rotateto: ${OBJ}/binlog/fix-rotateto.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pack-binlog: ${OBJ}/binlog/pack-binlog.o ${SRVOBJS} 
	${CC} -o $@ $^ ${LDFLAGS} -l lzma

${EXE}/statsx-engine:	${OBJ}/statsx/statsx-engine.o ${OBJ}/statsx/statsx-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/statsx-binlog:	${OBJ}/statsx/statsx-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/statsx-log-split:	${OBJ}/statsx/statsx-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/cache-binlog:	${OBJ}/cache/cache-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/cache-engine:	${OBJ}/cache/cache-engine.o ${OBJ}/cache/cache-data.o ${OBJ}/cache/cache-heap.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${OBJ}/vv/am-server-functions.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/cache-log-split:	${OBJ}/cache/cache-log-split.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/cache-simulator:	${OBJ}/cache/cache-simulator.o ${OBJ}/cache/cache-data.o ${OBJ}/cache/cache-heap.o ${SRVOBJS} ${OBJ}/vv/am-hash.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/search-binlog:	${OBJ}/search/search-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-import-dump:	${OBJ}/search/search-import-dump.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-merge:	${OBJ}/search/search-merge.o ${OBJ}/common/server-functions.o ${OBJ}/net/net-events.o ${OBJ}/net/net-buffers.o ${OBJ}/common/estimate-split.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-log-split:	${OBJ}/search/search-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-index:	${OBJ}/search/search-index.o ${OBJ}/common/kdb-data-common.o ${OBJ}/binlog/kdb-binlog-common.o ${KFSOBJS} ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/md5.o ${OBJ}/search/utils.o ${OBJ}/search/search-common.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-engine:	${OBJ}/search/search-engine.o ${OBJ}/search/search-data.o ${OBJ}/search/search-common.o ${SRVOBJS} ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/search/utils.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o ${OBJ}/search/search-value-buffer.o ${OBJ}/search/search-profile.o ${OBJ}/vv/am-stats.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-x-engine:	${OBJ}/search/search-x-engine.o ${OBJ}/search/search-x-data.o ${SRVOBJS} ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/search/utils.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o ${OBJ}/search/search-value-buffer.o ${OBJ}/search/search-common.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-x-index:	${OBJ}/search/search-x-index.o ${OBJ}/common/kdb-data-common.o ${OBJ}/binlog/kdb-binlog-common.o ${KFSOBJS} ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/md5.o ${OBJ}/search/utils.o ${OBJ}/search/search-common.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-y-engine:	${OBJ}/search/search-y-engine.o ${OBJ}/search/search-y-data.o ${OBJ}/common/search-y-parse.o ${SRVOBJS} ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/search/utils.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o ${OBJ}/search/search-profile.o ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-server-functions.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/search-y-index:	${OBJ}/search/search-y-index.o ${OBJ}/common/kdb-data-common.o ${OBJ}/binlog/kdb-binlog-common.o ${KFSOBJS} ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/md5.o ${OBJ}/search/utils.o ${OBJ}/common/search-y-parse.o ${OBJ}/search/search-index-layout.o ${OBJ}/common/listcomp.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/friend-import-dump:	${OBJ}/friend/friend-import-dump.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/friend-engine:	${OBJ}/friend/friend-engine.o ${OBJ}/friend/friend-data.o ${SRVOBJS} ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-parse.o ${OBJ}/vv/am-stats.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/friend-log-merge:	${OBJ}/friend/friend-log-merge.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/friend-log-split:	${OBJ}/friend/friend-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/news-engine:	${OBJ}/news/news-engine.o ${OBJ}/news/news-data.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-parse.o ${OBJ}/net/net-aio.o ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-server-functions.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/news-binlog:	${OBJ}/news/news-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/news-log-split:	${OBJ}/news/news-log-split.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/news-import-dump:	${OBJ}/news/news-import-dump.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/lists-import-dump:	${OBJ}/lists/lists-import-dump.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-engine:	${OBJ}/lists/lists-engine.o ${OBJ}/lists/lists-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-log-merge:	${OBJ}/lists/lists-log-merge.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-binlog:	${OBJ}/lists/lists-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-log-split:	${OBJ}/lists/lists-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

lists-x:	${EXE}/lists-x-engine
${EXE}/lists-x-engine:	${OBJ}/lists/lists-x-engine.o ${OBJ}/lists/lists-x-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-x-binlog:	${OBJ}/lists/lists-x-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/lists/lists-x-binlog.o: ${OBJ}/%.o: lists/lists-binlog.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-x-engine.o: ${OBJ}/%.o: lists/lists-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-x-data.o: ${OBJ}/%.o: lists/lists-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

lists-y:	${EXE}/lists-y-engine
${EXE}/lists-y-engine:	${OBJ}/lists/lists-y-engine.o ${OBJ}/lists/lists-y-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-y-binlog:	${OBJ}/lists/lists-y-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/lists/lists-y-engine.o: ${OBJ}/%.o: lists/lists-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-y-data.o: ${OBJ}/%.o: lists/lists-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-y-binlog.o: ${OBJ}/%.o: lists/lists-binlog.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

lists-z:	${EXE}/lists-z-engine
${EXE}/lists-z-engine:	${OBJ}/lists/lists-z-engine.o ${OBJ}/lists/lists-z-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/lists-z-binlog:	${OBJ}/lists/lists-z-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/lists/lists-z-engine.o: ${OBJ}/%.o: lists/lists-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-z-data.o: ${OBJ}/%.o: lists/lists-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-z-binlog.o: ${OBJ}/%.o: lists/lists-binlog.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS_Z=1 -DVALUES64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

lists-w:	${EXE}/lists-w-engine
${EXE}/lists-w-engine:	${OBJ}/lists/lists-w-engine.o ${OBJ}/lists/lists-w-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/lists/lists-w-engine.o: ${OBJ}/%.o: lists/lists-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/lists/lists-w-data.o: ${OBJ}/%.o: lists/lists-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DLISTS64=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${OBJ}/search/search-x-index.o: ${OBJ}/%.o: search/search-index.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DSEARCHX -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${EXE}/text-import-dump:	${OBJ}/text/text-import-dump.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/text-index:	${OBJ}/text/text-index.o ${OBJ}/common/kdb-data-common.o ${OBJ}/binlog/kdb-binlog-common.o ${KFSOBJS} ${OBJ}/common/crc32.o ${OBJ}/common/server-functions.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/md5.o ${OBJ}/common/listcomp.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/text-engine:	${OBJ}/text/text-engine.o ${OBJ}/text/text-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-http-server.o ${OBJ}/net/net-parse.o ${SRVOBJS} ${OBJ}/common/word-split.o ${OBJ}/common/stemmer.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/listcomp.o ${OBJ}/common/aho-kmp.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/text-log-merge:	${OBJ}/text/text-log-merge.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/text-log-split:	${OBJ}/text/text-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o ${OBJ}/common/translit.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/text-binlog: ${OBJ}/text/text-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/money-engine:	${OBJ}/money/money-engine.o ${OBJ}/money/money-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/money-import-dump:	${OBJ}/money/money-import-dump.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/mc-proxy:	${OBJ}/mc-proxy/mc-proxy.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-memcache-client.o ${OBJ}/common/resolver.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/mc-proxy-search:	${OBJ}/mc-proxy/mc-proxy-search.o ${OBJ}/mc-proxy/mc-proxy-search-extension.o ${OBJ}/common/estimate-split.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-memcache-client.o ${OBJ}/common/resolver.o ${OBJ}/mc-proxy/mc-proxy-merge-extension.o ${OBJ}/mc-proxy/mc-proxy-news-extension.o ${OBJ}/mc-proxy/mc-proxy-random-extension.o ${OBJ}/mc-proxy/mc-proxy-statsx-extension.o ${OBJ}/net/net-parse.o ${OBJ}/mc-proxy/mc-proxy-friends-extension.o ${OBJ}/mc-proxy/mc-proxy-targ-extension.o ${OBJ}/mc-proxy/mc-proxy-news-recommend-extension.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/rpc-proxy:	${OBJ}/rpc-proxy/rpc-proxy.o ${RPC_PROXY_OBJS} ${OBJ}/common/estimate-split.o ${OBJ}/common/resolver.o ${TL_ENGINE_OBJS} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/rpc-proxy-delete-old:	${OBJ}/rpc-proxy/rpc-proxy-delete-old.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/mc-proxy/mc-proxy-search.o: ${OBJ}/%.o: mc-proxy/mc-proxy.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DSEARCH_MODE_ENABLED -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${EXE}/db-proxy:	${OBJ}/db-proxy/db-proxy.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-mysql-server.o ${OBJ}/net/net-mysql-client.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/resolver.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/targ-recover:	${OBJ}/targ/targ-recover.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-client.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/replicator:	${OBJ}/util/replicator.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/common/resolver.o ${OBJ}/common/common-data.o ${OBJ}/monitor/monitor-common.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/backup-engine:	${OBJ}/util/backup-engine.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${KFSOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/memcached:	${OBJ}/memcached/memcached-engine.o ${OBJ}/memcached/memcached-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${OBJ}/vv/vv-tl-parse.o ${TL_ENGINE_OBJS} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pmemcached-ram:	${OBJ}/pmemcached/pmemcached-engine.o ${OBJ}/pmemcached/pmemcached-data.o ${OBJ}/pmemcached/pmemcached-index-ram.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pmemcached-disk:	${OBJ}/pmemcached/pmemcached-engine.o ${OBJ}/pmemcached/pmemcached-data.o ${OBJ}/pmemcached/pmemcached-index-disk.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-aio.o ${OBJ}/vv/vv-tl-parse.o ${OBJ}/vv/vv-tl-aio.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${SRVOBJS} ${OBJ}/vv/am-stats.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pmemcached-import-dump:	${OBJ}/pmemcached/pmemcached-import-dump.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o ${OBJ}/common/base64.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pmemcached-binlog:	${OBJ}/pmemcached/pmemcached-binlog.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/pmemcached-log-split:	${OBJ}/pmemcached/pmemcached-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/hints-engine:	${OBJ}/hints/utils.o ${OBJ}/hints/utf8_utils.o ${OBJ}/hints/perfect-hashing.o ${OBJ}/hints/treap.o ${OBJ}/hints/maccub.o ${OBJ}/hints/hash_table.o ${OBJ}/hints/hints-engine.o ${OBJ}/hints/hints-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/utf8_utils.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/hints-log-split:	${OBJ}/hints/hints-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o ${DLSTD}
	${CC} -o $@ $^ ${LDFLAGS}

rating:	${EXE}/rating-engine
${EXE}/rating-engine:	${OBJ}/hints/rating-utils.o ${OBJ}/hints/treap.o ${OBJ}/hints/rating-maccub.o ${OBJ}/hints/rating-hash_table.o ${OBJ}/hints/rating-engine.o ${OBJ}/hints/rating-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/hints/rating-engine.o: ${OBJ}/%.o: hints/hints-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOHINTS=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/hints/rating-data.o: ${OBJ}/%.o: hints/hints-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOHINTS=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/hints/rating-maccub.o: ${OBJ}/%.o: hints/maccub.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOHINTS=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/hints/rating-hash_table.o: ${OBJ}/%.o: hints/hash_table.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOHINTS=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/hints/rating-utils.o: ${OBJ}/%.o: hints/utils.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOHINTS=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<


${EXE}/bayes-engine:	${OBJ}/bayes/utils.o ${OBJ}/bayes/hash_table.o ${OBJ}/bayes/bayes-engine.o ${OBJ}/bayes/bayes-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/utf8_utils.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/mf-prepare-file:	${OBJ}/mutual-friends/mf-prepare-file.o ${OBJ}/mutual-friends/treap.o ${OBJ}/mutual-friends/utils.o ${OBJ}/mutual-friends/maccub.o ${OBJ}/common/crc32.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/mf-merge-files:	${OBJ}/mutual-friends/mf-merge-files.o ${OBJ}/mutual-friends/treap.o ${OBJ}/mutual-friends/utils.o ${OBJ}/mutual-friends/hash_table.o ${OBJ}/mutual-friends/maccub.o ${OBJ}/common/crc32.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/mutual-friends/maccub-x.o: ${OBJ}/%.o: mutual-friends/maccub.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DBLIST=1 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${EXE}/mf-engine:	${OBJ}/mutual-friends/utils.o ${OBJ}/mutual-friends/hash_table.o ${OBJ}/mutual-friends/mf-engine.o ${OBJ}/mutual-friends/mf-data.o ${OBJ}/mutual-friends/treap.o ${OBJ}/mutual-friends/maccub-x.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/mf-xor:	${OBJ}/mutual-friends/mf-xor.o ${OBJ}/mutual-friends/utils.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/mf-process-file:	${OBJ}/mutual-friends/mf-process-file.o ${OBJ}/mutual-friends/utils.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/isearch-engine:	${OBJ}/isearch/dl.o ${OBJ}/isearch/utf8_utils.o ${OBJ}/isearch/isearch-engine.o ${OBJ}/isearch/isearch-data.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/word-split.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/isearch/isearch-engine.o: ${OBJ}/%.o: isearch/isearch-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DMAXQ=5 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/isearch/isearch-data.o: ${OBJ}/%.o: isearch/isearch-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DMAXQ=5 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

isearch-interests:	${EXE}/isearch-interests-engine
${EXE}/isearch-interests-engine:	${OBJ}/isearch/dl.o ${OBJ}/isearch/utf8_utils.o ${OBJ}/isearch/isearch-interests-engine.o ${OBJ}/isearch/isearch-interests-data.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/word-split.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/isearch/isearch-interests-engine.o: ${OBJ}/%.o: isearch/isearch-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOTYPES=1 -DNOFADING=1 -DNOISE_PERCENT=850 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/isearch/isearch-interests-data.o: ${OBJ}/%.o: isearch/isearch-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOTYPES=1 -DNOFADING=1 -DNOISE_PERCENT=850 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

isearch-x:	${EXE}/isearch-x-engine
${EXE}/isearch-x-engine:	${OBJ}/isearch/dl.o ${OBJ}/isearch/utf8_utils.o ${OBJ}/isearch/isearch-x-engine.o ${OBJ}/isearch/isearch-x-data.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/word-split.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${OBJ}/isearch/isearch-x-engine.o: ${OBJ}/%.o: isearch/isearch-engine.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOTYPES=1 -DNOISE_PERCENT=900 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<
${OBJ}/isearch/isearch-x-data.o: ${OBJ}/%.o: isearch/isearch-data.c | create_dirs_and_headers
	${CC} ${CFLAGS} ${CINCLUDE} -DNOTYPES=1 -DNOISE_PERCENT=900 -c -MP -MD -MF ${DEP}/$*.d -MQ ${OBJ}/$*.o -o $@ $<

${EXE}/logs-engine:	${OBJ}/logs/logs-engine.o ${OBJ}/logs/logs-data.o ${OBJ}/logs/dl.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/string-processing.o ${DLSTD} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/logs-merge-dumps:	${OBJ}/logs/logs-merge-dumps.o ${DLSTD} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/logs-merge-stats:	${OBJ}/logs/logs-merge-stats.o ${DLSTD} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/queue-engine:	${OBJ}/queue/queue-engine.o ${OBJ}/queue/queue-data.o ${OBJ}/queue/utils.o ${OBJ}/watchcat/utils.o ${OBJ}/common/search-y-parse.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${DLDEF} ${OBJ}/net/net-http-server.o ${OBJ}/net/net-memcache-client.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/common/resolver.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/poll-engine:	${OBJ}/poll/poll-engine.o ${OBJ}/poll/poll-data.o ${OBJ}/poll/utils.o ${OBJ}/common/search-y-parse.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${DLDEF} ${OBJ}/net/net-http-server.o ${OBJ}/net/net-memcache-client.o ${OBJ}/common/resolver.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/watchcat-engine:	${OBJ}/watchcat/watchcat-engine.o ${OBJ}/watchcat/watchcat-data.o ${OBJ}/watchcat/utils.o ${OBJ}/common/search-y-parse.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-memcache-client.o ${DLDEF} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/magus-precalc:	${OBJ}/magus/magus-precalc.o ${OBJ}/magus/dl.o ${OBJ}/common/unicode-utils.o ${OBJ}/common/utf8_utils.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o ${OBJ}/drinkless/dl-utils.o ${OBJ}/drinkless/dl-utils-lite.o ${OBJ}/common/string-processing.o ${OBJ}/hints/utils.o 
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/magus-engine:	${OBJ}/magus/magus-engine.o ${OBJ}/magus/magus-data.o ${OBJ}/magus/dl.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/drinkless/dl-utils.o ${OBJ}/drinkless/dl-utils-lite.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/image-engine: ${OBJ}/image/image-engine.o ${OBJ}/image/image-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o ${OBJ}/common/md5.o ${OBJ}/common/sha1.o ${OBJ}/net/net-events.o ${OBJ}/net/net-buffers.o ${OBJ}/net/net-crypto-aes.o ${OBJ}/crypto/aesni256.o ${OBJ}/vv/am-stats.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGSGM}

${EXE}/tlc: ${OBJ}/TL/tlc.o ${OBJ}/TL/tl-parser.o ${OBJ}/TL/tl-scheme.o ${OBJ}/TL/tl-serialize.o ${OBJ}/TL/tl-utils.o ${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/tlc-new: ${OBJ}/TL/tlc-new.o ${OBJ}/TL/tl-parser-new.o ${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o ${OBJ}/common/crc32.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/tlclient: ${OBJ}/TL/tlclient.o ${OBJ}/TL/tl-parser.o ${OBJ}/TL/tl-scheme.o ${OBJ}/TL/tl-serialize.o ${OBJ}/TL/tl-utils.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-rpc-common.o ${OBJ}/net/net-rpc-client.o
	${CC} -o $@ $^ ${LDFLAGS} -l:libreadline.a -l:libtermcap.a

${EXE}/icplc: ${OBJ}/TL/icplc.o ${OBJ}/TL/icpl-data.o ${OBJ}/common/kdb-data-common.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/random-engine: ${OBJ}/random/random-engine.o ${OBJ}/random/random-data.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-server-functions.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/weights-engine: ${OBJ}/weights/weights-engine.o ${OBJ}/weights/weights-data.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/vv/am-stats.o ${OBJ}/vv/am-hash.o ${OBJ}/vv/am-amortization.o ${OBJ}/vv/am-server-functions.o ${OBJ}/common/crc32c.o ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/dns-engine: ${OBJ}/dns/dns-engine.o ${OBJ}/dns/dns-data.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-udp.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${OBJ}/vv/am-hash.o ${OBJ}/vv/am-stats.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/dns-binlog-diff: ${OBJ}/dns/dns-binlog-diff.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/tftp: ${OBJ}/util/tftp.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-udp.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${OBJ}/vv/am-stats.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/dhcp-engine: ${OBJ}/dhcp/dhcp-engine.o ${OBJ}/dhcp/dhcp-data.o ${OBJ}/dhcp/dhcp-proto.o ${OBJ}/net/net-msg-buffers.o ${OBJ}/net/net-msg.o ${OBJ}/net/net-udp.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-connections.o ${OBJ}/vv/am-stats.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/spell-engine: ${OBJ}/spell/spell-engine.o ${OBJ}/spell/spell-data.o ${SRVOBJS} ${OBJ}/common/word-split.o ${OBJ}/common/stemmer.o ${OBJ}/common/utf8_utils.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS} -laspell

${EXE}/filesys-commit-changes: ${OBJ}/filesys/filesys-commit-changes.o ${OBJ}/filesys/filesys-utils.o ${OBJ}/filesys/filesys-pending-operations.o ${SRVOBJS} ${OBJ}/common/listcomp.o ${OBJ}/common/diff-patch.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/filesys-xfs-engine: ${OBJ}/filesys/filesys-xfs-engine.o ${OBJ}/filesys/filesys-utils.o ${OBJ}/filesys/filesys-pending-operations.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/listcomp.o ${OBJ}/common/diff-patch.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/filesys-engine: ${OBJ}/filesys/filesys-engine.o ${OBJ}/filesys/filesys-mount.o ${OBJ}/filesys/filesys-data.o ${OBJ}/filesys/filesys-memcache.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o 
	${CC} -o $@ $^ ${LDFLAGS} ${FUSE_LIBS_FLAGS} -pthread -l:libfuse.a -ldl

${EXE}/copyexec-commit: ${OBJ}/copyexec/copyexec-commit.o ${OBJ}/copyexec/copyexec-data.o ${OBJ}/copyexec/copyexec-err.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-crypto-rsa.o ${OBJ}/filesys/filesys-utils.o ${OBJ}/common/base64.o
	${CC} -o $@ $^ ${LDFLAGS} && chmod 0750 ${EXE}/copyexec-commit

${EXE}/copyexec-engine: ${OBJ}/copyexec/copyexec-engine.o ${OBJ}/copyexec/copyexec-data.o ${OBJ}/copyexec/copyexec-err.o ${OBJ}/copyexec/copyexec-results-client.o ${OBJ}/copyexec/copyexec-rpc.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-crypto-rsa.o ${OBJ}/net/net-rpc-common.o ${OBJ}/net/net-rpc-client.o ${OBJ}/filesys/filesys-utils.o ${OBJ}/common/base64.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS} && chmod 0750 ${EXE}/copyexec-engine

${EXE}/copyexec-binlog: ${OBJ}/copyexec/copyexec-binlog.o ${OBJ}/copyexec/copyexec-err.o ${SRVOBJS} ${OBJ}/net/net-connections.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/copyexec-results-engine: ${OBJ}/copyexec/copyexec-results-engine.o ${OBJ}/copyexec/copyexec-results-data.o ${OBJ}/copyexec/copyexec-rpc.o ${OBJ}/copyexec/copyexec-err.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-crypto-rsa.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-common.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS} && chmod 0750 ${EXE}/copyexec-results-engine

${EXE}/storage-engine:	${OBJ}/storage/storage-engine.o ${OBJ}/storage/storage-data.o ${OBJ}/storage/storage-content.o ${OBJ}/storage/storage-rpc.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-http-server.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/common/base64.o ${OBJ}/net/net-aio.o ${OBJ}/vv/am-stats.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/storage-import:	${OBJ}/storage/storage-import.o ${OBJ}/storage/storage-content.o ${SRVOBJS} ${OBJ}/common/base64.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/storage-binlog-check: ${OBJ}/storage/storage-binlog-check.o ${OBJ}/storage/storage-data.o ${OBJ}/storage/storage-content.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/storage-append: ${OBJ}/storage/storage-append.o ${OBJ}/storage/storage-content.o ${OBJ}/common/base64.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/storage-binlog: ${OBJ}/storage/storage-binlog.o ${OBJ}/storage/storage-data.o ${OBJ}/storage/storage-content.o ${SRVOBJS} ${OBJ}/net/net-connections.o ${OBJ}/common/base64.o ${OBJ}/net/net-aio.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/letters-engine:	${OBJ}/letters/letters-engine.o ${OBJ}/letters/letters-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${DLDEF} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/photo-engine:	${OBJ}/photo/photo-engine.o ${OBJ}/photo/photo-data.o ${OBJ}/photo/utils.o ${OBJ}/photo/dl.o ${OBJ}/common/base64.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${DLDEF} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/photo-import-dump:	${OBJ}/photo/photo-import-dump.o ${OBJ}/photo/photo-data.o ${OBJ}/photo/utils.o ${OBJ}/photo/dl.o ${OBJ}/common/base64.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-connections.o ${DLDEF} ${SRVOBJS} ${TL_ENGINE_OBJS}
	${CC} -o $@ $^ ${LDFLAGS}
${EXE}/photo-log-split:	${OBJ}/photo/photo-log-split.o ${OBJ}/common/server-functions.o ${KFSOBJS} ${OBJ}/binlog/kdb-binlog-common.o ${OBJ}/common/base64.o ${OBJ}/common/crc32.o ${OBJ}/common/md5.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/truncate:	${OBJ}/util/truncate.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/crc32:	${OBJ}/util/crc32.o ${OBJ}/common/crc32.o ${OBJ}/common/server-functions.o
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/copyfast-server: ${OBJ}/copyfast/copyfast-server.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/copyfast/copyfast-common.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/copyfast-engine: ${OBJ}/copyfast/copyfast-engine.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${OBJ}/net/net-rpc-server.o ${OBJ}/net/net-rpc-client.o ${OBJ}/net/net-rpc-common.o ${OBJ}/copyfast/copyfast-common.o ${OBJ}/copyfast/copyfast-engine-data.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/support-engine: ${OBJ}/support/support-engine.o ${OBJ}/support/support-data.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-aio.o ${OBJ}/net/net-memcache-server.o ${OBJ}/common/word-split.o ${OBJ}/common/stemmer-new.o ${OBJ}/common/utf8_utils.o ${SRVOBJS} ${DLDEF}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/antispam-engine: ${OBJ}/common/utf8_utils.o ${OBJ}/common/string-processing.o ${OBJ}/antispam/antispam-engine.o ${OBJ}/antispam/antispam-engine-impl.o ${OBJ}/antispam/antispam-data.o ${OBJ}/antispam/antispam-db.o ${OBJ}/net/net-connections.o ${OBJ}/net/net-memcache-server.o ${SRVOBJS} ${DLDEF} ${SKATOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/antispam-import-dump: ${OBJ}/common/utf8_utils.o ${OBJ}/common/string-processing.o ${OBJ}/antispam/antispam-import-dump.o ${OBJ}/antispam/antispam-engine-impl.o ${OBJ}/antispam/antispam-data.o ${OBJ}/antispam/antispam-db.o ${SRVOBJS} ${DLDEF} ${SKATOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/geoip:	${OBJ}/geoip/geoip.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/geoip_v6:	${OBJ}/geoip/geoip_v6.o ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/seqmap-engine:	${OBJ}/seqmap/seqmap-engine.o ${OBJ}/seqmap/seqmap-data.o ${TL_ENGINE_OBJS} ${SRVOBJS}
	${CC} -o $@ $^ ${LDFLAGS}

${EXE}/combined.tl: ${TL_SCHEMA_LIST}
	cat ${TL_SCHEMA_LIST} | sed 's/[[:space:]]*$$//' > $@ || ( rm $@ && false )

${OBJ}/bin/combined2.tl: ${EXE}/combined.tl ${EXE}/tlc-new
	${EXE}/tlc-new -w 2 -E $< 2> $@ || ( cat $@ && rm $@ && false )

${OBJ}/bin/combined.tlo: ${EXE}/combined.tl ${EXE}/tlc-new
	${EXE}/tlc-new -w 2 -e $@ $< || ( rm $@ && false )

${OBJ}/TL/constants.h: ${EXE}/combined2.tl TL/gen_constants_h.awk
	awk -f TL/gen_constants_h.awk <$< >$@ || ( rm $@ && false )

clean:	
	rm -rf ${OBJ} ${DEP} ${EXE} || true

dist:	all
	install -m 750 -t /usr/share/engine/bin ${EXELIST}
