ifdef KPHP_PATH
	SRC = $(KPHP_PATH)
else ifdef VK_PATH
	SRC = $(VK_PATH)
else
#change to source directory or provide KPHP_PATH or VK_PATH environment variable
	SRC = $(HOME)/engine/src.vk
endif

ifeq ($m, 32)
OBJ	=	$(SRC)/objs32
MF	=	-m32
DEP = dep32
else
OBJ	=	$(SRC)/objs
MF	=	-m64
DEP = dep
endif

ifeq ($p, 1)
OBJ	:=	${OBJ}p
DEP := ${DEP}p
PROF	=	-pg
PROFC	=	-pg -DDEBUG=1
OPT	=
else
PROF	=
OPT	=	-fno-exceptions -Os
endif

KPHP_DIR:=$(CURDIR)
CFLAGS = -Wall -Wno-parentheses -Wno-trigraphs ${OPT} ${MF} -ggdb -march=core2 -mfpmath=sse -mssse3 -fno-strict-aliasing -fno-strict-overflow -fwrapv -fno-omit-frame-pointer ${PROFC}
LDFLAGS = ${MF} -lm -lz -lpthread -lrt -lcrypto -lpcre -lre2 -ggdb -rdynamic ${PROF}

CINCLUDE	= -iquote $(SRC)/KPHP -iquote $(SRC)/drinkless -iquote $(SRC)/common -iquote $(KPHP_DIR)/kphp
KPHP_OBJ := objs
EXE	:=	bin

ifeq ($p, 1)
KPHP_OBJ	:=	${KPHP_OBJ}p
EXE	:=	${EXE}p
endif

OBJDIRS	=	${EXE} ${DEP} ${KPHP_OBJ}

SRVOBJS	=	${OBJ}/common/kdb-data-common.o ${OBJ}/common/fast-backtrace.o ${OBJ}/common/server-functions.o ${OBJ}/common/utf8_utils.o \
			${OBJ}/common/crc32.o ${OBJ}/common/md5.o ${OBJ}/common/sha1.o ${OBJ}/common/base64.o ${OBJ}/common/unicode-utils.o \
			${OBJ}/net/net-events.o ${OBJ}/net/net-buffers.o ${OBJ}/net/net-crypto-aes.o ${OBJ}/crypto/aesni256.o

.PHONY:	all script engine main create_dirs 

all:	main 
engine: ${EXE}/engine
script: ${KPHP_OBJ}/script.o
main: ${EXE}/main

KPHP_ENGINE_OBJ=${OBJ}/KPHP/php-engine_.o

SUBDIRS:=$(patsubst %/,%,$(sort $(dir $(wildcard kphp/*/))))
SUBDIR_OBJS := $(SUBDIRS:%=$(KPHP_OBJ)/%.o)

ALL_CPP_FILES:=$(foreach dir,${SUBDIRS},$(wildcard ${dir}/*.cpp))
ALL_CPP_OBJS := $(patsubst %.cpp, $(KPHP_OBJ)/%.o, ${ALL_CPP_FILES})

OBJDIRS += $(foreach dir,${SUBDIRS},${KPHP_OBJ}/${dir})
DEPDIRS += $(foreach dir,${SUBDIRS},${DEP}/${dir})

DIRS = ${OBJDIRS} ${DEPDIRS}

create_dirs: ${DIRS} 
${DIRS}:	
	@test -d $@ || mkdir -p $@

DEPENDENCE_CPP := $(subst ${KPHP_OBJ}/,${DEP}/,$(patsubst %.o,%.d,${ALL_CPP_OBJS}))
-include ${DEPENDENCE_CPP}

${ALL_CPP_OBJS}:${KPHP_OBJ}/%.o: %.cpp | create_dirs
	${CXX} -c ${CFLAGS} ${CINCLUDE} -MP -MD -MF ${DEP}/$*.d -MQ ${KPHP_OBJ}/$*.o -o $@ $<

${KPHP_OBJ}/script.o: ${SUBDIR_OBJS} | create_dirs
	ld -r -o $@ $^

${EXE}/engine: ${SUBDIR_OBJS} ${KPHP_ENGINE_OBJ} | create_dirs
	${CXX} -o $@ $^ ${LDFLAGS}

${EXE}/main: $(KPHP_OBJ)/kphp/xmain.o ${KPHP_OBJ}/script.o ${OBJ}/KPHP/runtime/allocator.o ${OBJ}/KPHP/runtime/array_functions.o ${OBJ}/KPHP/runtime/bcmath.o ${OBJ}/KPHP/runtime/datetime.o ${OBJ}/KPHP/runtime/drivers.o ${OBJ}/KPHP/runtime/exception.o ${OBJ}/KPHP/runtime/files.o ${OBJ}/KPHP/runtime/interface.o ${OBJ}/KPHP/runtime/math_functions.o ${OBJ}/KPHP/runtime/mbstring.o ${OBJ}/KPHP/runtime/misc.o ${OBJ}/KPHP/runtime/openssl.o ${OBJ}/KPHP/runtime/regexp.o ${OBJ}/KPHP/runtime/rpc.o ${OBJ}/KPHP/runtime/string_functions.o ${OBJ}/KPHP/runtime/url.o ${OBJ}/KPHP/runtime/zlib.o ${OBJ}/KPHP/runtime/gost_hash.o ${OBJ}/KPHP/php_script.o ${OBJ}/drinkless/dl-utils-lite.o ${SRVOBJS} | create_dirs
	${CXX} -o $@ $^ ${LDFLAGS}


.SECONDEXPANSION: 
${SUBDIR_OBJS} : $$(filter-out $(KPHP_OBJ)/kphp/xmain.o,$$(patsubst %.cpp,$(KPHP_OBJ)/%.o,$$(wildcard $$(patsubst $(KPHP_OBJ)/%.o,%,$$@)/*.cpp))) | create_dirs
	ld -r -o $@ $^

