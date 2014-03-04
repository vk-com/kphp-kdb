/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <magick/api.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "md5.h"
#include "image-data.h"
#include "server-functions.h"

//#define DEBUG_HANG

int gm_max_load_image_area = -1;

/* dirty hack: need for link common/kdb-data-common.c without binlog and crc32 */
int default_replay_logevent (void *E __attribute__((__unused__)), int size __attribute__((__unused__))) {
  return 0;
}

static void *failf (struct forth_stack *st, char *format, ...) __attribute__ ((format (printf, 2, 3)));
static void gm_catch_exception (struct forth_stack *st, ExceptionInfo *e_info);
static int endswith (const char *s, const char *suffix, int suffix_len);

////////////////////////////////// WEBP functions ///////////////////////
#ifdef __LP64__
#define WEBP
#endif
#ifdef WEBP
#include "webp/encode.h"
#define WEBP_MAX_DIMENSION_SIZE 2048
int webp_save_image (struct forth_stack *st, Image *I, ImageInfo *info) {
  if (I->rows > WEBP_MAX_DIMENSION_SIZE || I->columns > WEBP_MAX_DIMENSION_SIZE) {
    failf (st, "%s: Image '%s' is too big (%dx%d), but WEBP_MAX_DIMENSION_SIZE is %d.",
      __func__, I->filename, (int) I->rows, (int) I->columns, WEBP_MAX_DIMENSION_SIZE);
    return -1;
  }
  const long long sz_rgb_image = (3LL * I->rows) * I->columns;
  uint8_t *a = malloc (sz_rgb_image);
  if (a == NULL) {
    failf (st, "%s: Image '%s' is too big (%dx%d), fail to allocate RGB image, malloc (%lld) returns NULL.",
      __func__, I->filename, (int) I->rows, (int) I->columns, sz_rgb_image);
    return -1;
  }

  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  if (DispatchImage (I, 0, 0, I->columns, I->rows, "RGB", CharPixel, a, &exception) == MagickFail) {
    gm_catch_exception (st, &exception);
    return -1;
  }
  DestroyExceptionInfo (&exception);

  uint8_t *b = NULL;
  size_t r = WebPEncodeRGB (a, I->columns, I->rows, 3 * I->columns, info->quality, &b);
  free (a); a = NULL;
  if (!r) {
    failf (st, "%s: WebPEncodeRGB returns 0, image '%s'", __func__, I->filename);
    return -1;
  }

  int fd = open (I->filename, O_WRONLY | O_TRUNC | O_CREAT, 0640);
  if (fd < 0) {
    failf (st, "%s: fail to creat '%s'. %m", __func__, I->filename);
    free (b);
    return -1;
  }

  if (write (fd, b, r) != r) {
    kprintf ("%s: write to file '%s' failed. %m\n", __func__, I->filename);
    assert (!close (fd));
    free (b);
    return -1;
  }
  free (b);
  assert (!close (fd));
  return 0;
}
#endif

////////////////////////////////// Graphics Magick helper functions ///////////////////////
static void fail_exception (struct forth_stack *st, ExceptionInfo *e_info) {
  if (e_info->severity != UndefinedException) {
    failf (st, "Magick: %s (%s).", e_info->reason, e_info->description);
  }
}

static void gm_catch_exception (struct forth_stack *st, ExceptionInfo *e_info) {
  fail_exception (st, e_info);
  CatchException (e_info);
  DestroyExceptionInfo (e_info);
}

static Image *gm_read_image (struct forth_stack *st, const char *filename) {
  if (strlen (filename) > MaxTextExtent-1) {
    failf (st, "%s: filename is too long", __func__);
    return 0;
  }

  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  ImageInfo *image_info = CloneImageInfo (NULL);
  strcpy (image_info->filename, filename);

  Image *w = NULL;
  if (gm_max_load_image_area > 0) {
    w = PingImage (image_info, &exception);
    if (w == NULL) {
      DestroyImageInfo (image_info);
      gm_catch_exception (st, &exception);
      return 0;
    }

    {
      int area_overflow = 0;
      unsigned long long area = 0;
      Image *t = GetFirstImageInList (w);
      while (t != NULL) {
        area += (unsigned long long) t->columns * (unsigned long long) t->rows;
        vkprintf (4, "area: %llu\n", area);
        t = GetNextImageInList (t);
        if (area > gm_max_load_image_area) {
          area_overflow = 1;
        }
      }

      if (!area || area_overflow) {
        DestroyImageInfo (image_info);
        failf (st, "%s: bad area (%llu pixels)", __func__, area);
        DestroyImage (w);
        return 0;
      }
    }
  }

  Image *r = ReadImage (image_info, &exception);
  if (w != NULL) {
    DestroyImage (w);
  }

  DestroyImageInfo (image_info);
  if (r == NULL) {
    gm_catch_exception (st, &exception);
    return 0;
  }

  DestroyExceptionInfo (&exception);
  if (GetImageListLength (r) > 1) {
    Image *w = ReferenceImage (GetFirstImageInList (r));
    DestroyImageList (r);
    r = w;
  }

  if (ProfileImage (r, "*", NULL, 0, MagickFalse) == MagickFail) {
    DestroyImage (r);
    failf (st, "%s: ProfileImage returns MagickFail", __func__);
    return 0;
  }

  return r;
}

static int gm_write_image (struct forth_stack *st, Image *image, char *filename, int quality) {
  vkprintf (3, "%s: '%s'\n", __func__, filename);
  if (strlen (filename) + 1 > sizeof (image->filename)) {
    return -1;
  }
  ImageInfo *image_info = CloneImageInfo (NULL);
  image_info->quality = quality;
  image_info->sampling_factor = AllocateString ("2x2");
  strcpy (image->filename, filename);
  vkprintf (4, "%s: image->filename: '%s'\n", __func__, image->filename);
#ifdef WEBP
  if (endswith (filename, ".webp", 5)) {
    int r = webp_save_image (st, image, image_info);
    DestroyImageInfo (image_info);
    return r;
  }
#endif
  if (WriteImage (image_info, image) == MagickFail) {
    fail_exception (st, &image->exception);
    DestroyImageInfo (image_info);
    return -1;
  }
  DestroyImageInfo (image_info);
  return 0;
}

static Image *gm_replace_transparent_color_by_white (struct forth_stack *st, Image *r) {
  Image *w = gm_read_image (st, "xc:white");
  if (w == NULL) {
    return NULL;
  }
  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  Image *v = ScaleImage (w, r->columns, r->rows, &exception);
  if (exception.severity != UndefinedException) {
    gm_catch_exception (st, &exception);
    DestroyImage (w);
    if (v != NULL) {
      DestroyImage (v);
    }
    return NULL;
  }
  DestroyExceptionInfo (&exception);
  DestroyImage (w);
  w = v;
  if (CompositeImage (w, OverCompositeOp, r, 0, 0) == MagickFail) {
    failf (st, "%s: CompositeImage failed", __func__);
    DestroyImage (w);
    return NULL;
  }
  return w;
}

static int get_thumbs_dimensions (int orig_width, int orig_height, int desired_width, int desired_height,
                                  int *new_width, int *new_height) {
	if (orig_width == desired_width && orig_height == desired_height) {
		*new_width  = desired_width;
		*new_height = desired_height;
		return 1;
	}

  if (desired_width <= 0 || desired_height <= 0) {
    return 0;
  }

  double ratio_x = (double)desired_width / (double)orig_width;
  double ratio_y = (double)desired_height / (double)orig_height;

  if (ratio_x < ratio_y) {
    *new_width  = desired_width;
    *new_height = (0.5 - 1e-9) + ratio_x * (double)orig_height;
  } else {
    *new_height = desired_height;
    *new_width  = (0.5 - 1e-9) + ratio_y * (double)orig_width;
  }

  if (*new_width < 1) {
    *new_width = 1;
  }

  if (*new_height < 1) {
    *new_height = 1;
  }
  return 1;
}

static Image *gm_minify_image (struct forth_stack *st, Image *image, int desired_width, int desired_height, FilterTypes filter) {
  int width, height;
  if (!get_thumbs_dimensions (image->columns, image->rows, desired_width, desired_height, &width, &height)) {
    failf (st, "%s: get_thumbs_dimensions fail", __func__);
    return 0;
  }
  ExceptionInfo exception;
  GetExceptionInfo (&exception);

  if (width > (int) image->columns || height > (int) image->rows) {
    return ReferenceImage (image);
  }

  Image *r = ResizeImage (image, width, height, filter, 1.0, &exception);

  if (exception.severity != UndefinedException) {
    gm_catch_exception (st, &exception);
    if (r != NULL) {
      DestroyImage (r);
    }
    return 0;
  }
  DestroyExceptionInfo (&exception);
  return r;
}

static Image *gm_resize_image (struct forth_stack *st, Image *image, int desired_width, int desired_height, int keep_aspect_ratio, FilterTypes filter) {
  int width = desired_width, height = desired_height;
  if (keep_aspect_ratio && !get_thumbs_dimensions (image->columns, image->rows, desired_width, desired_height, &width, &height)) {
    failf (st, "%s: get_thumbs_dimensions fail", __func__);
    return 0;
  }
  ExceptionInfo exception;
  GetExceptionInfo (&exception);

  //pthread_mutex_lock (&resize_image_mutex);
  Image *r = ResizeImage (image, width, height, filter, 1.0, &exception);
  //pthread_mutex_unlock (&resize_image_mutex);

  if (exception.severity != UndefinedException) {
    gm_catch_exception (st, &exception);
    if (r != NULL) {
      DestroyImage (r);
    }
    return 0;
  }
  DestroyExceptionInfo (&exception);
  return r;
}

static Image *gm_crop_image (struct forth_stack *st, Image *image, RectangleInfo *rect) {
  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  Image *r = CropImage (image, rect, &exception);
  if (exception.severity != UndefinedException) {
    gm_catch_exception (st, &exception);
    if (r != NULL) {
      DestroyImage (r);
    }
    return 0;
  }
  DestroyExceptionInfo (&exception);
  return r;
}

static Image *gm_rotate_image (struct forth_stack *st, Image *image, int angle) {
  assert (angle >= 0 && angle < 360);
  if (angle == 0) {
    return ReferenceImage (image);
  }
  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  Image *r = RotateImage (image, (double) angle, &exception);
  if (exception.severity != UndefinedException) {
    gm_catch_exception (st, &exception);
    if (r != NULL) {
      DestroyImage (r);
    }
    return 0;
  }
  DestroyExceptionInfo (&exception);
  return r;
}

////////////////////////////////////////////// Forth functions /////////////////////////////////////////////////////////////

#define FORTH_FAIL ((void*) 0)
#define FORTH_PASS ((void*) 1)


static const char *type_to_string (enum forth_literal_type tp) {
  switch (tp) {
  case ft_int:
    return "int";
  case ft_str:
    return "str";
  case ft_image:
    return "image";
  default:
    return "unknown";
  }
}

static FilterTypes get_filter_type (const char* s) {
  switch (*s++) {
  case 'b':
    if (!strcmp ("ox", s)) {
      return BoxFilter;
    }
    if (!strcmp ("lackman", s)) {
      return BlackmanFilter;
    }
    if (!strcmp ("essel", s)) {
      return BlackmanFilter;
    }
    break;
  case 'c':
    if (!strcmp ("ubic", s)) {
      return CubicFilter;
    }
    if (!strcmp ("atrom", s)) {
      return CatromFilter;
    }
    break;
  case 'g':
    if (!strcmp ("aussian", s)) {
      return GaussianFilter;
    }
    break;
  case 'h':
    if (!strcmp ("ermite", s)) {
      return HermiteFilter;
    }
    if (!strcmp ("anning", s)) {
      return HanningFilter;
    }
    break;
  case 'l':
    if (!strcmp ("anczos", s)) {
      return LanczosFilter;
    }
    break;
  case 'm':
    if (!strcmp ("itchell", s)) {
      return MitchellFilter;
    }
    break;
  case 'p':
    if (!strcmp ("oint", s)) {
      return PointFilter;
    }
    break;
  case 'q':
    if (!strcmp ("uadratic", s)) {
      return QuadraticFilter;
    }
    break;
  case 's':
    if (!strcmp ("inc", s)) {
      return SincFilter;
    }
    break;
  case 't':
    if (!strcmp ("riangle", s)) {
      return TriangleFilter;
    }
    break;
  }
  return UndefinedFilter;
}


typedef void *(*fpr_t)(void **, struct forth_stack *);
#define	NEXT	return (* (fpr_t *) IP)(IP + 1, st);


static void new_int (struct stack_entry *E, int i) {
  memcpy (&E->a, &i, 4);
  E->tp = ft_int;
}

static void new_str (struct stack_entry *E, char *str, int clone) {
  E->a = clone ? strdup (str) : str;
  E->tp = ft_str;
}

static void new_image (struct stack_entry *E, Image *I) {
  E->a =  I;
  E->tp = ft_image;
}

static void free_stack_entry (struct stack_entry *E) {
  switch (E->tp) {
  case ft_int:
  case ft_str:
    break;
  case ft_image:
    vkprintf (4, "free_stack_entry: DestroyImage (image->reference_count: %d)\n", (int) ((Image *) E->a)->reference_count);
    DestroyImage ((Image *) E->a);
  }
}

static void free_stack (struct forth_stack *st, int bottom, int top) {
  int i;
  for (i = bottom; i <= top; i++) {
    free_stack_entry (&st->x[i]);
  }
}

/*
static void *fail (struct forth_stack *st, char *msg) {
  if (msg[0]) {
    strcpy (st->error, msg);
  }
  return FORTH_FAIL;
}
*/

static void *failf (struct forth_stack *st, char *format, ...) {
  if (verbosity >= 2) {
    va_list aq;
    va_start (aq, format);
    vfprintf (stderr, format, aq);
    va_end (aq);
    fprintf (stderr, "\n");
  }
  const int l = sizeof (st->error);
  int o = st->error_len;
  if (l - o > 4) {
    st->error[o] = '\n';
    o++;
    st->error_len++;
    va_list ap;
    va_start (ap, format);
    st->error_len += vsnprintf (st->error + o, l - o, format, ap);
    va_end (ap);
    if (st->error_len >= l) {
      st->error[l - 1] = 0;
    }
  }
  return FORTH_FAIL;
}

static void *forth_bye (void **IP __attribute__((__unused__)), struct forth_stack *st __attribute__((__unused__))) {
  return FORTH_PASS;
}

static int check_type (struct forth_stack *st, int tp_bitset, char *who) {
  if (st->top < 0) {
    snprintf (st->error, sizeof (st->error), "%s (stack underflow)", who);
    return 0;
  }
  if (!(st->x[st->top].tp & tp_bitset)) {
    snprintf (st->error, sizeof (st->error), "%s (%s type found instead of 0x%x type))", who, type_to_string (st->x[st->top].tp), tp_bitset);
    return 0;
  }
  return 1;
}

static int pop_image (struct forth_stack *st, Image **image, char *who) __attribute__ ((warn_unused_result));
static int pop_image (struct forth_stack *st, Image **image, char *who) {
  if (!check_type (st, ft_image, who)) {
    return 0;
  }
  *image = (Image *) st->x[st->top--].a;
  return 1;
}

static int pop_str (struct forth_stack *st, char **i, char *who) __attribute__ ((warn_unused_result));
static int pop_str (struct forth_stack *st, char **i, char *who) {
  if (!check_type (st, ft_str, who)) {
    return 0;
  }
  *i = st->x[st->top--].a;
  return 1;
}

static int pop_int (struct forth_stack *st, int *i, char *who) __attribute__ ((warn_unused_result));
static int pop_int (struct forth_stack *st, int *i, char *who) {
  if (!check_type (st, ft_int, who)) {
    return 0;
  }
  memcpy (i, &st->x[st->top--].a, 4);
  return 1;
}

static void push_int (struct forth_stack *st, int x) {
  st->top++;
  new_int (&st->x[st->top], x);
}

static void push_image (struct forth_stack *st, Image *image) {
  st->top++;
  new_image (&st->x[st->top], image);
}

static void *lit_str (void **IP, struct forth_stack *st) {
  if (st->top == MAX_STACK_SIZE-1) {
    return failf (st, "lit_str (stack overflow)");
  }
  st->top++;
  new_str (&st->x[st->top], (char*) IP[0], 0);
  IP++;

  NEXT
}

static void *lit_int (void **IP, struct forth_stack *st) {
  if (st->top == MAX_STACK_SIZE-1) {
    return failf (st, "lit_int (stack overflow)");
  }
  int x;
  if (sscanf ((char*) IP[0], "%d", &x) != 1) {
    return failf (st, "lit_int (parse int fail)[%s]", (char *) IP[0]);
  }
  IP++;
  st->top++;
  new_int (&st->x[st->top], x);

  NEXT
}

static void *forth_nope (void **IP, struct forth_stack *st) {
  NEXT
}

static void *forth_min (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "min")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "min")) {
    return FORTH_FAIL;
  }
  push_int (st, (x <= y) ? x : y);
  NEXT
}

static void *forth_max (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "max")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "max")) {
    return FORTH_FAIL;
  }
  push_int (st, (x >= y) ? x : y);
  NEXT
}

static void *forth_add (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "+")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "+")) {
    return FORTH_FAIL;
  }
  push_int (st, x + y);
  NEXT
}

static void *forth_ge (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, ">=")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, ">=")) {
    return FORTH_FAIL;
  }
  push_int (st, x >= y ? -1 : 0);
  NEXT
}

static void *forth_eq (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "=")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "=")) {
    return FORTH_FAIL;
  }
  push_int (st, x == y ? -1 : 0);
  NEXT
}

static void *forth_neq (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "<>")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "<>")) {
    return FORTH_FAIL;
  }
  push_int (st, x != y ? -1 : 0);
  NEXT
}

static void *forth_le (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "<=")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "<=")) {
    return FORTH_FAIL;
  }
  push_int (st, x <= y ? -1 : 0);
  NEXT
}

static void *forth_less (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "<")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "<")) {
    return FORTH_FAIL;
  }
  push_int (st, x < y ? -1 : 0);
  NEXT
}

static void *forth_greater (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, ">")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, ">")) {
    return FORTH_FAIL;
  }
  push_int (st, x > y ? -1 : 0);
  NEXT
}

static void *forth_subtract (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "-")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "-")) {
    return FORTH_FAIL;
  }
  push_int (st, x - y);
  NEXT
}

static void *forth_multiply (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "*")) {
    return FORTH_FAIL;
  }
  if (!pop_int (st, &x, "*")) {
    return FORTH_FAIL;
  }
  push_int (st, x * y);
  NEXT
}

static void *forth_divide (void **IP, struct forth_stack *st) {
  int x, y;
  if (!pop_int (st, &y, "/")) {
    return FORTH_FAIL;
  }
  if (y == 0) {
    return failf (st, "division by zero");
  }
  if (!pop_int (st, &x, "/")) {
    return FORTH_FAIL;
  }
  push_int (st, x / y);
  NEXT
}

static void *forth_swap (void **IP, struct forth_stack *st) {
  if (st->top < 1) {
    return failf (st, "swap: stack underflow");
  }
  struct stack_entry tmp;
  memmove (&tmp, &st->x[st->top-1], sizeof (struct stack_entry));
  memmove (&st->x[st->top-1], &st->x[st->top], sizeof (struct stack_entry));
  memmove (&st->x[st->top], &tmp, sizeof (struct stack_entry));
  NEXT
}

static void *forth_rot (void **IP, struct forth_stack *st) {
  if (st->top < 2) {
    return failf (st, "rot: stack underflow");
  }
  struct stack_entry tmp;
  memmove (&tmp, &st->x[st->top-2], sizeof (struct stack_entry));
  memmove (&st->x[st->top-2], &st->x[st->top-1], sizeof (struct stack_entry));
  memmove (&st->x[st->top-1], &st->x[st->top], sizeof (struct stack_entry));
  memmove (&st->x[st->top], &tmp, sizeof (struct stack_entry));
  NEXT
}

static void *forth_drop (void **IP, struct forth_stack *st) {
  vkprintf (4, "forth_drop\n");
  if (st->top < 0) {
    return failf (st, "drop: stack underflow");
  }
  free_stack_entry (&st->x[st->top--]);
  NEXT
}

static void *forth_jz (void **IP, struct forth_stack *st) {
  int x;
  if (!pop_int (st, &x, "jz")) {
    return FORTH_FAIL;
  }
  if (!x) {
    IP = *IP;
  } else {
    IP++;
  }

  NEXT
}

static void *forth_jmp (void **IP, struct forth_stack *st) {
  IP = *IP;

  NEXT
}

static void *forth_dup (void **IP, struct forth_stack *st) {
  if (st->top == MAX_STACK_SIZE-1) {
    return failf (st, "dup: stack overflow");
  }
  if (st->top < 0) {
    return failf (st, "dup: stack underflow");
  }
  if (st->x[st->top].tp == ft_image) {
    push_image (st, ReferenceImage (st->x[st->top].a));
  } else {
    assert (st->x[st->top].tp & (ft_int | ft_str));
    memcpy (&st->x[st->top+1], &st->x[st->top], sizeof (struct stack_entry));
    (st->top)++;
  }
  NEXT
}

static void *forth_over (void **IP, struct forth_stack *st) {
  if (st->top == MAX_STACK_SIZE-1) {
    return failf (st, "over: stack overflow");
  }
  if (st->top < 1) {
    return failf (st, "over: stack underflow");
  }
  if (st->x[st->top-1].tp == ft_image) {
    push_image (st, ReferenceImage (st->x[st->top-1].a));
  } else {
    assert (st->x[st->top-1].tp & (ft_int | ft_str));
    memcpy (&st->x[st->top+1], &st->x[st->top-1], sizeof (struct stack_entry));
    (st->top)++;
  }
  NEXT
}

static int append_blob (struct forth_output *O, void *blob, size_t size) {
  if (O == NULL) { return -1; }
  int o = (sizeof (O->s) - O->l);
  if (o < size) {
    return -1;
  }
  memcpy (O->s + O->l, blob, size);
  O->l += size;
  return 0;
}

static void append_int (struct forth_output *O, int i) {
  if (O == NULL) { return; }
  int o = (sizeof (O->s) - O->l) - 2;
  if (o <= 0) { return; }
  int sz = snprintf (O->s + O->l, o, "%d\n", i);
  if (sz < 0 || sz >= o) { return; }
  O->l += sz;
}

static void append_str (struct forth_output *O, char *s) {
  if (O == NULL) { return; }
  int o = (sizeof (O->s) - O->l) - 2;
  if (o <= 0) { return; }
  int sz = snprintf (O->s + O->l, o, "%s\n", s);
  if (sz < 0 || sz >= o) { return; }
  O->l += sz;
}

static void *print (void **IP, struct forth_stack *st) {
  if (!check_type (st, ft_int | ft_str, "print")) {
    return FORTH_FAIL;
  }

  if (st->x[st->top].tp == ft_int) {
    int x;
    memcpy (&x, &st->x[st->top].a, 4);
    append_int (st->O, x);
  } else {
    append_str (st->O, (char *) st->x[st->top].a);
  }
  st->top--;

  NEXT
}

static void *save_image (void **IP, struct forth_stack *st) {
  vkprintf (3, "save_image\n");
  int old_stack_top = st->top;
  char *filename;
  int quality;
  if (!pop_int (st, &quality, "save_image 3rd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_str (st, &filename, "save_image 2nd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (strlen (filename) > MaxTextExtent-1) {
    st->top = old_stack_top;
    return failf (st, "save_image filename too long");
  }

  Image *image;
  if (!pop_image (st, &image, "save_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (gm_write_image (st, image, filename, quality) < 0) {
    st->top = old_stack_top;
    return failf (st, "save_image: WriteImage failed");
  }

  free_stack (st, st->top + 1, old_stack_top);

  NEXT
}

static void *copy_image (void **IP, struct forth_stack *st) {
  vkprintf (3, "copy_image\n");
  int old_stack_top = st->top;
  int quality;
  if (!pop_int (st, &quality, "copy_image 2nd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  Image *image;
  if (!pop_image (st, &image, "copy_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  ImageInfo *image_info = CloneImageInfo (NULL);
  image_info->quality = quality;
  image_info->sampling_factor = AllocateString ("2x2");
  strcpy (image_info->magick, "JPEG");

  size_t size = sizeof (st->O->s);
  void *blob = ImageToBlob (image_info, image, &size, &exception);

  if (blob == NULL) {
    DestroyImageInfo (image_info);
    gm_catch_exception (st, &exception);
    return failf (st, "copy_image: ImageToBlob throws exception");
  }

  DestroyImageInfo (image_info);
  DestroyExceptionInfo (&exception);

  vkprintf (4, "ImageToBlob: blob = %p, size = %lld\n", blob, (long long) size);

  if (append_blob (st->O, blob, size) < 0) {
    return failf (st, "copy_image: append_blob failed.");
  }

  free_stack (st, st->top + 1, old_stack_top);

  NEXT
}

static void *forth_ping (void **IP, struct forth_stack *st) {
  char *filename;
  int old_stack_top = st->top;
  if (!pop_str (st, &filename, "ping: 1st isn't str")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  if (strlen (filename) > MaxTextExtent-1) {
    return failf (st, "ping: filename too long");
  }
  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  ImageInfo *image_info = CloneImageInfo (NULL);
  strcpy (image_info->filename, filename);
  Image *r = PingImage (image_info, &exception);
  DestroyImageInfo (image_info);
  if (r == NULL) {
    st->top = old_stack_top;
    CatchException (&exception);
    DestroyExceptionInfo (&exception);
    return failf (st, "ping: PingImage (%s) throws exception", filename);
  }

  free_stack (st, st->top + 1, old_stack_top);

  push_image (st, r);

  NEXT;
}

static void *forth_get_attr (void **IP, struct forth_stack *st) {
  char *attr;
  Image *image;
  int old_stack_top = st->top;

  if (!pop_str (st, &attr, "getattr: 2nd isn't str")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "getattr: 1st isn't image")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  free_stack (st, st->top + 1, old_stack_top);

  const ImageAttribute *a = GetImageAttribute (image, attr);
  if (a == NULL) {
    st->top++;
    new_str (&st->x[st->top], "NULL", 0);
  } else {
    char *b = malloc (a->length+1); assert (b);
    memcpy (b, a->value, a->length);
    b[a->length] = 0;
    st->top++;
    new_str (&st->x[st->top], b, 0);
  }

  NEXT;
}

static void *forth_getimagesize (void **IP, struct forth_stack *st) {
  if (st->top >= MAX_STACK_SIZE - 1) {
    return failf (st, "getimagesize: stack overflow");
  }
  char *filename;

  int old_stack_top = st->top;
  if (!pop_str (st, &filename, "getimagesize: 1st isn't str")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  if (strlen (filename) > MaxTextExtent-1) {
    return failf (st, "getimagesize: filename too long");
  }

  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  ImageInfo *image_info = CloneImageInfo (NULL);
  strcpy (image_info->filename, filename);
  Image *r = PingImage (image_info, &exception);
  DestroyImageInfo (image_info);
  if (r == NULL) {
    st->top = old_stack_top;
    gm_catch_exception (st, &exception);
    return failf (st, "getimagesize: PingImage (%s) throws exception", filename);
  }

  DestroyExceptionInfo (&exception);

  free_stack (st, st->top + 1, old_stack_top);

  push_int (st, r->rows);
  push_int (st, r->columns);
  DestroyImage (r);

  NEXT

}

static void *forth_md5_hex (void **IP, struct forth_stack *st) {
  char *filename;
  unsigned char out[16];

  int old_stack_top = st->top;
  if (!pop_str (st, &filename, "md5_hex: 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (md5_file (filename, out)) {
    st->top = old_stack_top;
    return failf (st, "md5_hex: md5_file failed");
  }

  char s[33];
  if (1) {
    st->top = old_stack_top;
    return failf (st, "md5_hex: convert_to_hex failed");
  }

  free_stack (st, st->top + 1, old_stack_top);

  st->top++;
  new_str (&st->x[st->top], s, 1);

  NEXT
}

static void *forth_kid16_hex (void **IP, struct forth_stack *st) {
  vkprintf (4, "forth_kid16_hex\n");
  return FORTH_FAIL;
}

#ifdef DEBUG_HANG
static void *forth_hang (void **IP, struct forth_stack *st) {
  for (;;) {

  }

  NEXT
}
static void *forth_mhang (void **IP, struct forth_stack *st) {
  for (;;) {
    void *a = calloc (4096, 1);
    memset (a, 1, 4096);
    assert (a);
  }

  NEXT
}
#endif

static void *forth_kad16_hex (void **IP, struct forth_stack *st) {
  return FORTH_FAIL;
}

static void *load_image (void **IP, struct forth_stack *st) {
  vkprintf (3, "load_image\n");
  char *filename;
  int old_stack_top = st->top;
  if (!pop_str (st, &filename, "read_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  Image *r = gm_read_image (st, filename);

  if (r == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  free_stack (st, st->top + 1, old_stack_top);

  push_image (st, r);

  NEXT;
}

static void *crop_image (void **IP, struct forth_stack *st) {
  int old_stack_top = st->top;
  int x, y, width, height;
  Image *image;

  if (!pop_int (st, &height, "crop_image 5th")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &width, "crop_image 4th")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &y, "crop_image 3rd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &x, "crop_image 2nd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "crop_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  RectangleInfo rect;
  rect.x = x;
  rect.y = y;
  rect.width = width;
  rect.height = height;
  Image *r = gm_crop_image (st, image, &rect);
  if (r == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  free_stack (st, st->top + 1, old_stack_top);

  push_image (st, r);

  NEXT;

}

static void *rotate_image (void **IP, struct forth_stack *st) {
  Image *image;
  int old_stack_top = st->top;
  int angle;

  if (!pop_int (st, &angle, "rotate_image 2nd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "rotate_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  angle %= 4;
  if (angle < 0) {
    angle += 4;
  }
  Image *r = gm_rotate_image (st, image, 90 * angle);
  if (r == NULL) {
    st->top = old_stack_top;
    return failf (st, "rotate_image: gm_rotate_image fail");
  }

  free_stack (st, st->top + 1, old_stack_top);

  push_image (st, r);

  NEXT;
}

static void *forth_minify_keep_aspect_ratio (void **IP, struct forth_stack *st) {
  Image *image;
  int old_stack_top = st->top;
  int width, height;
  char *resize_filter_name;
  if (!pop_str (st, &resize_filter_name, "MINIFY_KEEP_ASPECT_RATIO: filter")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &height, "MINIFY_KEEP_ASPECT_RATIO: height")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &width, "MINIFY_KEEP_ASPECT_RATIO: width")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "MINIFY_KEEP_ASPECT_RATIO: image")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  FilterTypes resize_filter = get_filter_type (resize_filter_name);

  Image *w = gm_minify_image (st, image, width, height, resize_filter);

  if (w == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  free_stack (st, st->top + 1, old_stack_top);
  push_image (st, w);

  NEXT

}

static void *forth_resize_keep_aspect_ratio (void **IP, struct forth_stack *st) {
  Image *image;
  int old_stack_top = st->top;
  int width, height;
  char *resize_filter_name;
  if (!pop_str (st, &resize_filter_name, "RESIZE_KEEP_ASPECT_RATIO: filter")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &height, "RESIZE_KEEP_ASPECT_RATIO: height")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &width, "RESIZE_KEEP_ASPECT_RATIO: width")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "RESIZE_KEEP_ASPECT_RATIO: image")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  FilterTypes resize_filter = get_filter_type (resize_filter_name);

  Image *w = gm_resize_image (st, image, width, height, 1, resize_filter);
  if (w == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  free_stack (st, st->top + 1, old_stack_top);
  push_image (st, w);

  NEXT
}

static void *forth_resize_borderless (void **IP, struct forth_stack *st) {
  Image *image;
  int old_stack_top = st->top;
  int width, height;
  char *resize_filter_name;
  if (!pop_str (st, &resize_filter_name, "RESIZE_BORDERLESS: filter")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &height, "RESIZE_BORDERLESS: height")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &width, "RESIZE_BORDERLESS: width")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "RESIZE_BORDERLESS: image")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  FilterTypes filter = (get_filter_type (resize_filter_name));

  int orig_width = image->columns, orig_height = image->rows;
  double dest_width = ((double) orig_width * (double) height) / (double) orig_height;
  int resize_width, resize_height, crop = 1;
  RectangleInfo rect;
  if (dest_width >= width) {
    resize_width = (int) (dest_width + 0.5);
    resize_height = height;
    rect.x = (resize_width - width) / 2;
    rect.y = 0;
    if (resize_width == width) {
      crop = 0;
    }
  } else {
    resize_width = width;
    resize_height = ((double) orig_height * (double) width) / (double) orig_width;
    rect.x = 0;
    rect.y = (resize_height - height) / 2;
    if (resize_height == height) {
      crop = 0;
    }
  }
  rect.width = width;
  rect.height = height;

  Image *r = gm_resize_image (st, image, resize_width, resize_height, 0, filter);
  if (r == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (crop) {
    Image *w = gm_crop_image (st, r, &rect);
    DestroyImage (r);
    if (w != NULL) {
      st->top = old_stack_top;
      return failf (st, "%s: gm_crop_image fail", __func__);
    }
    r = w;
  }

  free_stack (st, st->top + 1, old_stack_top);
  push_image (st, r);

  NEXT
}

static void *resize_image (void **IP, struct forth_stack *st) {
  Image *image;
  int old_stack_top = st->top;
  int width, height;
  if (!pop_int (st, &height, "resize_image 3rd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &width, "resize_image 2nd")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_image (st, &image, "resize_image 1st")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  ExceptionInfo exception;
  GetExceptionInfo (&exception);
  Image *r = ResizeImage (image, width, height, LanczosFilter, 1.0, &exception);
  if (exception.severity != UndefinedException) {
    st->top = old_stack_top;
    gm_catch_exception (st, &exception);
    if (r != NULL) {
      DestroyImage (r);
    }
    return FORTH_FAIL;
  }
  DestroyExceptionInfo (&exception);

  free_stack (st, st->top + 1, old_stack_top);

  push_image (st, r);

  NEXT;
}

struct thumb {
  int width;
  int height;
  char *filename;
  int quality;
};

/*
  's' => array('width' => 75, 'height' => 75),     0
  'm' => array('width' => 130, 'height' => 130),   1
  'x' => array('width' => 604, 'height' => 604),   2
  'y' => array('width' => 807, 'height' => 807),   3
  'z' => array('width' => 1280, 'height' => 1024), 4
  'w' => array('width' => 2560, 'height' => 2048)  5
*/

static int get_max_save_size (const char *const s) {
  if (strlen (s) != 1) {
    return 5;
  }
  switch (s[0]) {
  case 'w': return 5;
  case 'z': return 4;
  case 'y': return 3;
  case 'x': return 2;
  case 'm': return 1;
  case 's': return 0;
  default: return 5;
  }
}

static void fix_angle_using_orientation (Image *image, int *angle) {
  switch (image->orientation) {
    case BottomRightOrientation:
      (*angle) += 2;
      break;
    case RightTopOrientation:
      (*angle) += 1;
      break;
    case LeftBottomOrientation:
      (*angle) += 3;
      break;
    default:
      break;
  }
}

static void *generate_thumbs_matte_resize_rotate (struct forth_stack *st, Image **I, int angle, int *width, int *height, int max_width, int max_height, FilterTypes resize_filter, int *trans_flags) {
  if (trans_flags) {
    *trans_flags = 0;
  }
  Image *r = *I, *w;
  *width = r->columns;
  *height = r->rows;

  if ((angle % 2) != 0) {
    int tmp = *width; *width = *height; *height = tmp;
  }

  /******************* resizeAndRotate ************************/
  if (*width > max_width || *height > max_height) {
    if (trans_flags) {
      (*trans_flags) |= 1;
    }
    w = gm_resize_image (st, r, max_width, max_height, 1, resize_filter);
    if (w == NULL) {
      DestroyImage (r);
      return FORTH_FAIL;
    }
    DestroyImage (r);
    r = w;
  }

  vkprintf (3, "[%d] after gm_resize_image\n", st->thread_id);
  angle %= 4;
  if (angle < 0) {
    angle += 4;
  }
  if (angle) {
    if (trans_flags) {
      (*trans_flags) |= 2;
    }
    w = gm_rotate_image (st, r, 90 * angle);
    if (w == NULL) {
      DestroyImage (r);
      return FORTH_FAIL;
    }
    DestroyImage (r);
    r = w;
  }

  vkprintf (3, "[%d] after gm_rotate_image\n", st->thread_id);

  if (r->matte || r->colorspace != RGBColorspace) {
    if (trans_flags) {
      (*trans_flags) |= 4;
    }
    w = gm_replace_transparent_color_by_white (st, r);
    if (w == NULL) {
      DestroyImage (r);
      return FORTH_FAIL;
    }
    DestroyImage (r);
    r = w;
  }

  *width = r->columns;
  *height = r->rows;
  *I = r;
  return FORTH_PASS;
}

static void *generate_thumbs (void **IP, struct forth_stack *st) {
  int i, quality, angle, unlink_orig_file;
  struct thumb t[6];
  char *orig_filename;
  char *resize_filter_name;
  int old_stack_top = st->top;
  for (i = 5; i >= 0; i--) {
    if (!pop_str (st, &t[i].filename, "generate_thumbs (thumb filename)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (strlen(t[i].filename) >  MaxTextExtent-1) {
      st->top = old_stack_top;
      return failf (st, "generate_thums (thumb filename too long)");
    }

    if (!pop_int (st, &t[i].height, "generate_thumbs (thumb height)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (!pop_int (st, &t[i].width, "generate_thumbs (thumb width)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
  }

  if (!pop_int (st, &unlink_orig_file, "generate_thumbs (unlink_orig_file)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  char *max_save_size;
  if (!pop_str (st, &max_save_size, "generate_thumbs (max_save_size)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  int mss = get_max_save_size (max_save_size);

  if (!pop_int (st, &quality, "generate_thumbs (quality)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_str (st, &resize_filter_name, "generate_thumbs (filter)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }
  if (!pop_int (st, &angle, "generate_thumbs (angle)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_str (st, &orig_filename, "generate_thumbs (orig_filename)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  vkprintf (3, "[%d] before read_image %s\n", st->thread_id, orig_filename);

  Image *w, *r = gm_read_image (st, orig_filename);
  if (r == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  vkprintf (2, "[%d] after read_image %s\n", st->thread_id, orig_filename);

  fix_angle_using_orientation (r, &angle);

  FilterTypes resize_filter = get_filter_type (resize_filter_name);
  int width, height;
  if (FORTH_FAIL == generate_thumbs_matte_resize_rotate (st, &r, angle, &width, &height, t[5].width, t[5].height, resize_filter, NULL)) {
     st->top = old_stack_top;
     return FORTH_FAIL;
  }

  int max_size = 2;
  for (i = 4; i >= 2; i--) {
    if (width > t[i].width || height > t[i].height) {
      max_size = i + 1;
      break;
    }
  }

  vkprintf (3, "max_size = %d, t[%d].filename = %s\n", max_size, max_size, t[max_size].filename);

  for (i = max_size; i >= 0; i--) {
    if ((int) r->columns > t[i].width || (int) r->rows > t[i].height) {
      vkprintf (3, "[%d] before gm_resize_image(%d)\n", st->thread_id, i);
      w = gm_resize_image (st, r, t[i].width, t[i].height, 1, resize_filter);
      vkprintf (3, "[%d] after gm_resize_image(%d)\n", st->thread_id, i);
      if (w == NULL) {
        st->top = old_stack_top;
        DestroyImage (r);
        return FORTH_FAIL;
      }
      DestroyImage (r);
      r = w;
    }
    if (i <= mss) {
      if (gm_write_image (st, r, t[i].filename, quality) < 0) {
        st->top = old_stack_top;
        return failf (st, "generate_thumbs: WriteImage failed");
      }
    }
    vkprintf (3, "[%d] after generate_thumbs(%d)\n", st->thread_id, i);
  }
  DestroyImage (r);

  vkprintf (3, "[%d] after generate_thumbs\n", st->thread_id);

  if (unlink_orig_file) {
    if (unlink (orig_filename)) {
      st->top = old_stack_top;
      return failf (st, "generate_thumbs: unlink %s fail", orig_filename);
    }
  }

  free_stack (st, st->top + 1, old_stack_top);

  NEXT

}

static int endswith (const char *s, const char *suffix, int suffix_len) {
  int l = strlen (s);
  return l >= suffix_len && !strcmp (s + (l - suffix_len), suffix);
}

static int jpg_delete_profiles (const char *src, const char *dst) {
  int fd = open (src, O_RDONLY);
  if (fd < 0) {
    kprintf ("%s: open '%s' in readonly mode failed. %m\n", __func__, src);
    return -1;
  }
  struct stat st;
  if (fstat (fd, &st) < 0) {
    assert (!close (fd));
    return -1;
  }
  if (st.st_size <= 2 || st.st_size > (4 << 20)) {
    return -1;
  }
  const int n = st.st_size;
  unsigned char *a = alloca (n);
  if (read (fd, a, n) != n) {
    kprintf ("%s: read from file '%s' failed.\n", __func__, src);
    assert (!close (fd));
    return -1;
  }

  if (a[0] != 0xff && a[1] != 0xd8) {
    return -1;
  }

  char tbl[256];
  memset (tbl, 0, 256);
  tbl[0xc0]= tbl[0xc2]= tbl[0xc4] = tbl[0xdb]= tbl[0xdd]= tbl[0xe0] = 1;

  int i = 2, m = 2;

  while (1) {
    unsigned char c;
    if (i >= n || a[i++] != 0xff) {
      return -1;
    }
    if (i >= n) {
      return -1;
    }
    c = a[i++];
    if (c == 0xc0 || c== 0xda) {
      a[m++] = 0xff;
      a[m++] = c;
      const int len = n - i;
      memmove (a + m, a + i, len);
      m += len;
      i += len;
      break;
    }
    if (i + 2 > n) {
      return -1;
    }
    const int h = a[i++], l = a[i++], len = ((h<<8) | l) - 2;
    if (len < 0 || i + len > n) {
      return -1;
    }
    if (tbl[c]) {
      a[m++] = 0xff;
      a[m++] = c;
      a[m++] = h;
      a[m++] = l;
      memmove (a + m, a + i, len);
      m += len;
      i += len;
    } else {
      i += len;
    }
  }

  fd = open (dst, O_WRONLY | O_TRUNC | O_CREAT, 0640);
  if (fd < 0) {
    kprintf ("%s: creat '%s' failed. %m\n", __func__, dst);
    return -1;
  }

  if (write (fd, a, m) != m) {
    kprintf ("%s: write to file '%s' failed. %m\n", __func__, dst);
    assert (!close (fd));
    return -2;
  }

  assert (!close (fd));
  return 0;
}

struct thumb2 {
  char *filename;
  int width;
  int min_height;
  int max_height;
  int computed;
  int quality;
};

#define THUMB2_RESIZE_MASK 1
#define THUMB2_CROP_MASK 2

static inline int resize_height (double old_width, double new_width, double old_height) {
  return (int) (old_height * (new_width / old_width) + 0.5);
}

static int get_thumb2_transforms (int width, int height, struct thumb2 *u, RectangleInfo *resize_rect, RectangleInfo *crop_rect) {
  memset (resize_rect, 0, sizeof (RectangleInfo));
  memset (crop_rect, 0, sizeof (RectangleInfo));
  int r = 0;
  if (width <= u->width) {
    if (height > u->max_height) {
      r |= THUMB2_CROP_MASK;
      crop_rect->width = width;
      crop_rect->height = u->max_height;
      crop_rect->x = crop_rect->y = 0;
    }
    return r;
  }
  int y = resize_height (width, u->width, height);
  if (y >= u->min_height) {
    r |= THUMB2_RESIZE_MASK;
    resize_rect->width = u->width;
    resize_rect->height = y;
    if (y > u->max_height) {
      r |= THUMB2_CROP_MASK;
      crop_rect->width = u->width;
      crop_rect->height = u->max_height;
      crop_rect->x = crop_rect->y = 0;
    }
    return r;
  }
  if (height < u->min_height) {
    crop_rect->width = u->width;
    crop_rect->height = height;
    crop_rect->x = (width - u->width) / 2;
    crop_rect->y = 0;
    return THUMB2_CROP_MASK;
  }
  assert (width > u->width && height >= u->min_height);
  int x = resize_height (height, u->min_height, width);
  resize_rect->width = x;
  resize_rect->height = u->min_height;
  crop_rect->width = u->width;
  crop_rect->height = u->min_height;
  crop_rect->x = (x - u->width) / 2;
  crop_rect->y = 0;
  return THUMB2_RESIZE_MASK | THUMB2_CROP_MASK;
}

static int thumbs2_want_resize (int width, int height, struct thumb2 *u) {
  return width >= u->width && height >= u->max_height;
  /*
  RectangleInfo resize_rect, crop_rect;
  int res = get_thumb2_transforms (width, height, u, &resize_rect, &crop_rect);
  return (res & THUMB2_RESIZE_MASK) ? 1 : 0;
  */
}

static void *make_thumbs2 (Image *r, struct thumb2 *u, struct forth_stack *st, const FilterTypes resize_filter, const int quality) {
  const int width = r->columns, height = r->rows;
  RectangleInfo resize_rect, crop_rect;
  int res = get_thumb2_transforms (width, height, u, &resize_rect, &crop_rect);
  Image *w = ReferenceImage (r);

  if (res & THUMB2_RESIZE_MASK) {
    Image *v = gm_resize_image (st, w, resize_rect.width, resize_rect.height, 0, resize_filter);
    if (v == NULL) {
      DestroyImage (w);
      return failf (st, "GENERATE_THUMBS2: gm_resize_image fail. %s", u->filename);
    }
    DestroyImage (w);
    w = v;
  }

  if (res & THUMB2_CROP_MASK) {
    Image *v = gm_crop_image (st, w, &crop_rect);
    if (v == NULL) {
      DestroyImage (w);
      return failf (st, "GENERATE_THUMBS2: gm_crop_image fail. %s", u->filename);
    }
    DestroyImage (w);
    w = v;
  }

  if (gm_write_image (st, w, u->filename, u->quality) < 0) {
    DestroyImage (w);
    return failf (st, "GENERATE_THUMBS2: write_image fail. %s", u->filename);
  }

  DestroyImage (w);
  u->computed = 1;
  return FORTH_PASS;
}

static long long get_file_size (const char *filename) {
  struct stat st;
  if (stat (filename, &st) < 0) {
    return -1LL;
  }
  vkprintf (4, "%s: filename: %s, size: %lld bytes.\n", __func__, filename, (long long) st.st_size);

  return st.st_size;
}

static void extract_filename_quality (char *filename, int *r, int quality) {
  vkprintf (3, "%s: filename: %s\n", __func__, filename);
  *r = quality;
  char *p = strstr (filename, "?q=");
  if (p) {
    int ok = 0;
    *p = 0;
    quality = 0;
    p += 3;
    while (*p) {
      if (isdigit (*p)) {
        quality *= 10;
        quality += (*p) - '0';
        ok++;
      } else {
        ok = -1;
        break;
      }
      p++;
    }
    if (ok > 0) {
      *r = quality;
    }
  }
}

static void *generate_thumbs2 (void **IP, struct forth_stack *st) {
  int i, quality, angle, unlink_orig_file, m, n, required_save_size, max_save_size, old_stack_top = st->top;
  struct thumb *t;
  struct thumb2 *u;
  char *orig_filename, *resize_filter_name;

  if (!pop_int (st, &m, "GENERATE_THUMBS2: (m)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (m < 0 || m > 30) {
    failf (st, "GENERATE_THUMBS2: (m = %d is out of range)", m);
  }

  u = calloc (m+1, sizeof (u[0]));
  for (i = m - 1; i >= 0; i--) {
    if (!pop_str (st, &u[i].filename, "GENERATE_THUMBS2: (thumb2 filename)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (strlen (u[i].filename) >  MaxTextExtent-1) {
      st->top = old_stack_top;
      return failf (st, "generate_thums2 (thumb2 filename too long)");
    }

    if (!pop_int (st, &u[i].max_height, "GENERATE_THUMBS2: (thumb2 max_height)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (!pop_int (st, &u[i].min_height, "GENERATE_THUMBS2: (thumb2 min_height)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (!pop_int (st, &u[i].width, "GENERATE_THUMBS2: (thumb2 width)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
  }

  if (!pop_int (st, &n, "GENERATE_THUMBS2: (n)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (n < 1 || n > 30) {
    failf (st, "GENERATE_THUMBS2: (n = %d is out of range)", n);
  }

  t = calloc (n + 1, sizeof (t[0]));

  for (i = n - 1; i >= 0; i--) {
    if (!pop_str (st, &t[i].filename, "GENERATE_THUMBS2: (thumb filename)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (strlen(t[i].filename) >  MaxTextExtent-1) {
      st->top = old_stack_top;
      return failf (st, "GENERATE_THUMBS2: (thumb filename too long)");
    }
    if (!pop_int (st, &t[i].height, "GENERATE_THUMBS2: (thumb height)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
    if (!pop_int (st, &t[i].width, "GENERATE_THUMBS2: (thumb width)")) {
      st->top = old_stack_top;
      return FORTH_FAIL;
    }
  }

  if (!pop_int (st, &unlink_orig_file, "GENERATE_THUMBS2: (unlink_orig_file)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &max_save_size, "GENERATE_THUMBS2: (max_save_size)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (max_save_size < 0 || max_save_size >= n) {
    st->top = old_stack_top;
    return failf (st, "GENERATE_THUMBS2: max_save_size is out of range (0 <= max_save_size < n)");
  }

  if (!pop_int (st, &required_save_size, "GENERATE_THUMBS2: (required_save_size)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (required_save_size < 0 || required_save_size >= n) {
    st->top = old_stack_top;
    return failf (st, "GENERATE_THUMBS2: required_save_size is out of range (0 <= required_save_size < n)");
  }

  if (!pop_int (st, &quality, "GENERATE_THUMBS2: (quality)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  for (i = 0; i < n; i++) {
    extract_filename_quality (t[i].filename, &t[i].quality, quality);
  }

  for (i = 0; i < m; i++) {
    extract_filename_quality (u[i].filename, &u[i].quality, quality);
  }

  if (!pop_str (st, &resize_filter_name, "GENERATE_THUMBS2: (filter)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_int (st, &angle, "GENERATE_THUMBS2: (angle)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  if (!pop_str (st, &orig_filename, "GENERATE_THUMBS2: (orig_filename)")) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  vkprintf (3, "[%d] before read_image %s\n", st->thread_id, orig_filename);

  Image *w, *r = gm_read_image (st, orig_filename);
  if (r == NULL) {
    st->top = old_stack_top;
    return FORTH_FAIL;
  }

  const int orig_columns = r->columns, orig_rows = r->rows;

  vkprintf (2, "[%d] after read_image %s\n", st->thread_id, orig_filename);
  fix_angle_using_orientation (r, &angle);

  FilterTypes resize_filter = get_filter_type (resize_filter_name);
  int width, height, transform_flags;
  if (FORTH_FAIL == generate_thumbs_matte_resize_rotate (st, &r, angle, &width, &height, t[n-1].width, t[n-1].height, resize_filter, &transform_flags)) {
     st->top = old_stack_top;
     return FORTH_FAIL;
  }
  vkprintf (4, "%s: Image encoding format is %s.\n", __func__, r->magick);
  const int original_image_is_jpeg = !strcmp (r->magick, "JPEG");
  int max_size = required_save_size;
  for (i = n-2; i >= required_save_size; i--) {
    if (width > t[i].width || height > t[i].height) {
      max_size = i + 1;
      break;
    }
  }

  vkprintf (3, "max_size = %d, t[%d].filename = %s\n", max_size, max_size, t[max_size].filename);

  int iphone_thumbs_remaining = m, j;

  struct {
    char *filename;
    unsigned int rows;
    unsigned int columns;
  } last_saved_image = {
    .rows = 0,
    .columns = 0,
    .filename = NULL
  };

  for (i = max_size; i >= 0; i--) {
    if ((int) r->columns > t[i].width || (int) r->rows > t[i].height) {

      if (iphone_thumbs_remaining > 0) {
        int small_width = -1, small_height = -1;
        if (!get_thumbs_dimensions (r->columns, r->rows, t[i].width, t[i].height, &small_width, &small_height)) {
          st->top = old_stack_top;
          return failf (st, "GENERATE_THUMBS2: get_thumbs_dimensions (%d, %d, %d, %d) failed.", (int) r->columns, (int) r->rows, t[i].width, t[i].height);
        }
        assert (small_width >= 0 && small_height >= 0);
        for (j = 0; j < m; j++) {
          if (!u[j].computed) {
             if (!thumbs2_want_resize (r->columns, r->rows, u + j) || !thumbs2_want_resize (small_width, small_height, u + j)) {
               iphone_thumbs_remaining--;
               if (FORTH_FAIL == make_thumbs2 (r, u + j, st, resize_filter, quality)) {
                 DestroyImage (r);
                 st->top = old_stack_top;
                 return FORTH_FAIL;
               }
             }
          }
        }
      }

      vkprintf (3, "[%d] before gm_resize_image(%d)\n", st->thread_id, i);
      w = gm_resize_image (st, r, t[i].width, t[i].height, 1, resize_filter);
      vkprintf (3, "[%d] after gm_resize_image(%d)\n", st->thread_id, i);

      if (w == NULL) {
        st->top = old_stack_top;
        DestroyImage (r);
        return FORTH_FAIL;
      }
      DestroyImage (r);
      r = w;
    }
    if (i <= max_save_size) {
      int ok = 0;
      if (last_saved_image.filename && last_saved_image.rows == r->rows && last_saved_image.columns == r->columns) {
        if (link (last_saved_image.filename, t[i].filename) < 0) {
          kprintf ("%s: link ('%s', '%s') fail. %m\n", __func__, last_saved_image.filename, t[i].filename);
        } else {
          ok = 1;
        }
      }
      if (!ok) {
        if (gm_write_image (st, r, t[i].filename, t[i].quality) < 0) {
          st->top = old_stack_top;
          return failf (st, "%s: WriteImage failed", __func__);
        }
        vkprintf (4, "%s: transform_flags = %d, r->columes = %d, orig_columns = %d, r->rows = %d, orig_rows = %d, dest_jpg = %d\n",
            __func__, transform_flags, (int) r->columns, orig_columns, (int) r->rows, orig_rows, endswith (t[i].filename, ".jpg", 4));
        if (original_image_is_jpeg && !transform_flags && r->columns == orig_columns && r->rows == orig_rows && endswith (t[i].filename, ".jpg", 4) &&
            get_file_size (orig_filename) < get_file_size (t[i].filename)) {
          int res = jpg_delete_profiles (orig_filename, t[i].filename);
          vkprintf (3, "jpg_delete_profiles ('%.256s', '%.256s'), res: %d\n", orig_filename, t[i].filename, res);
          if (res < 0) {
            st->top = old_stack_top;
            return failf (st, "%s: jpg_delete_profiles ('%.128s', '%.128s') failed", __func__, orig_filename, t[i].filename);
          }
        }
        last_saved_image.filename = t[i].filename;
        last_saved_image.rows = r->rows;
        last_saved_image.columns = r->columns;
      }
    }
    vkprintf (3, "[%d] after generate_thumbs(%d)\n", st->thread_id, i);
  }

  for (j = 0; j < m; j++) {
    if (!u[j].computed) {
      if (FORTH_FAIL == make_thumbs2 (r, u + j, st, resize_filter, quality)) {
        DestroyImage (r);
        st->top = old_stack_top;
        return FORTH_FAIL;
      }
    }
  }

  DestroyImage (r);

  if (unlink_orig_file) {
    if (unlink (orig_filename)) {
      st->top = old_stack_top;
      return failf (st, "GENERATE_THUMBS2: unlink %s fail", orig_filename);
    }
  }

  free (u);
  free (t);

  free_stack (st, st->top + 1, old_stack_top);
  vkprintf (3, "[%d] after generate_thumbs2\n", st->thread_id);

  NEXT
}

static int ip_append (void **IP, int *n, int ip_size, void *cmd, char last_error[MAX_ERROR_BUF_SIZE]) {
  vkprintf (4, "ip_append (*n = %d, cmd = %p)\n", *n, cmd);
  if (*n >= ip_size) {
    snprintf (last_error, sizeof (last_error[0]) * MAX_ERROR_BUF_SIZE, "too many commands (%d)", *n);
    return 0;
  }
  IP[(*n)++] = cmd;
  return 1;
}

/************************** Reserved words hashtable *************************/
#define RESERVED_WORDS_HASHTABLE_EXP 9
#define RESERVED_WORDS_HASHTABLE_SIZE (1 << RESERVED_WORDS_HASHTABLE_EXP)

const char *RESERVED_WORDS_H[RESERVED_WORDS_HASHTABLE_SIZE];
fpr_t RESERVED_WORDS_F[RESERVED_WORDS_HASHTABLE_SIZE];
int reserved_words, unperfect_hash;
#define ERR_UNKNOWN_RESERVED_WORD (-1)
#define ERR_CYRILIC_SYMBOL (-2)
static int get_cmd_f (const char* cmd, int force) {
  unsigned hc = 0;
  const char *p = cmd;
  while (*p) {
    char c = *p++;
    if (c < 0) {
      return ERR_CYRILIC_SYMBOL;
    }
    if (islower (c)) {
      c = toupper (c);
    }
    hc *= 131;
    hc += c;
  }

  unsigned i = hc &= RESERVED_WORDS_HASHTABLE_SIZE - 1;
  while (RESERVED_WORDS_H[i]) {
    if (!strcasecmp (cmd, RESERVED_WORDS_H[i])) {
      return i;
    }
    if (++i == RESERVED_WORDS_HASHTABLE_SIZE) {
      i = 0;
    }
  }

  if (force) {
    RESERVED_WORDS_H[i] = cmd;
    reserved_words++;
    if (i != hc) {
      unperfect_hash++;
    }
    return i;
  } else {
    return ERR_UNKNOWN_RESERVED_WORD;
  }
}

static int add_reserved_word (const char* cmd, fpr_t f) {
  int i = get_cmd_f (cmd, 1);
  assert (i >= 0);
  RESERVED_WORDS_F[i] = f;
  vkprintf (3, "add_reserved_word %s at slot %d\n", cmd, i);
  return i;
}

int id_if, id_else, id_then;
void image_reserved_words_hashtable_init (void) {
  memset (RESERVED_WORDS_H, 0, sizeof (RESERVED_WORDS_H));
  reserved_words = unperfect_hash = 0;
  add_reserved_word ("GENERATE_THUMBS" , generate_thumbs);
  add_reserved_word ("GENERATE_THUMBS2" , generate_thumbs2);
  add_reserved_word ("DUP", forth_dup);
  add_reserved_word ("LOAD", load_image);
  add_reserved_word ("SAVE", save_image);
  add_reserved_word ("COPY", copy_image);
  add_reserved_word ("PING", forth_ping);
  add_reserved_word ("GETATTR", forth_get_attr);
  add_reserved_word ("RESIZE", resize_image);
  add_reserved_word ("RESIZE_KEEP_ASPECT_RATIO", forth_resize_keep_aspect_ratio);
  add_reserved_word ("MINIFY_KEEP_ASPECT_RATIO", forth_minify_keep_aspect_ratio);
  add_reserved_word ("RESIZE_BORDERLESS", forth_resize_borderless);
  add_reserved_word ("ROTATE", rotate_image);
  add_reserved_word ("CROP", crop_image);
  add_reserved_word ("GETIMAGESIZE", forth_getimagesize);
  add_reserved_word ("PRINTLN", print);
  id_if = add_reserved_word ("IF", NULL);
  id_else = add_reserved_word ("ELSE", NULL);
  id_then = add_reserved_word ("THEN", NULL);
  add_reserved_word ("+", forth_add);
  add_reserved_word ("-", forth_subtract);
  add_reserved_word ("*", forth_multiply);
  add_reserved_word ("/", forth_divide);
  add_reserved_word (">=", forth_ge);
  add_reserved_word ("<=", forth_le);
  add_reserved_word ("=", forth_eq);
  add_reserved_word ("<>", forth_neq);
  add_reserved_word ("<", forth_less);
  add_reserved_word (">", forth_greater);
  add_reserved_word ("MIN", forth_min);
  add_reserved_word ("MAX", forth_max);
  add_reserved_word ("SWAP", forth_swap);
  add_reserved_word ("DROP", forth_drop);
  add_reserved_word ("ROT", forth_rot);
  add_reserved_word ("OVER", forth_over);
  add_reserved_word ("BYE", forth_bye);
  add_reserved_word ("MD5_FILE_HEX", forth_md5_hex);
  add_reserved_word ("KID16_HEX", forth_kid16_hex);
  add_reserved_word ("KAD16_HEX", forth_kad16_hex);

#ifdef DEBUG_HANG
  add_reserved_word ("HANG", forth_hang);
  add_reserved_word ("MHANG", forth_mhang);
#endif
  if (unperfect_hash) {
    vkprintf (1, "Warning: reserved words hash table (hash function is unperfect, try increase RESERVED_WORDS_HASHTABLE_EXP)\n"
                     "unperfect_reserved_words = %d\n", unperfect_hash);
  }
}

static int add_lit_str (char *s, void **IP, int *n, int ip_size, char last_error[MAX_ERROR_BUF_SIZE])  {
  if (!ip_append (IP, n, ip_size, lit_str, last_error)) {
    return 0;
  }

  if (!ip_append (IP, n, ip_size, s, last_error)) {
    return 0;
  }
  return 1;
}

#define MAX_COND_STACK_SIZE 128
struct forth_condition_stack_entry {
  int jz_addr_ip_idx;
  int jmp_addr_before_else;
  int was_else_statement;
};
struct forth_condition_stack {
  int top;
  struct forth_condition_stack_entry x[MAX_COND_STACK_SIZE];
};

static int parse_token (char *s, void **IP, int *n, int ip_size, struct forth_condition_stack *CS, char last_error[MAX_ERROR_BUF_SIZE]) {
  vkprintf (3, "parse_token (%s, *n = %d)\n", s, *n);

  int i = get_cmd_f (s, 0);
  /*
  if (i == ERR_UNKNOWN_RESERVED_WORD) {
    if (verbosity >= 3) {
      fprintf (stderr, "%s is unknown reserved word\n", s);
    }
    return 0;
  }
  */

  if (i >= 0) {
    void *cmd = RESERVED_WORDS_F[i];
    if (cmd == NULL) {
      if (i == id_if) {
        if (!ip_append (IP, n, ip_size, forth_jz, last_error)) {
          return 0;
        }
        if (++CS->top >= MAX_COND_STACK_SIZE) {
          strcpy (last_error, "if/else/then stack overflow");
          return 0;
        }
        CS->x[CS->top].jz_addr_ip_idx = (*n); /* address will be updated when we reach else or then statement */
        CS->x[CS->top].was_else_statement = 0;
        if (!ip_append (IP, n, ip_size, forth_nope, last_error)) {
          return 0;
        }
      } else if (i == id_else) {
        if (CS->top < 0) {
          strcpy (last_error, "if/else/then stack underflow");
          return 0;
        }
        if (CS->x[CS->top].was_else_statement) {
          strcpy (last_error, "if else else");
          return 0;
        }
        if (!ip_append (IP, n, ip_size, forth_jmp, last_error)) {
          return 0;
        }
        CS->x[CS->top].jmp_addr_before_else = *n;
        if (!ip_append (IP, n, ip_size, forth_nope, last_error)) {
          return 0;
        }
        IP[CS->x[CS->top].jz_addr_ip_idx] = IP + (*n);
        CS->x[CS->top].was_else_statement = 1;
      } else if (i == id_then) {
        if (CS->top < 0) {
          strcpy (last_error, "if/else/then stack underflow");
          return 0;
        }
        if (CS->x[CS->top].was_else_statement) {
          IP[CS->x[CS->top].jmp_addr_before_else] = IP + (*n);
        } else {
          IP[CS->x[CS->top].jz_addr_ip_idx] = IP + (*n);
        }
        CS->top--;
      } else {
        assert (0);
      }
      return 1;
    }
    if (!ip_append (IP, n, ip_size, cmd, last_error)) {
      return 0;
    }
    return 1;
  }

  /* literal integer */
  if (!ip_append (IP, n, ip_size, lit_int, last_error)) {
    return 0;
  }

  if (!ip_append (IP, n, ip_size, &s[0], last_error)) {
    return 0;
  }

  return 1;
}

int lex (char *value, int value_len, void **IP, int ip_size, char last_error[MAX_ERROR_BUF_SIZE]) {
  int i, j, k = 0, n = 0;
  vkprintf (3, "lex (\"%.*s\", ip_size = %d\n", value_len, value, ip_size);
  struct forth_condition_stack cond_stack;
  cond_stack.top = -1;
  for (i = 0; ; k++) {
    while (i < value_len && isspace (value[i])) {
      i++;
    }
    if (i >= value_len) {
      break;
    }

    if (value[i] == '"') {
      //read string literal in double quot
      i++;
      j = i;
      while (i < value_len && value[i] != '"') {
        i++;
      }
      if (i >= value_len) {
        strcpy (last_error, "lex: unclosed double quot");
        return -1;
      }
      value[i] = 0;
      if (!add_lit_str (value + j, IP, &n, ip_size, last_error)) {
        return -1;
      }
      i++;
      continue;
    }

    j = i;
    while (j < value_len && !isspace (value[j])) {
      j++;
    }
    value[j] = 0;
    if (!parse_token (value + i, IP, &n, ip_size, &cond_stack, last_error)) {
      int l = strlen (last_error);
      int o = MAX_ERROR_BUF_SIZE - l - 1;
      if (o > 0) {
        memset (last_error + l, 0, o + 1);
        snprintf (last_error + l, o , "\nlex: couldn't parse %d-th token (%s)", k, value + i);
      }
      return -1;
    }
    i = j + 1;
  }

  if (!parse_token ("BYE", IP, &n, ip_size, &cond_stack, last_error)) {
    return -1;
  }

  if (cond_stack.top >= 0) {
    strcpy (last_error, "unclosed if statement");
    return -1;
  }

  return n;
}

int image_initialized = 0;

void image_list_info (void) {
  if (verbosity > 0) {
    ExceptionInfo exception;
    GetExceptionInfo (&exception);
    ListMagickInfo (stderr, &exception);
    if (exception.severity != UndefinedException) {
      CatchException (&exception);
    }
    DestroyExceptionInfo (&exception);
  }
}

void image_init (char *prog_name, long long max_load_image_area, long long memory_limit, long long map_limit, long long disk_limit, int threads_limit) {
  char *t[4];
  int k = 0;
  if (image_initialized) {
    return;
  }
  assert (sizeof (struct forth_output) == MAX_SHARED_MEMORY_SIZE);
  image_initialized = 1;

  char s[32];

  #define SETENV_TMP(x) assert(!setenv(t[k++] = x,s,1))
  if (memory_limit > 0) {
    assert (snprintf (s, sizeof (s), "%lldk", memory_limit >> 10) < sizeof (s));
    SETENV_TMP ("MAGICK_LIMIT_MEMORY");
  }

  if (map_limit > 0) {
    assert (snprintf (s, sizeof (s), "%lldk", map_limit >> 10) < sizeof (s));
    SETENV_TMP ("MAGICK_LIMIT_MAP");
  }

  if (disk_limit >= 0) {
    assert (snprintf (s, sizeof (s), "%lldk", disk_limit >> 10) < sizeof (s));
    SETENV_TMP ("MAGICK_LIMIT_DISK");
  }

  if (max_load_image_area > 0) {
    gm_max_load_image_area = max_load_image_area;
    assert (snprintf (s, sizeof (s), "%lld", max_load_image_area) < sizeof (s));
    SETENV_TMP ("MAGICK_LIMIT_PIXELS");
  }

  #undef SETENV_TMP

  InitializeMagick (prog_name);

  while (--k >= 0) {
    unsetenv (t[k]);
  }

  SetMagickResourceLimit (ThreadsResource, threads_limit);

  if (verbosity >= 3) {
    long long x = GetMagickResourceLimit (MemoryResource);
    kprintf ("GetMagickResourceLimit (MemoryResource): %lld\n", x);
    x = GetMagickResourceLimit (DiskResource);
    kprintf ("GetMagickResourceLimit (DiskResource): %lld\n", x);
    x = GetMagickResourceLimit (MapResource);
    kprintf ("GetMagickResourceLimit (MapResource): %lld\n", x);
    x = GetMagickResourceLimit (PixelsResource);
    kprintf ("GetMagickResourceLimit (PixelsResource): %lld\n", x);
    x = GetMagickResourceLimit (ThreadsResource);
    kprintf ("GetMagickResourceLimit (ThreadsResource): %lld\n", x);
  }

}

void image_done (void) {
  if (image_initialized) {
    DestroyMagick ();
    image_initialized = 0;
  }
}

double get_rusage_time (int who) {
  struct rusage r;
  if (getrusage (who, &r)) { return 0.0; }
  const double res = (double) r.ru_utime.tv_sec + (double) r.ru_stime.tv_sec + 1e-6 * ((double) r.ru_utime.tv_usec + (double) r.ru_stime.tv_usec);
  if (res > 1e9) {
    return 0.0;
  }
  return res;
}

#define MAX_PROG_SIZE 65536
int image_exec (long long prog_id, char *value, int value_len, int thread_id, int shm_descriptor) {
  vkprintf (2, "image_exec (%s, %d)\n", value, value_len);
  void *IP_BUF[MAX_PROG_SIZE];
  void **IP = IP_BUF;
  struct forth_stack st;
  st.error_len = 0;
  st.error[0] = 0;
  st.top = -1;
  st.thread_id = thread_id;
  st.O = NULL;
  if (shm_descriptor >= 0) {
    st.O = (struct forth_output *) mmap (NULL, MAX_SHARED_MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_descriptor, 0);
    if (st.O == MAP_FAILED) {
      kprintf ("image_exec, mmap failed. %m\n");
      st.O = NULL;
    }
  }
  if (st.O != NULL) {
    st.O->l = 0;
  }
  void *res = FORTH_FAIL;
  int n = lex (value, value_len, IP, MAX_PROG_SIZE, st.error);
  if (n < 0) {
    vkprintf (1, "[%d] lex_error: %s\n", thread_id, st.error);
  } else {
    res = (* (fpr_t *) IP)(IP + 1, &st);
  }
  if (st.top >= 0) {
    vkprintf (1, "[%d] stack contains %d values\n", thread_id, st.top + 1);
  }
  int i, x;
  if (st.O != NULL) {
    for (i = 0; i <= st.top; i++) {
      switch (st.x[i].tp) {
        case ft_int:
          memcpy (&x, &st.x[i].a, 4);
          append_int (st.O, x);
          break;
        case ft_str:
          append_str (st.O, (char *) st.x[i].a);
          break;
        case ft_image:
          append_str (st.O, (char *) "<IMAGE>");
          break;
      }
    }
  }
  free_stack (&st, 0, st.top);
  int r = EXIT_SUCCESS;
  if (res == FORTH_FAIL) {
    vkprintf (1, "[%d] exec_error: %s\n", thread_id, st.error);
    if (st.O != NULL) {
      append_str (st.O, st.error);
    }
    r = EXIT_FAILURE;
/*
    if (!strncmp (st.error, "read_image: bad area", 20)) {
      r = EXIT_BAD_AREA;
    }
*/
  }
  if (st.O != NULL) {
    //st.O->s[st.O->l] = 0;
    st.O->prog_id = prog_id;
    st.O->working_time = get_rusage_time (RUSAGE_SELF);
    munmap (st.O, MAX_SHARED_MEMORY_SIZE);
  }
  return r;
}

