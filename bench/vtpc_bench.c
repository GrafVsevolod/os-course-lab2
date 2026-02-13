#define _GNU_SOURCE

#include "../lib/vtpc.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#endif

static size_t page_size(void) {
  long ps = sysconf(_SC_PAGESIZE);
  return (ps > 0) ? (size_t)ps : 4096;
}

static uint64_t xorshift64(uint64_t *s) {
  uint64_t x = *s;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  *s = x;
  return x;
}

static double now_sec(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void die(const char *msg) {
  fprintf(stderr, "fatal: %s (errno=%d: %s)\n", msg, errno, strerror(errno));
  exit(2);
}

static int open_direct_fallback(const char *path, int flags, mode_t mode, int *is_direct) {
  int fd = open(path, flags | O_DIRECT, mode);
  if (fd >= 0) {
#ifdef __APPLE__
    (void)fcntl(fd, F_NOCACHE, 1);
#endif
    *is_direct = 1;
    return fd;
  }
  if (errno == EINVAL) {
    fd = open(path, flags, mode);
    if (fd >= 0) {
#ifdef __APPLE__
      (void)fcntl(fd, F_NOCACHE, 1);
#endif
      *is_direct = 0;
      return fd;
    }
  }
  return -1;
}

static void fill_file_if_needed(const char *path, size_t file_pages) {
  int is_direct = 0;
  int fd = open_direct_fallback(path, O_CREAT | O_RDWR, 0644, &is_direct);
  if (fd < 0) die("open for fill");

  struct stat st;
  if (fstat(fd, &st) != 0) die("fstat");

  size_t ps = page_size();
  off_t want = (off_t)(file_pages * ps);

  if (st.st_size >= want) {
    close(fd);
    return;
  }

  void *buf = NULL;
  if (posix_memalign(&buf, ps, ps) != 0) die("posix_memalign");
  memset(buf, 0xAB, ps);

  /* extend by writing pages */
  for (size_t i = (size_t)(st.st_size / (off_t)ps); i < file_pages; i++) {
    off_t off = (off_t)(i * ps);
    ssize_t w = pwrite(fd, buf, ps, off);
    if (w < 0) die("pwrite fill");
    if (w != (ssize_t)ps) die("short write fill");
  }
  if (ftruncate(fd, want) != 0) die("ftruncate fill");
  fsync(fd);

  free(buf);
  close(fd);
}

static void usage(const char *argv0) {
  fprintf(stderr,
    "Usage:\n"
    "  %s --mode=libc|vtpc --file=PATH --file-pages=N --ws-pages=N --ops=N [--seed=N]\n\n"
    "Notes:\n"
    "  - libc mode uses open+O_DIRECT (fallback if unsupported) + pread, no cache.\n"
    "  - vtpc mode uses vtpc_* (your 2Q cache).\n"
    "  - For cache size set env: VTPC_CACHE_PAGES (default 256).\n",
    argv0
  );
  exit(1);
}

int main(int argc, char **argv) {
  const char *mode = NULL;
  const char *path = NULL;
  size_t file_pages = 4096;
  size_t ws_pages = 256;
  size_t ops = 500000;
  uint64_t seed = 1;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--mode=", 7) == 0) mode = argv[i] + 7;
    else if (strncmp(argv[i], "--file=", 7) == 0) path = argv[i] + 7;
    else if (strncmp(argv[i], "--file-pages=", 13) == 0) file_pages = (size_t)strtoull(argv[i] + 13, NULL, 10);
    else if (strncmp(argv[i], "--ws-pages=", 11) == 0) ws_pages = (size_t)strtoull(argv[i] + 11, NULL, 10);
    else if (strncmp(argv[i], "--ops=", 6) == 0) ops = (size_t)strtoull(argv[i] + 6, NULL, 10);
    else if (strncmp(argv[i], "--seed=", 7) == 0) seed = (uint64_t)strtoull(argv[i] + 7, NULL, 10);
    else usage(argv[0]);
  }

  if (!mode || !path) usage(argv[0]);
  if (ws_pages == 0 || ops == 0 || file_pages == 0) usage(argv[0]);
  if (ws_pages > file_pages) ws_pages = file_pages;

  size_t ps = page_size();

  fill_file_if_needed(path, file_pages);

  void *buf = NULL;
  if (posix_memalign(&buf, ps, ps) != 0) die("posix_memalign buf");

  double t0 = now_sec();

  if (strcmp(mode, "libc") == 0) {
    int is_direct = 0;
    int fd = open_direct_fallback(path, O_RDONLY, 0, &is_direct);
    if (fd < 0) die("open libc");

    for (size_t i = 0; i < ops; i++) {
      uint64_t r = xorshift64(&seed);
      uint64_t page = (r % ws_pages);
      off_t off = (off_t)(page * ps);
      ssize_t n = pread(fd, buf, ps, off);
      if (n < 0) die("pread libc");
      if (n == 0) die("unexpected EOF");
    }

    close(fd);
  } else if (strcmp(mode, "vtpc") == 0) {
    int fd = vtpc_open(path, O_RDONLY, 0);
    if (fd < 0) die("vtpc_open");

    for (size_t i = 0; i < ops; i++) {
      uint64_t r = xorshift64(&seed);
      uint64_t page = (r % ws_pages);
      off_t off = (off_t)(page * ps);

      if (vtpc_lseek(fd, off, SEEK_SET) < 0) die("vtpc_lseek");
      ssize_t n = vtpc_read(fd, buf, ps);
      if (n < 0) die("vtpc_read");
      if ((size_t)n != ps) die("short vtpc_read");
    }

    vtpc_close(fd);
  } else {
    usage(argv[0]);
  }

  double t1 = now_sec();
  double dt = t1 - t0;

  double total_bytes = (double)ops * (double)ps;
  double mb = total_bytes / (1024.0 * 1024.0);
  double mbps = mb / dt;
  double ops_s = (double)ops / dt;

  printf("mode=%s file_pages=%zu ws_pages=%zu ops=%zu page_size=%zu\n", mode, file_pages, ws_pages, ops, ps);
  printf("time_sec=%.6f throughput_mib_s=%.2f ops_s=%.2f\n", dt, mbps, ops_s);

  free(buf);
  return 0;
}
