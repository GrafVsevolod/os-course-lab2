#define _GNU_SOURCE

#include "vtpc.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __APPLE__
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#endif

#ifndef VTPC_MAX_HANDLES
#define VTPC_MAX_HANDLES 1024
#endif

#ifndef VTPC_DEFAULT_CACHE_PAGES
#define VTPC_DEFAULT_CACHE_PAGES 256
#endif



static size_t vtpc_page_size(void) {
  long ps = sysconf(_SC_PAGESIZE);
  if (ps <= 0) return 4096;
  return (size_t)ps;
}

static size_t next_pow2(size_t x) {
  size_t p = 1;
  while (p < x) p <<= 1;
  return p;
}

static uint64_t hash_u64(uint64_t x) {

  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

static size_t min_sz(size_t a, size_t b) { return (a < b) ? a : b; }
static size_t max_sz(size_t a, size_t b) { return (a > b) ? a : b; }


typedef struct {
  size_t cap;       
  uint64_t *keys;
  void **vals;
  uint8_t *state;  
} ht_t;

static int ht_init(ht_t *t, size_t cap_pow2) {
  t->cap = cap_pow2;
  t->keys = (uint64_t*)calloc(t->cap, sizeof(uint64_t));
  t->vals = (void**)calloc(t->cap, sizeof(void*));
  t->state = (uint8_t*)calloc(t->cap, sizeof(uint8_t));
  if (!t->keys || !t->vals || !t->state) {
    free(t->keys); free(t->vals); free(t->state);
    memset(t, 0, sizeof(*t));
    errno = ENOMEM;
    return -1;
  }
  return 0;
}

static void ht_destroy(ht_t *t) {
  free(t->keys);
  free(t->vals);
  free(t->state);
  memset(t, 0, sizeof(*t));
}

static void* ht_get(const ht_t *t, uint64_t key) {
  if (t->cap == 0) return NULL;
  size_t mask = t->cap - 1;
  size_t i = (size_t)(hash_u64(key) & mask);
  for (;;) {
    uint8_t st = t->state[i];
    if (st == 0) return NULL;
    if (st == 1 && t->keys[i] == key) return t->vals[i];
    i = (i + 1) & mask;
  }
}

static int ht_put(ht_t *t, uint64_t key, void *val) {
  size_t mask = t->cap - 1;
  size_t i = (size_t)(hash_u64(key) & mask);
  size_t first_tomb = (size_t)-1;

  for (;;) {
    uint8_t st = t->state[i];
    if (st == 0) {
      size_t idx = (first_tomb != (size_t)-1) ? first_tomb : i;
      t->state[idx] = 1;
      t->keys[idx] = key;
      t->vals[idx] = val;
      return 0;
    }
    if (st == 2 && first_tomb == (size_t)-1) first_tomb = i;
    if (st == 1 && t->keys[i] == key) {
      t->vals[i] = val;
      return 0;
    }
    i = (i + 1) & mask;
  }
}

static void ht_del(ht_t *t, uint64_t key) {
  size_t mask = t->cap - 1;
  size_t i = (size_t)(hash_u64(key) & mask);
  for (;;) {
    uint8_t st = t->state[i];
    if (st == 0) return;
    if (st == 1 && t->keys[i] == key) {
      t->state[i] = 2;
      t->vals[i] = NULL;
      return;
    }
    i = (i + 1) & mask;
  }
}


typedef enum {
  Q_A1IN = 1,
  Q_AM = 2
} page_queue_t;

typedef struct page_entry {
  uint64_t page_no;
  void *data;             
  size_t valid_len;      
  int dirty;              
  page_queue_t q;

  struct page_entry *prev;
  struct page_entry *next;
} page_entry_t;

typedef struct ghost_entry {
  uint64_t page_no;
  struct ghost_entry *prev;
  struct ghost_entry *next;
} ghost_entry_t;

typedef struct vtpc_cache {
  size_t page_size;

  size_t capacity;        
  size_t kin;             
  size_t kout;           
  size_t am_cap;          

  size_t a1in_sz;
  size_t am_sz;
  size_t a1out_sz;

  page_entry_t *a1in_head, *a1in_tail; 
  page_entry_t *am_head, *am_tail;     
  ghost_entry_t *a1out_head, *a1out_tail; 

  ht_t resident;        
  ht_t ghosts;            
} vtpc_cache_t;



typedef struct vtpc_handle {
  int used;
  int os_fd;
  int flags;             
  int direct;            
  off_t pos;
  off_t size;            

  vtpc_cache_t cache;
} vtpc_handle_t;

static vtpc_handle_t g_handles[VTPC_MAX_HANDLES];
static int g_inited = 0;
static size_t g_cfg_cache_pages = 0;

static void vtpc_init_once(void) {
  if (g_inited) return;
  g_inited = 1;

  const char *env = getenv("VTPC_CACHE_PAGES");
  if (env && *env) {
    char *end = NULL;
    long v = strtol(env, &end, 10);
    if (end != env && v > 0 && v < 10L * 1000L * 1000L) {
      g_cfg_cache_pages = (size_t)v;
    }
  }
  if (g_cfg_cache_pages == 0) g_cfg_cache_pages = VTPC_DEFAULT_CACHE_PAGES;

  memset(g_handles, 0, sizeof(g_handles));
}

static vtpc_handle_t* get_handle(int fd) {
  if (fd < 0 || fd >= VTPC_MAX_HANDLES) return NULL;
  if (!g_handles[fd].used) return NULL;
  return &g_handles[fd];
}

static int alloc_handle_slot(void) {

  for (int i = 3; i < VTPC_MAX_HANDLES; i++) {
    if (!g_handles[i].used) return i;
  }
  errno = EMFILE;
  return -1;
}



static void page_list_remove(page_entry_t **head, page_entry_t **tail, page_entry_t *p) {
  if (!p) return;
  if (p->prev) p->prev->next = p->next;
  if (p->next) p->next->prev = p->prev;
  if (*head == p) *head = p->next;
  if (*tail == p) *tail = p->prev;
  p->prev = p->next = NULL;
}

static void page_list_push_front(page_entry_t **head, page_entry_t **tail, page_entry_t *p) {
  p->prev = NULL;
  p->next = *head;
  if (*head) (*head)->prev = p;
  *head = p;
  if (!*tail) *tail = p;
}

static page_entry_t* page_list_pop_back(page_entry_t **head, page_entry_t **tail) {
  page_entry_t *p = *tail;
  if (!p) return NULL;
  page_list_remove(head, tail, p);
  return p;
}

static void ghost_list_remove(ghost_entry_t **head, ghost_entry_t **tail, ghost_entry_t *g) {
  if (!g) return;
  if (g->prev) g->prev->next = g->next;
  if (g->next) g->next->prev = g->prev;
  if (*head == g) *head = g->next;
  if (*tail == g) *tail = g->prev;
  g->prev = g->next = NULL;
}

static void ghost_list_push_front(ghost_entry_t **head, ghost_entry_t **tail, ghost_entry_t *g) {
  g->prev = NULL;
  g->next = *head;
  if (*head) (*head)->prev = g;
  *head = g;
  if (!*tail) *tail = g;
}

static ghost_entry_t* ghost_list_pop_back(ghost_entry_t **head, ghost_entry_t **tail) {
  ghost_entry_t *g = *tail;
  if (!g) return NULL;
  ghost_list_remove(head, tail, g);
  return g;
}



static int drop_os_cache(int fd, off_t offset, size_t len) {
#ifdef POSIX_FADV_DONTNEED
  /* ignore errors: it's advisory */
  (void)posix_fadvise(fd, offset, (off_t)len, POSIX_FADV_DONTNEED);
#else
  (void)fd; (void)offset; (void)len;
#endif
  return 0;
}

static ssize_t pread_fullpage(vtpc_handle_t *h, void *buf, size_t page_size, off_t off) {
  ssize_t r = pread(h->os_fd, buf, page_size, off);
  if (!h->direct && r >= 0) drop_os_cache(h->os_fd, off, page_size);
  return r;
}

static ssize_t pwrite_fullpage(vtpc_handle_t *h, const void *buf, size_t page_size, off_t off) {
  ssize_t w = pwrite(h->os_fd, buf, page_size, off);
  if (!h->direct && w >= 0) drop_os_cache(h->os_fd, off, page_size);
  return w;
}



static int cache_init(vtpc_cache_t *c, size_t page_size) {
  memset(c, 0, sizeof(*c));
  c->page_size = page_size;

  c->capacity = g_cfg_cache_pages;
  if (c->capacity < 4) c->capacity = 4;

  c->kin = c->capacity / 4;
  if (c->kin < 1) c->kin = 1;
  if (c->kin >= c->capacity) c->kin = c->capacity / 2;

  c->am_cap = c->capacity - c->kin;
  if (c->am_cap < 1) c->am_cap = 1;

  c->kout = c->capacity / 2;
  if (c->kout < 1) c->kout = 1;

  size_t resident_cap = next_pow2(c->capacity * 4);
  size_t ghosts_cap = next_pow2(c->kout * 4);

  if (ht_init(&c->resident, resident_cap) != 0) return -1;
  if (ht_init(&c->ghosts, ghosts_cap) != 0) {
    ht_destroy(&c->resident);
    return -1;
  }
  return 0;
}

static void cache_free_page(page_entry_t *p) {
  if (!p) return;
  free(p->data);
  free(p);
}

static void cache_free_ghost(ghost_entry_t *g) {
  free(g);
}

static int cache_flush_page(vtpc_handle_t *h, page_entry_t *p) {
  if (!p || !p->dirty) return 0;

  off_t off = (off_t)(p->page_no * (uint64_t)h->cache.page_size);
  ssize_t w = pwrite_fullpage(h, p->data, h->cache.page_size, off);
  if (w < 0) return -1;


  if (ftruncate(h->os_fd, h->size) != 0) return -1;

  p->dirty = 0;
  return 0;
}

static int cache_add_ghost(vtpc_cache_t *c, uint64_t page_no) {
  ghost_entry_t *existing = (ghost_entry_t*)ht_get(&c->ghosts, page_no);
  if (existing) {

    ghost_list_remove(&c->a1out_head, &c->a1out_tail, existing);
    ghost_list_push_front(&c->a1out_head, &c->a1out_tail, existing);
    return 0;
  }

  ghost_entry_t *g = (ghost_entry_t*)calloc(1, sizeof(*g));
  if (!g) { errno = ENOMEM; return -1; }
  g->page_no = page_no;

  ghost_list_push_front(&c->a1out_head, &c->a1out_tail, g);
  c->a1out_sz++;
  ht_put(&c->ghosts, page_no, g);

  /* trim A1out */
  while (c->a1out_sz > c->kout) {
    ghost_entry_t *old = ghost_list_pop_back(&c->a1out_head, &c->a1out_tail);
    if (!old) break;
    ht_del(&c->ghosts, old->page_no);
    c->a1out_sz--;
    cache_free_ghost(old);
  }
  return 0;
}

static int evict_from_a1in(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;
  page_entry_t *victim = page_list_pop_back(&c->a1in_head, &c->a1in_tail);
  if (!victim) return 0;

  c->a1in_sz--;
  ht_del(&c->resident, victim->page_no);

  if (cache_flush_page(h, victim) != 0) {

    page_list_push_front(&c->a1in_head, &c->a1in_tail, victim);
    c->a1in_sz++;
    ht_put(&c->resident, victim->page_no, victim);
    return -1;
  }


  if (cache_add_ghost(c, victim->page_no) != 0) {

  }

  cache_free_page(victim);
  return 0;
}

static int evict_from_am(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;
  page_entry_t *victim = page_list_pop_back(&c->am_head, &c->am_tail);
  if (!victim) return 0;

  c->am_sz--;
  ht_del(&c->resident, victim->page_no);

  if (cache_flush_page(h, victim) != 0) {

    page_list_push_front(&c->am_head, &c->am_tail, victim);
    c->am_sz++;
    ht_put(&c->resident, victim->page_no, victim);
    return -1;
  }

  cache_free_page(victim);
  return 0;
}

static int ensure_space_for_a1in(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;


  if (c->a1in_sz >= c->kin) {
    return evict_from_a1in(h);
  }

  while ((c->a1in_sz + c->am_sz) >= c->capacity) {

    if (c->am_sz > 0) {
      if (evict_from_am(h) != 0) return -1;
    } else {
      if (evict_from_a1in(h) != 0) return -1;
    }
  }
  return 0;
}

static int ensure_space_for_am(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;


  while (c->am_sz >= c->am_cap) {
    if (evict_from_am(h) != 0) return -1;
  }


  while ((c->a1in_sz + c->am_sz) >= c->capacity) {

    if (c->a1in_sz > 0) {
      if (evict_from_a1in(h) != 0) return -1;
    } else {
      if (evict_from_am(h) != 0) return -1;
    }
  }
  return 0;
}

static page_entry_t* load_page(vtpc_handle_t *h, uint64_t page_no) {
  vtpc_cache_t *c = &h->cache;

  page_entry_t *p = (page_entry_t*)calloc(1, sizeof(*p));
  if (!p) { errno = ENOMEM; return NULL; }

  p->page_no = page_no;
  p->dirty = 0;
  p->valid_len = 0;
  p->prev = p->next = NULL;

  void *buf = NULL;
  int rc = posix_memalign(&buf, c->page_size, c->page_size);
  if (rc != 0) {
    free(p);
    errno = ENOMEM;
    return NULL;
  }
  p->data = buf;

  off_t off = (off_t)(page_no * (uint64_t)c->page_size);
  ssize_t r = pread_fullpage(h, p->data, c->page_size, off);
  if (r < 0) {
    cache_free_page(p);
    return NULL;
  }
  p->valid_len = (size_t)r;
  if (p->valid_len < c->page_size) {
    memset((uint8_t*)p->data + p->valid_len, 0, c->page_size - p->valid_len);
  }
  return p;
}

static page_entry_t* cache_get(vtpc_handle_t *h, uint64_t page_no) {
  vtpc_cache_t *c = &h->cache;

  page_entry_t *p = (page_entry_t*)ht_get(&c->resident, page_no);
  if (p) {

    if (p->q == Q_A1IN) {

      page_list_remove(&c->a1in_head, &c->a1in_tail, p);
      c->a1in_sz--;

      if (ensure_space_for_am(h) != 0) return NULL;

      p->q = Q_AM;
      page_list_push_front(&c->am_head, &c->am_tail, p);
      c->am_sz++;
    } else {
      page_list_remove(&c->am_head, &c->am_tail, p);
      page_list_push_front(&c->am_head, &c->am_tail, p);
    }
    return p;
  }

  ghost_entry_t *g = (ghost_entry_t*)ht_get(&c->ghosts, page_no);
  if (g) {
    ghost_list_remove(&c->a1out_head, &c->a1out_tail, g);
    ht_del(&c->ghosts, page_no);
    c->a1out_sz--;
    cache_free_ghost(g);

    if (ensure_space_for_am(h) != 0) return NULL;

    p = load_page(h, page_no);
    if (!p) return NULL;

    p->q = Q_AM;
    page_list_push_front(&c->am_head, &c->am_tail, p);
    c->am_sz++;
    ht_put(&c->resident, page_no, p);
    return p;
  }

  if (ensure_space_for_a1in(h) != 0) return NULL;

  p = load_page(h, page_no);
  if (!p) return NULL;

  p->q = Q_A1IN;
  page_list_push_front(&c->a1in_head, &c->a1in_tail, p);
  c->a1in_sz++;
  ht_put(&c->resident, page_no, p);
  return p;
}

static int cache_flush_all(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;

  for (page_entry_t *p = c->a1in_head; p; p = p->next) {
    if (cache_flush_page(h, p) != 0) return -1;
  }
  for (page_entry_t *p = c->am_head; p; p = p->next) {
    if (cache_flush_page(h, p) != 0) return -1;
  }

  if (fsync(h->os_fd) != 0) return -1;
  if (ftruncate(h->os_fd, h->size) != 0) return -1;
  return 0;
}

static void cache_destroy(vtpc_handle_t *h) {
  vtpc_cache_t *c = &h->cache;

  page_entry_t *p = c->a1in_head;
  while (p) {
    page_entry_t *n = p->next;
    cache_free_page(p);
    p = n;
  }
  p = c->am_head;
  while (p) {
    page_entry_t *n = p->next;
    cache_free_page(p);
    p = n;
  }

  ghost_entry_t *g = c->a1out_head;
  while (g) {
    ghost_entry_t *n = g->next;
    cache_free_ghost(g);
    g = n;
  }

  ht_destroy(&c->resident);
  ht_destroy(&c->ghosts);
  memset(c, 0, sizeof(*c));
}


int vtpc_open(const char* path, int mode, int access) {
  vtpc_init_once();
  if (!path) { errno = EINVAL; return -1; }

  int slot = alloc_handle_slot();
  if (slot < 0) return -1;

  int flags = mode;
  int direct = 1;

  int fd = open(path, flags | O_DIRECT, access);
  if (fd < 0) {
    if (errno == EINVAL) {
      direct = 0;
      fd = open(path, flags, access);
    }
  }
  if (fd < 0) return -1;

  #ifdef __APPLE__
    /* macOS analog of O_DIRECT: avoid kernel buffer cache */
    (void)fcntl(fd, F_NOCACHE, 1);
  #endif

  struct stat st;
  if (fstat(fd, &st) != 0) {
    int e = errno;
    close(fd);
    errno = e;
    return -1;
  }

  vtpc_handle_t *h = &g_handles[slot];
  memset(h, 0, sizeof(*h));
  h->used = 1;
  h->os_fd = fd;
  h->flags = flags;
  h->direct = direct;
  h->pos = 0;
  h->size = st.st_size;

  if (cache_init(&h->cache, vtpc_page_size()) != 0) {
    int e = errno;
    close(fd);
    memset(h, 0, sizeof(*h));
    errno = e;
    return -1;
  }

  return slot;
}

int vtpc_close(int fd) {
  vtpc_handle_t *h = get_handle(fd);
  if (!h) { errno = EBADF; return -1; }

  int flush_rc = cache_flush_all(h);
  int flush_errno = errno;

  int rc = close(h->os_fd);
  int close_errno = errno;

  cache_destroy(h);
  memset(h, 0, sizeof(*h));

  if (flush_rc != 0) { errno = flush_errno; return -1; }
  if (rc != 0) { errno = close_errno; return -1; }
  return 0;
}

off_t vtpc_lseek(int fd, off_t offset, int whence) {
  vtpc_handle_t *h = get_handle(fd);
  if (!h) { errno = EBADF; return (off_t)-1; }

  off_t base = 0;
  if (whence == SEEK_SET) {
    base = 0;
  } else if (whence == SEEK_CUR) {
    base = h->pos;
  } else if (whence == SEEK_END) {
    base = h->size;
  } else {
    errno = EINVAL;
    return (off_t)-1;
  }

  off_t np = base + offset;
  if (np < 0) { errno = EINVAL; return (off_t)-1; }
  h->pos = np;
  return np;
}

ssize_t vtpc_read(int fd, void* buf, size_t count) {
  vtpc_handle_t *h = get_handle(fd);
  if (!h) { errno = EBADF; return -1; }
  if (!buf && count > 0) { errno = EINVAL; return -1; }

  if (count == 0) return 0;

  if ((h->flags & O_ACCMODE) == O_WRONLY) { errno = EBADF; return -1; }

  size_t ps = h->cache.page_size;
  size_t total = 0;

  while (total < count) {
    off_t cur = h->pos;
    uint64_t page_no = (uint64_t)(cur / (off_t)ps);
    size_t in_page = (size_t)(cur % (off_t)ps);

    size_t want = min_sz(count - total, ps - in_page);

    page_entry_t *p = cache_get(h, page_no);
    if (!p) {
      if (total > 0) return (ssize_t)total;
      return -1;
    }

    if (in_page >= p->valid_len) {
      /* EOF */
      break;
    }

    size_t avail = p->valid_len - in_page;
    size_t take = min_sz(want, avail);

    memcpy((uint8_t*)buf + total, (uint8_t*)p->data + in_page, take);

    total += take;
    h->pos += (off_t)take;

    if (take < want) {
      break;
    }
  }

  return (ssize_t)total;
}

ssize_t vtpc_write(int fd, const void* buf, size_t count) {
  vtpc_handle_t *h = get_handle(fd);
  if (!h) { errno = EBADF; return -1; }
  if (!buf && count > 0) { errno = EINVAL; return -1; }

  if (count == 0) return 0;

  int acc = (h->flags & O_ACCMODE);
  if (acc == O_RDONLY) { errno = EBADF; return -1; }

  size_t ps = h->cache.page_size;

  if (h->flags & O_APPEND) h->pos = h->size;

  size_t total = 0;

  while (total < count) {
    off_t cur = h->pos;
    uint64_t page_no = (uint64_t)(cur / (off_t)ps);
    size_t in_page = (size_t)(cur % (off_t)ps);

    size_t chunk = min_sz(count - total, ps - in_page);

    page_entry_t *p = cache_get(h, page_no);
    if (!p) {
      if (total > 0) return (ssize_t)total;
      return -1;
    }

    if (in_page > p->valid_len) {
      memset((uint8_t*)p->data + p->valid_len, 0, in_page - p->valid_len);
    }

    memcpy((uint8_t*)p->data + in_page, (const uint8_t*)buf + total, chunk);

    p->valid_len = max_sz(p->valid_len, in_page + chunk);
    p->dirty = 1;

    total += chunk;
    h->pos += (off_t)chunk;

    off_t new_end = h->pos;
    if (new_end > h->size) {
      h->size = new_end;
      
      if (ftruncate(h->os_fd, h->size) != 0) {
        if (total > 0) return (ssize_t)total;
        return -1;
      }
    }
  }

  return (ssize_t)total;
}

int vtpc_fsync(int fd) {
  vtpc_handle_t *h = get_handle(fd);
  if (!h) { errno = EBADF; return -1; }
  return cache_flush_all(h);
}
