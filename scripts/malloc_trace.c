/*
 * Minimal LD_PRELOAD shim that logs mallocs >= MALLOC_TRACE_THRESHOLD bytes
 * with a backtrace, plus frees of any tracked allocation.
 *
 * Build: gcc -O2 -fPIC -shared -o malloc_trace.so malloc_trace.c -ldl
 * Use:   MALLOC_TRACE_THRESHOLD=4194304 \
 *        MALLOC_TRACE_LOG=/tmp/malloc_trace.log \
 *        LD_PRELOAD=./malloc_trace.so python ...
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

static int log_fd = -1;
static size_t threshold = 4UL << 20;
static __thread int in_hook = 0;

static void *(*real_malloc)(size_t);
static void *(*real_calloc)(size_t, size_t);
static void *(*real_realloc)(void *, size_t);
static int (*real_posix_memalign)(void **, size_t, size_t);
static void *(*real_aligned_alloc)(size_t, size_t);
static void *(*real_memalign)(size_t, size_t);
static void (*real_free)(void *);

static char bootstrap_pool[1 << 16];
static size_t bootstrap_used = 0;
static pthread_mutex_t bootstrap_mu = PTHREAD_MUTEX_INITIALIZER;

static int is_bootstrap(void *p) {
    return (char *)p >= bootstrap_pool && (char *)p < bootstrap_pool + sizeof(bootstrap_pool);
}

static void *bootstrap_alloc(size_t size) {
    size = (size + 15) & ~(size_t)15;
    pthread_mutex_lock(&bootstrap_mu);
    if (bootstrap_used + size > sizeof(bootstrap_pool)) {
        pthread_mutex_unlock(&bootstrap_mu);
        return NULL;
    }
    void *p = bootstrap_pool + bootstrap_used;
    bootstrap_used += size;
    pthread_mutex_unlock(&bootstrap_mu);
    return p;
}

#define WATCH_BUCKETS 4096
static struct {
    void *ptr;
    size_t size;
} watch[WATCH_BUCKETS];
static pthread_mutex_t watch_mu = PTHREAD_MUTEX_INITIALIZER;

static size_t hash_ptr(void *p) {
    uintptr_t x = (uintptr_t)p;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    return x & (WATCH_BUCKETS - 1);
}

static void watch_add(void *p, size_t size) {
    if (!p)
        return;
    pthread_mutex_lock(&watch_mu);
    size_t h = hash_ptr(p);
    for (size_t i = 0; i < WATCH_BUCKETS; i++) {
        size_t k = (h + i) & (WATCH_BUCKETS - 1);
        if (watch[k].ptr == NULL) {
            watch[k].ptr = p;
            watch[k].size = size;
            break;
        }
    }
    pthread_mutex_unlock(&watch_mu);
}

static size_t watch_remove(void *p) {
    if (!p)
        return 0;
    size_t size = 0;
    pthread_mutex_lock(&watch_mu);
    size_t h = hash_ptr(p);
    for (size_t i = 0; i < WATCH_BUCKETS; i++) {
        size_t k = (h + i) & (WATCH_BUCKETS - 1);
        if (watch[k].ptr == p) {
            size = watch[k].size;
            watch[k].ptr = NULL;
            watch[k].size = 0;
            break;
        }
        if (watch[k].ptr == NULL)
            break;
    }
    pthread_mutex_unlock(&watch_mu);
    return size;
}

static __attribute__((constructor)) void init(void) {
    real_malloc = dlsym(RTLD_NEXT, "malloc");
    real_calloc = dlsym(RTLD_NEXT, "calloc");
    real_realloc = dlsym(RTLD_NEXT, "realloc");
    real_posix_memalign = dlsym(RTLD_NEXT, "posix_memalign");
    real_aligned_alloc = dlsym(RTLD_NEXT, "aligned_alloc");
    real_memalign = dlsym(RTLD_NEXT, "memalign");
    real_free = dlsym(RTLD_NEXT, "free");
    const char *path = getenv("MALLOC_TRACE_LOG");
    if (!path)
        path = "/tmp/malloc_trace.log";
    log_fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    const char *thr = getenv("MALLOC_TRACE_THRESHOLD");
    if (thr)
        threshold = strtoul(thr, NULL, 0);
    char banner[128];
    int n = snprintf(banner, sizeof(banner), "# malloc_trace.so loaded, threshold=%zu fd=%d\n", threshold, log_fd);
    if (log_fd >= 0)
        write(log_fd, banner, n);
    fprintf(stderr, "%s", banner);
}

static void log_big_alloc(char op, void *p, size_t size) {
    if (in_hook || log_fd < 0 || size < threshold)
        return;
    in_hook = 1;

    char header[256];
    int hlen = snprintf(header, sizeof(header), "%c size=%zu ptr=%p tid=%d\n", op, size, p, (int)gettid());
    write(log_fd, header, hlen);

    void *frames[32];
    int n = backtrace(frames, 32);
    for (int i = 0; i < n; i++) {
        char line[64];
        int llen = snprintf(line, sizeof(line), "  %p\n", frames[i]);
        write(log_fd, line, llen);
    }
    watch_add(p, size);
    in_hook = 0;
}

static void log_free(void *p) {
    if (in_hook || log_fd < 0 || !p)
        return;
    size_t size = watch_remove(p);
    if (size == 0)
        return;
    in_hook = 1;
    char buf[128];
    int len = snprintf(buf, sizeof(buf), "F size=%zu ptr=%p tid=%d\n", size, p, (int)gettid());
    write(log_fd, buf, len);
    in_hook = 0;
}

void *malloc(size_t size) {
    if (!real_malloc)
        return bootstrap_alloc(size);
    void *p = real_malloc(size);
    log_big_alloc('M', p, size);
    return p;
}

void *calloc(size_t nmemb, size_t size) {
    if (!real_calloc) {
        void *p = bootstrap_alloc(nmemb * size);
        if (p)
            memset(p, 0, nmemb * size);
        return p;
    }
    void *p = real_calloc(nmemb, size);
    log_big_alloc('C', p, nmemb * size);
    return p;
}

void *realloc(void *ptr, size_t size) {
    if (!real_realloc) {
        void *p = bootstrap_alloc(size);
        if (p && ptr)
            memcpy(p, ptr, size);
        return p;
    }
    if (is_bootstrap(ptr)) {
        void *p = real_malloc(size);
        if (p && ptr)
            memcpy(p, ptr, size);
        log_big_alloc('R', p, size);
        return p;
    }
    if (ptr)
        log_free(ptr);
    void *p = real_realloc(ptr, size);
    log_big_alloc('R', p, size);
    return p;
}

int posix_memalign(void **memptr, size_t align, size_t size) {
    if (!real_posix_memalign) {
        *memptr = bootstrap_alloc(size);
        return *memptr ? 0 : 12;
    }
    int rc = real_posix_memalign(memptr, align, size);
    if (rc == 0)
        log_big_alloc('P', *memptr, size);
    return rc;
}

void *aligned_alloc(size_t align, size_t size) {
    if (!real_aligned_alloc)
        return bootstrap_alloc(size);
    void *p = real_aligned_alloc(align, size);
    log_big_alloc('A', p, size);
    return p;
}

void *memalign(size_t align, size_t size) {
    if (!real_memalign)
        return bootstrap_alloc(size);
    void *p = real_memalign(align, size);
    log_big_alloc('A', p, size);
    return p;
}

void free(void *ptr) {
    if (!ptr)
        return;
    if (is_bootstrap(ptr))
        return;
    if (!real_free)
        return;
    log_free(ptr);
    real_free(ptr);
}
