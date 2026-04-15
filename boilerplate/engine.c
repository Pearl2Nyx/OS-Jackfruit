/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;         /* set before sending SIGTERM via 'stop' */
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* forward declaration so pipe_reader_thread can be defined before
 * run_supervisor but still reference bounded_buffer_push */
static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);

/* pipe reader arg struct — defined here so it is visible to both
 * pipe_reader_thread and run_supervisor */
typedef struct {
    int fd;
    bounded_buffer_t *buf;
    char id[CONTAINER_ID_LEN];
} pipe_reader_arg_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* block while the buffer is full, unless we are shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    /* if shutdown started while we were waiting, discard the item */
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* insert at tail and advance */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* wake at least one consumer */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* wait while empty, unless shutdown has been requested */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* if the buffer is empty and we are shutting down, signal done */
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* remove from head and advance */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* wake at least one producer that may be blocked on not_full */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char path[PATH_MAX];
    int fd;

    /*
     * Keep popping until bounded_buffer_pop signals that the buffer
     * is empty AND shutting_down — at that point all pending data has
     * been drained and we can exit safely.
     */
    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        /* open in append mode so multiple writes accumulate correctly */
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open log file");
            continue;
        }
        write(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

/*
 * pipe_reader_thread - one per container, reads stdout/stderr from the
 * container pipe and pushes chunks into the shared bounded buffer.
 */
void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *pra = (pipe_reader_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, pra->id, CONTAINER_ID_LEN - 1);

    while ((n = read(pra->fd, item.data, LOG_CHUNK_SIZE)) > 0) {
        item.length = (size_t)n;
        bounded_buffer_push(pra->buf, &item);
    }

    close(pra->fd);
    free(pra);
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /*
     * Redirect stdout and stderr into the write end of the pipe that
     * the supervisor holds open for this container.  This is the
     * producer side of Path A (logging IPC).
     */
    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0)
            perror("dup2 stdout");
        if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
            perror("dup2 stderr");
        close(cfg->log_write_fd);
    }

    /* give the container its own hostname (UTS namespace) */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    /* chroot into the container's private rootfs copy */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    /*
     * Mount /proc so that tools like ps(1) and top(1) work inside the
     * container.  Non-fatal: some minimal rootfs images may not have
     * a /proc directory yet.
     */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0)
        perror("mount /proc (non-fatal)");

    /* apply scheduler priority if the caller requested it */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* exec the requested command — argv[0] is the command itself */
    execl("/bin/sh", "/bin/sh", "-c", cfg->command, NULL);

  /* execl only returns on error */
   perror("execl");
   return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* Global supervisor context pointer — used by signal handlers
 * which cannot receive arbitrary arguments.
 */
static supervisor_ctx_t *g_ctx = NULL;

/*
 * SIGCHLD handler — reap exited children and update metadata.
 * Uses WNOHANG so it never blocks.
 */
static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        if (!g_ctx)
            continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(wstatus)) {
                    rec->exit_code   = WEXITSTATUS(wstatus);
                    rec->exit_signal = 0;
                    rec->state       = CONTAINER_EXITED;
                } else if (WIFSIGNALED(wstatus)) {
                    rec->exit_signal = WTERMSIG(wstatus);
                    rec->exit_code   = 128 + rec->exit_signal;
                    /*
                     * Attribution rule (Task 4):
                     *   - if stop_requested is set, the operator asked for
                     *     termination → classify as STOPPED
                     *   - otherwise a SIGKILL from the kernel monitor
                     *     → classify as KILLED (hard_limit_killed)
                     */
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else
                        rec->state = CONTAINER_KILLED;
                }
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }

    errno = saved_errno;
}

/*
 * SIGINT / SIGTERM handler — set the should_stop flag so the main
 * event loop exits cleanly after the current iteration.
 */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    /* ensure the log directory exists */
    mkdir(LOG_DIR, 0755);

    /* 1) initialise synchronisation primitives */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 2) open /dev/container_monitor (optional — carry on if absent) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "[supervisor] Warning: cannot open /dev/container_monitor: %s\n",
                strerror(errno));

    /* 3) create and bind the UNIX-domain control socket (Path B) */
    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* 4) install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 5) start the logging consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    fprintf(stderr,
            "[supervisor] Ready. rootfs=%s control=%s\n",
            rootfs, CONTROL_PATH);

    /* 
     * 6) Main event loop — wait for CLI connections one at a time.
     *    select() with a 1-second timeout lets us check should_stop
     *    even when no clients connect.
     */
    while (!ctx.should_stop) {
        fd_set rfds;
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);

        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (sel == 0)
            continue; /* timeout — loop back and recheck should_stop */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;
        }

        /* read exactly one control_request_t from the CLI client */
        control_request_t req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

         /* Dispatch on command kind */

        if (req.kind == CMD_PS) {
            /* Build a human-readable table of container metadata and
             * return it in the response message field. */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *r = ctx.containers;
            int off = 0;
            off += snprintf(resp.message + off,
                            sizeof(resp.message) - off,
                            "%-16s %-8s %-10s %-12s %-12s\n",
                            "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)");
            while (r && off < (int)sizeof(resp.message) - 1) {
                off += snprintf(resp.message + off,
                                sizeof(resp.message) - off,
                                "%-16s %-8d %-10s %-12lu %-12lu\n",
                                r->id,
                                r->host_pid,
                                state_to_string(r->state),
                                r->soft_limit_bytes >> 20,
                                r->hard_limit_bytes >> 20);
                r = r->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            resp.status = 0;

        } else if (req.kind == CMD_LOGS) {
            /*
             * Open the per-container log file and stream its contents
             * back to the CLI client after the initial response struct.
             */
            char logpath[PATH_MAX];
            snprintf(logpath, sizeof(logpath), "%s/%s.log",
                     LOG_DIR, req.container_id);

            int lfd = open(logpath, O_RDONLY);
            if (lfd < 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "No log found for container '%s'",
                         req.container_id);
                write(client_fd, &resp, sizeof(resp));
            } else {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "Log: %s", logpath);
                write(client_fd, &resp, sizeof(resp));

                /* stream raw log bytes after the response header */
                char buf[512];
                ssize_t nr;
                while ((nr = read(lfd, buf, sizeof(buf))) > 0)
                    write(client_fd, buf, (size_t)nr);
                close(lfd);
            }
            close(client_fd);
            continue; /* already sent response above */

        } else if (req.kind == CMD_STOP) {
            /*
             * Set stop_requested on the record BEFORE sending SIGTERM
             * so that the SIGCHLD handler classifies the exit correctly
             * (STOPPED rather than KILLED).
             */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *r = ctx.containers;
            int found = 0;
            while (r) {
                if (strncmp(r->id, req.container_id, CONTAINER_ID_LEN) == 0) {
                    if (r->state == CONTAINER_RUNNING ||
                        r->state == CONTAINER_STARTING) {
                        r->stop_requested = 1;
                        r->state          = CONTAINER_STOPPED;
                        kill(r->host_pid, SIGTERM);
                    }
                    found = 1;
                    break;
                }
                r = r->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            resp.status = found ? 0 : -1;
            snprintf(resp.message, sizeof(resp.message),
                     found ? "Stopped '%s'" : "Container '%s' not found",
                     req.container_id);

        } else if (req.kind == CMD_START || req.kind == CMD_RUN) {
            /* Allocate a stack for clone(), create a pipe for container
             * stdout/stderr, build the child config, and clone a new
             * process with PID, UTS, and mount namespace isolation.*/

            char *stack = malloc(STACK_SIZE);
            if (!stack) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Out of memory allocating container stack");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            /* pipe: child writes (pipefd[1]), supervisor reads (pipefd[0]) */
            int pipefd[2];
            if (pipe(pipefd) < 0) {
                perror("pipe");
                free(stack);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message), "pipe() failed");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            /* child_config lives on the heap so clone child can read it
             * before exec.  Freed after clone() returns in the parent. */
            child_config_t *cfg = calloc(1, sizeof(*cfg));
            if (!cfg) {
                close(pipefd[0]);
                close(pipefd[1]);
                free(stack);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Out of memory allocating child config");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            strncpy(cfg->id,      req.container_id, CONTAINER_ID_LEN - 1);
            strncpy(cfg->rootfs,  req.rootfs,        PATH_MAX - 1);
            strncpy(cfg->command, req.command,        CHILD_COMMAND_LEN - 1);
            cfg->nice_value   = req.nice_value;
            cfg->log_write_fd = pipefd[1]; /* child writes here */

            /*
             * clone() with namespace flags:
             *   CLONE_NEWPID  — container gets PID 1 inside its namespace
             *   CLONE_NEWUTS  — container gets its own hostname
             *   CLONE_NEWNS   — container gets its own mount namespace
             *   SIGCHLD       — parent receives SIGCHLD when child exits
             */
            pid_t pid = clone(child_fn,
                              stack + STACK_SIZE,
                              CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                              cfg);

            /* parent no longer needs the write end of the pipe */
            close(pipefd[1]);

            if (pid < 0) {
                perror("clone");
                close(pipefd[0]);
                free(stack);
                free(cfg);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "clone() failed: %s", strerror(errno));
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            /* allocate and initialise container metadata record */
            container_record_t *rec = calloc(1, sizeof(*rec));
            if (!rec) {
                kill(pid, SIGKILL);
                close(pipefd[0]);
                free(stack);
                free(cfg);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Out of memory allocating container record");
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

            strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
            rec->host_pid          = pid;
            rec->started_at        = time(NULL);
            rec->state             = CONTAINER_RUNNING;
            rec->soft_limit_bytes  = req.soft_limit_bytes;
            rec->hard_limit_bytes  = req.hard_limit_bytes;
            rec->stop_requested    = 0;
            snprintf(rec->log_path, PATH_MAX, "%s/%s.log",
                     LOG_DIR, req.container_id);

            /* prepend to the linked list under the metadata lock */
            pthread_mutex_lock(&ctx.metadata_lock);
            rec->next        = ctx.containers;
            ctx.containers   = rec;
            pthread_mutex_unlock(&ctx.metadata_lock);

            /* register with the kernel monitor if the device is open */
            if (ctx.monitor_fd >= 0)
                register_with_monitor(ctx.monitor_fd,
                                      req.container_id, pid,
                                      req.soft_limit_bytes,
                                      req.hard_limit_bytes);

            /*
             * Spawn a producer thread that reads from the pipe and
             * pushes chunks into the shared bounded buffer (Path A).
             * The thread is detached — it frees pra and closes the fd
             * itself when the container's pipe reaches EOF.
             */
            pipe_reader_arg_t *pra = malloc(sizeof(*pra));
            if (pra) {
                pra->fd  = pipefd[0];
                pra->buf = &ctx.log_buffer;
                strncpy(pra->id, req.container_id, CONTAINER_ID_LEN - 1);
                pra->id[CONTAINER_ID_LEN - 1] = '\0';

                pthread_t reader;
                if (pthread_create(&reader, NULL, pipe_reader_thread, pra) == 0)
                    pthread_detach(reader);
                else {
                    close(pipefd[0]);
                    free(pra);
                }
            } else {
                close(pipefd[0]);
            }

            free(stack);
            free(cfg);

            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Started container '%s' pid=%d",
                     req.container_id, pid);

            if (req.kind == CMD_RUN) {
                /*
                 * 'run' — send an initial acknowledgement, then block
                 * until the container exits and report the final status.
                 */
                write(client_fd, &resp, sizeof(resp));

                int wstatus;
                waitpid(pid, &wstatus, 0);

                int exit_code = WIFEXITED(wstatus)
                                    ? WEXITSTATUS(wstatus)
                                    : 128 + WTERMSIG(wstatus);

                memset(&resp, 0, sizeof(resp));
                resp.status = exit_code;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' exited with code %d",
                         req.container_id, exit_code);

                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }

        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Unknown command kind %d", req.kind);
        }

        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }

    /*Orderly shutdown — stop containers, drain logs, free everything. */
    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* send SIGTERM to every still-running container */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *r = ctx.containers;
    while (r) {
        if (r->state == CONTAINER_RUNNING || r->state == CONTAINER_STARTING) {
            r->stop_requested = 1;
            r->state          = CONTAINER_STOPPED;
            kill(r->host_pid, SIGTERM);
        }
        r = r->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* give containers a moment to exit gracefully */
    sleep(1);

    /* drain and join the logger thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* unregister from the kernel monitor and free metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *cur = ctx.containers;
    while (cur) {
        if (ctx.monitor_fd >= 0)
            unregister_from_monitor(ctx.monitor_fd, cur->id, cur->host_pid);
        container_record_t *nxt = cur->next;
        free(cur);
        cur = nxt;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Done.\n");
    return 0;

cleanup:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    return 1;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    /* connect to the supervisor's UNIX-domain socket */
    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    /* send the request struct */
    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write control request");
        close(fd);
        return 1;
    }

    /*
     * Read the first response struct.  For CMD_LOGS the supervisor
     * streams raw log bytes after the struct, so we read those too.
     * For CMD_RUN the supervisor sends two response structs (ack then
     * final exit status), so we loop until the connection closes.
     */
    int got_response = 0;
    while ((n = read(fd, &resp, sizeof(resp))) == (ssize_t)sizeof(resp)) {
        printf("%s\n", resp.message);
        got_response = 1;

        if (req->kind == CMD_RUN) {
            /* loop: first struct is the ack, second is the exit status */
            continue;
        }
        break;
    }

    /* for CMD_LOGS: stream any remaining raw bytes to stdout */
    if (req->kind == CMD_LOGS) {
        char buf[512];
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, (size_t)n, stdout);
    }

    if (!got_response)
        fprintf(stderr, "No response from supervisor.\n");

    close(fd);
    return (resp.status < 0) ? 1 : 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}