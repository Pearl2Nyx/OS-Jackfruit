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
    int stop_requested;
    int log_read_fd;
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
    int exit_code;   
    int exit_signal; 
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

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd;
} producer_arg_t;

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

/*TODO 1*/
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count==LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail=(buffer->tail+1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/* TODO 2 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/* TODO 3*/
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    mkdir(LOG_DIR, 0755);

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

/* Per-container producer thread*/
 
/* producer_thread — one per running container. */

static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    supervisor_ctx_t *ctx = parg->ctx;
    int fd = parg->pipe_fd;
    char id[CONTAINER_ID_LEN];
    strncpy(id, parg->container_id, CONTAINER_ID_LEN - 1);
    id[CONTAINER_ID_LEN - 1] = '\0';
    free(parg);

    while (1) {
    log_item_t item;
    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, id, CONTAINER_ID_LEN - 1);

    ssize_t n = read(fd, item.data, LOG_CHUNK_SIZE);
    if (n <= 0)
        break;  

    item.length = (size_t)n;

    if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
        break; 
    }

    close(fd);
    return NULL;
}


/* TODO 4*/
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    sethostname(cfg->id, strlen(cfg->id));

    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    chdir("/");

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount");
        return 1;
    }

    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    execlp(cfg->command, cfg->command, NULL);

    perror("exec");
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

static volatile sig_atomic_t g_stop = 0;

static void handle_sigint(int sig)
{
    (void)sig;
    g_stop = 1;
}

static void handle_sigchld(int sig)
{
    (void)sig;

    // Reap all dead children
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

/* Supervisor helper: launch one container*/
 
/*supervisor launch container: Called from the supervisor event loop when a CMD_START or CMD_RUN request arrives.
  Returns the new container_record pointer on success, NULL on failure. */

static container_record_t *supervisor_launch_container(supervisor_ctx_t *ctx,
                                                       const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return NULL;
    }
 
    /* Allocate clone stack (grows downward) */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }
 
    child_config_t *cfg = malloc(sizeof(*cfg));
    if (!cfg) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];
 
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t child_pid = clone(child_fn,
                            stack + STACK_SIZE,   /* top of stack */
                            flags,
                            cfg);
 
    /* Parent no longer needs the write end */
    close(pipefd[1]);
    free(cfg);        /* child_fn has already read what it needs */
    free(stack);
 
    if (child_pid < 0) {
        perror("clone");
        close(pipefd[0]);
        return NULL;
    }
 
    /* Build metadata record */
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) {
        kill(child_pid, SIGKILL);
        close(pipefd[0]);
        return NULL;
    }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid          = child_pid;
    rec->started_at        = time(NULL);
    rec->state             = CONTAINER_RUNNING;
    rec->soft_limit_bytes  = req->soft_limit_bytes;
    rec->hard_limit_bytes  = req->hard_limit_bytes;
    rec->log_read_fd       = pipefd[0];
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);
 
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd,
                              req->container_id,
                              child_pid,
                              req->soft_limit_bytes,
                              req->hard_limit_bytes);
 
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);
 
    producer_arg_t *parg = malloc(sizeof(*parg));
    if (parg) {
        parg->ctx = ctx;
        strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        parg->pipe_fd = pipefd[0];
        pthread_t tid;
        if (pthread_create(&tid, NULL, producer_thread, parg) != 0) {
            free(parg);
            close(pipefd[0]);
        } else {
            pthread_detach(tid);
        }
    }
 
    return rec;
}
 
/* Supervisor event loop helpers*/
 
/* Find a container record by id (caller must hold metadata_lock). */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *r = ctx->containers;
    while (r) {
        if (strncmp(r->id, id, CONTAINER_ID_LEN) == 0)
            return r;
        r = r->next;
    }
    return NULL;
}
 
/* Reap any exited children and update their metadata records. Called after SIGCHLD or periodically inside the event loop. */
static void reap_children(supervisor_ctx_t *ctx)
{
    int wstatus;
    pid_t pid;
 
    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r) {
            if (r->host_pid == pid) {
                if (WIFEXITED(wstatus)) {
                    r->exit_code = WEXITSTATUS(wstatus);
                    r->state     = CONTAINER_EXITED;
                } else if (WIFSIGNALED(wstatus)) {
                    r->exit_signal = WTERMSIG(wstatus);
                    if (r->stop_requested)
                        r->state = CONTAINER_STOPPED;
                    else if (r->exit_signal == SIGKILL)
                        r->state = CONTAINER_KILLED; /* hard-limit kill */
                    else
                        r->state = CONTAINER_EXITED;
                }
                /* Unregister from kernel monitor */
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, r->id, pid);
                break;
            }
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}
 
/* Handle one accepted client connection; returns 0 to continue, 1 to stop. */
static int handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
 
    memset(&resp, 0, sizeof(resp));
 
    ssize_t n = read(client_fd, &req, sizeof(req));
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "malformed request");
        write(client_fd, &resp, sizeof(resp));
        return 0;
    }
 
    switch (req.kind) {
 
    case CMD_START: {
        container_record_t *rec = supervisor_launch_container(ctx, &req);
        if (rec) {
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "started container %s (pid %d)", rec->id, (int)rec->host_pid);
        } else {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "failed to start container %s", req.container_id);
        }
        write(client_fd, &resp, sizeof(resp));
        break;
    }

    case CMD_RUN: {
    container_record_t *rec = supervisor_launch_container(ctx, &req);
    if (!rec) {
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "failed to start container %s", req.container_id);
        write(client_fd, &resp, sizeof(resp));
        break;
    } else {
        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "running container %s (pid %d)", rec->id, (int)rec->host_pid);
        write(client_fd, &resp, sizeof(resp));
         
    }
    write(client_fd, &resp, sizeof(resp));
    break;
}
 
    case CMD_PS: {
        char buf[4096];
        int  off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-12s %s\n",
                        "ID", "PID", "STATE", "STARTED");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r && off < (int)sizeof(buf) - 1) {
            char tstr[32];
            struct tm *tm_info = localtime(&r->started_at);
            strftime(tstr, sizeof(tstr), "%H:%M:%S", tm_info);
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-12s %s\n",
                            r->id, (int)r->host_pid,
                            state_to_string(r->state), tstr);
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "%s", buf);
        write(client_fd, &resp, sizeof(resp));
        break;
    }
 
    case CMD_LOGS: {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);
        int lfd = open(path, O_RDONLY);
        if (lfd < 0) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "no log file for container %s", req.container_id);
            write(client_fd, &resp, sizeof(resp));
        } else {
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "log for container %s:", req.container_id);
            write(client_fd, &resp, sizeof(resp));
            /* Stream log file back to client */
            char chunk[4096];
            ssize_t nr;
            while ((nr = read(lfd, chunk, sizeof(chunk))) > 0)
                write(client_fd, chunk, nr);
            close(lfd);
        }
        break;
    }
 
    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = find_container(ctx, req.container_id);
        if (r && (r->state == CONTAINER_RUNNING || r->state == CONTAINER_STARTING)) {
            r->stop_requested = 1;
            kill(r->host_pid, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "sent SIGTERM to container %s (pid %d)",
                     r->id, (int)r->host_pid);
        } else if (r) {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "container %s is not running (state: %s)",
                     r->id, state_to_string(r->state));
        } else {
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "container %s not found", req.container_id);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        write(client_fd, &resp, sizeof(resp));
        break;
    }
 
    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "unknown command %d", req.kind);
        write(client_fd, &resp, sizeof(resp));
        break;
    }
 
    return 0;
}

/* Supervisor: TODO 5*/ 
/* Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
 
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
 
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

        /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */
 
    /* 1. Open kernel monitor device (optional: skip if not loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: could not open /dev/container_monitor (%s). "
                        "Memory monitoring disabled.\n", strerror(errno));
 
    /* 2. Create control UNIX domain socket */
    unlink(CONTROL_PATH);   /* remove stale socket */
 
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }
 
    struct sockaddr_un addr;
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
 
    /* Make the socket non-blocking so accept() doesn't deadlock shutdown */
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);
 
    /* 3. Install signal handlers */
    struct sigaction sa_chld, sa_int;
 
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = handle_sigchld;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);
 
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = handle_sigint;
    sigaction(SIGINT,  &sa_int, NULL);
    sigaction(SIGTERM, &sa_int, NULL);
 
    /* 4. Spawn logger (consumer) thread */
    mkdir(LOG_DIR, 0755);
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }
 
    fprintf(stdout, "Supervisor ready (rootfs=%s, socket=%s)\n", rootfs, CONTROL_PATH);
    fflush(stdout);
 
    /* 5. Event loop */
    while (!g_stop && !ctx.should_stop) {
        /* Reap any children that have already exited */
        reap_children(&ctx);
 
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* No pending connection — brief sleep to avoid busy-spin */
                struct timespec ts = { 0, 10 * 1000 * 1000 }; /* 10 ms */
                nanosleep(&ts, NULL);
                continue;
            }
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }
 
        handle_client(&ctx, client_fd);
        close(client_fd);
    }
 
    fprintf(stdout, "Supervisor shutting down…\n");
    fflush(stdout);
 
    /* 6. Graceful shutdown */
 
    /* Signal all running containers to stop */
    pthread_mutex_lock(&ctx.metadata_lock);
    for (container_record_t *r = ctx.containers; r; r = r->next) {
        if (r->state == CONTAINER_RUNNING || r->state == CONTAINER_STARTING) {
            r->stop_requested = 1;
            kill(r->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
 
    /* Wait briefly for containers to exit, then reap */
    struct timespec ts = { 0, 200 * 1000 * 1000 }; /* 200 ms */
    nanosleep(&ts, NULL);
    reap_children(&ctx);
 
    /* Shutdown the log pipeline and join the consumer thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
 
cleanup:
    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
 
    /* Free container metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *r = ctx.containers;
    while (r) {
        container_record_t *next = r->next;
        free(r);
        r = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
 
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
 
    return 0;
}

/* TODO: 6*/
static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sock);
        return 1;
    }
 
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    if (read(sock, &resp, sizeof(resp)) < (ssize_t)sizeof(resp)) {
        perror("read");
        close(sock);
        return 1;
    }
 
    printf("%s\n", resp.message);

        // For CMD_LOGS: drain any streamed log data
    if (req->kind == CMD_LOGS && resp.status == 0) {
        char buf[4096];
        ssize_t n;
        while ((n = read(sock, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, n, stdout);
    }

    close(sock);
    return resp.status;
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
