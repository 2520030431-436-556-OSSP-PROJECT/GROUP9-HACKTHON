/*
 * vm_pipeline.c  --  Virtual Memory Page Replacement Visualizer
 *
 * Integrated Operating Systems pipeline (ONE SINGLE C FILE):
 *
 *   Workload Generation -> Process Creation -> Named-FIFO IPC ->
 *   Round Robin Scheduling Simulation -> Page Replacement Analysis -> Report
 *
 * REAL Linux facilities used : fork, waitpid, execlp (debug mode), mkfifo,
 *                              open, read, write, close, unlink, pipe, poll,
 *                              kill, signal handling, mkdir, stat, fopen...
 * SIMULATED in user space    : Round Robin CPU scheduling, virtual memory
 *                              (pages / frames / page faults).
 *                              Nothing here changes the real kernel scheduler
 *                              or real page tables.
 *
 * Build : gcc -Wall -Wextra -std=c11 vm_pipeline.c -o vm_pipeline
 * Run   : ./vm_pipeline            (interactive menu)
 *         ./vm_pipeline -h         (command-line options)
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */
#define FIFO_PATH         "/tmp/vm_pipeline_fifo"
#define REPORT_DIR        "output"
#define REPORT_PATH       "output/report.txt"

#define MAX_PROCS         8      /* simulated processes                */
#define MAX_REFS_PER_PROC 64     /* page references per process        */
#define MAX_STREAM        256    /* total references in scheduled run  */
#define MAX_FRAMES        10     /* physical page frames               */
#define MAX_PAGE          99     /* valid page numbers: 0..99          */
#define PAGE_SPAN         100    /* key = pid * PAGE_SPAN + page       */
#define DEFAULT_FRAMES    3
#define DEFAULT_QUANTUM   2
#define MAX_QUANTUM       64
#define NUM_ALGOS         3
#define NUM_PRESETS       4

enum { ALGO_FIFO = 0, ALGO_LRU = 1, ALGO_OPT = 2 };
static const char *ALGO_NAMES[NUM_ALGOS] = { "FIFO", "LRU", "Optimal" };

/* Predefined workloads: ';' separates processes, spaces/commas separate pages */
static const char *PRESET_TEXT[NUM_PRESETS] = {
    "1 2 3 1 4 2;2 3 4 2 5 1;1 4 2 5 3 2",
    "1 1 2 1 2 2;5 5 5 6 5 6",
    "7 0 1 2 0 3 0 4 2 3 0 3 2 1 2 0 1 7 0 1",
    "1 2 3 4 1 2 5 1 2 3 4 5"
};
static const char *PRESET_NAME[NUM_PRESETS] = {
    "Predefined 1: three processes (example from the project brief)",
    "Predefined 2: two processes with repeated pages (many hits)",
    "Predefined 3: single process, textbook string (heavy replacement)",
    "Predefined 4: single process, Belady's anomaly string"
};

/* ------------------------------------------------------------------ */
/* Data structures                                                     */
/* ------------------------------------------------------------------ */

/* Message sent through the named FIFO (fixed size => easy framing).   */
typedef struct {
    int pid;    /* 1..MAX_PROCS ; -1 marks END-OF-STREAM                */
    int page;   /* 0..MAX_PAGE  ; -1 for the end marker                 */
} Request;

typedef struct {
    int nproc;
    int count[MAX_PROCS + 1];
    int pages[MAX_PROCS + 1][MAX_REFS_PER_PROC];
} Workload;

typedef struct {
    int frames;
    int quantum;
    int workload_id;     /* 0 = custom, 1..NUM_PRESETS = predefined      */
    char custom[512];
    int pause;           /* optional debug pause (seconds)               */
    int debug;           /* run "ls -l" on the FIFO via fork+exec        */
    int show_sched;      /* print scheduling trace during the run        */
    int show_repl;       /* print page replacement traces during the run */
} Config;

typedef struct {
    int time;       /* simulated clock tick (1-based)                    */
    int pid;
    int page;
    int turn_pos;   /* position inside the current turn (1-based)        */
    int end_reason; /* 0 = turn continues, 1 = quantum expired,
                       2 = process finished                              */
    int next_pid;   /* process chosen next (0 = nobody left)             */
} SchedEvent;

/* Sent from the parent to each analysis worker through an anonymous pipe */
typedef struct {
    int algo;
    int nframes;
    int n;
    int keys[MAX_STREAM];
} WorkOrder;

/* Sent back from each worker */
typedef struct {
    char name[16];
    int total, hits, faults;
    int event[MAX_STREAM];                 /* 0 hit, 1 fault(free), 2 fault(replace) */
    int victim[MAX_STREAM];                /* evicted key or -1                       */
    int frames[MAX_STREAM][MAX_FRAMES];    /* snapshot after each reference           */
} AlgoResult;

typedef struct {
    int valid;
    int frames;
    int quantum;
    char wname[96];
    Workload wl;                  /* what the parent RECEIVED via the FIFO */
    Request stream[MAX_STREAM];   /* scheduled (Round Robin) stream        */
    int nstream;
    SchedEvent ev[MAX_STREAM];
    AlgoResult res[NUM_ALGOS];
} PipelineState;

static PipelineState g_state;
static volatile sig_atomic_t g_fifo_exists = 0;

/* ------------------------------------------------------------------ */
/* Small helpers                                                       */
/* ------------------------------------------------------------------ */

static double pct(int part, int whole)
{
    return (whole == 0) ? 0.0 : (100.0 * (double)part / (double)whole);
}

static int parse_int(const char *s, int lo, int hi, int *out)
{
    char *end = NULL;
    long v;
    errno = 0;
    v = strtol(s, &end, 10);
    if (end == s || errno != 0) return -1;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') return -1;
    if (v < lo || v > hi) return -1;
    *out = (int)v;
    return 0;
}

static void key_str(char *buf, size_t n, int key)
{
    if (key < 0) snprintf(buf, n, "-");
    else         snprintf(buf, n, "P%d:%d", key / PAGE_SPAN, key % PAGE_SPAN);
}

/* write() may write less than requested: loop until everything is sent */
static int write_full(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

/* read() may return less than requested: loop until the full size arrives.
 * Returns 0 on success, -1 on error or premature EOF. */
static int read_full(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static void banner(const char *text)
{
    printf("\n==================================================================\n");
    printf(" %s\n", text);
    printf("==================================================================\n");
}

/* ------------------------------------------------------------------ */
/* Stage 1: workload parsing and generation                            */
/* ------------------------------------------------------------------ */

/* Format: "1 2 3 1;2 3 4;..."  ';' separates processes, ' ' or ',' pages */
static int parse_workload_string(const char *text, Workload *w, char *err, size_t en)
{
    char copy[1024];
    char *save1 = NULL, *seg;
    int total = 0;

    if (strlen(text) >= sizeof copy) {
        snprintf(err, en, "workload text is too long");
        return -1;
    }
    snprintf(copy, sizeof copy, "%s", text);
    memset(w, 0, sizeof *w);

    for (seg = strtok_r(copy, ";", &save1); seg; seg = strtok_r(NULL, ";", &save1)) {
        char *save2 = NULL, *tok;
        int pid;
        if (w->nproc >= MAX_PROCS) {
            snprintf(err, en, "too many processes (maximum %d)", MAX_PROCS);
            return -1;
        }
        pid = ++w->nproc;
        for (tok = strtok_r(seg, " ,\t\r\n", &save2); tok;
             tok = strtok_r(NULL, " ,\t\r\n", &save2)) {
            int v;
            if (parse_int(tok, 0, MAX_PAGE, &v) != 0) {
                snprintf(err, en, "invalid page '%s' (must be an integer 0..%d)", tok, MAX_PAGE);
                return -1;
            }
            if (w->count[pid] >= MAX_REFS_PER_PROC) {
                snprintf(err, en, "process P%d has more than %d references", pid, MAX_REFS_PER_PROC);
                return -1;
            }
            w->pages[pid][w->count[pid]++] = v;
            total++;
        }
        if (w->count[pid] == 0) {
            snprintf(err, en, "process P%d has no page references", pid);
            return -1;
        }
    }
    if (w->nproc == 0) {
        snprintf(err, en, "workload is empty");
        return -1;
    }
    if (total > MAX_STREAM) {
        snprintf(err, en, "too many references in total (maximum %d)", MAX_STREAM);
        return -1;
    }
    return 0;
}

static int build_workload(const Config *cfg, Workload *w, char *name, size_t nn,
                          char *err, size_t en)
{
    if (cfg->workload_id >= 1 && cfg->workload_id <= NUM_PRESETS) {
        snprintf(name, nn, "%s", PRESET_NAME[cfg->workload_id - 1]);
        return parse_workload_string(PRESET_TEXT[cfg->workload_id - 1], w, err, en);
    }
    snprintf(name, nn, "Custom workload entered by the user");
    return parse_workload_string(cfg->custom, w, err, en);
}

static void print_workload(FILE *fp, const Workload *w)
{
    int p, i;
    for (p = 1; p <= w->nproc; p++) {
        fprintf(fp, "  P%d:", p);
        for (i = 0; i < w->count[p]; i++) fprintf(fp, " %d", w->pages[p][i]);
        fprintf(fp, "\n");
    }
}

static int total_refs(const Workload *w)
{
    int p, t = 0;
    for (p = 1; p <= w->nproc; p++) t += w->count[p];
    return t;
}

/* ------------------------------------------------------------------ */
/* Stage 3: named FIFO management and request transfer                 */
/* ------------------------------------------------------------------ */

static void on_signal(int sig)
{
    (void)sig;
    if (g_fifo_exists) unlink(FIFO_PATH);   /* unlink is async-signal-safe */
    _exit(130);
}

static int create_fifo(void)
{
    struct stat st;
    if (lstat(FIFO_PATH, &st) == 0) {
        if (!S_ISFIFO(st.st_mode)) {
            fprintf(stderr, "Error: %s exists and is NOT a FIFO. Refusing to delete it.\n", FIFO_PATH);
            return -1;
        }
        printf("  (an old FIFO was found at %s; removing the stale one)\n", FIFO_PATH);
        if (unlink(FIFO_PATH) != 0) { perror("unlink"); return -1; }
    } else if (errno != ENOENT) {
        perror("lstat");
        return -1;
    }
    if (mkfifo(FIFO_PATH, 0600) != 0) { perror("mkfifo"); return -1; }
    g_fifo_exists = 1;
    return 0;
}

static void destroy_fifo(void)
{
    if (g_fifo_exists) {
        if (unlink(FIFO_PATH) != 0 && errno != ENOENT) perror("unlink");
        g_fifo_exists = 0;
    }
}

/* Optional: shows genuine exec() use. Runs "ls -l <fifo>" in a child. */
static void show_fifo_with_exec(void)
{
    pid_t c;
    int st;
    fflush(stdout);
    c = fork();
    if (c < 0) { perror("fork"); return; }
    if (c == 0) {
        execlp("ls", "ls", "-l", FIFO_PATH, (char *)NULL);
        _exit(127);
    }
    if (waitpid(c, &st, 0) < 0) perror("waitpid");
}

/* Child #1: the WORKLOAD PRODUCER. Sends every request through the FIFO. */
static int producer_main(const Workload *w, int pause_s)
{
    int fd, p, i, sent = 0;
    Request r;

    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    if (pause_s > 0) sleep((unsigned)pause_s);

    fd = open(FIFO_PATH, O_WRONLY);   /* reader already exists => no deadlock */
    if (fd < 0) { perror("producer: open FIFO"); return 1; }

    for (p = 1; p <= w->nproc; p++) {
        for (i = 0; i < w->count[p]; i++) {
            r.pid = p;
            r.page = w->pages[p][i];
            if (write_full(fd, &r, sizeof r) != 0) {
                perror("producer: write");
                close(fd);
                return 1;
            }
            sent++;
        }
    }
    r.pid = -1; r.page = -1;                  /* end-of-stream marker */
    if (write_full(fd, &r, sizeof r) != 0) {
        perror("producer: write end marker");
        close(fd);
        return 1;
    }
    close(fd);
    printf("  [producer  pid=%d] sent %d requests + end marker through the FIFO\n",
           (int)getpid(), sent);
    fflush(stdout);
    return 0;
}

/* Parent: receive requests. The read end is non-blocking and guarded by
 * poll() with a timeout, so a crashed producer can never hang the program. */
static int receive_requests(int fd, pid_t prod, Workload *w, int *reaped, int *status,
                            char *err, size_t en)
{
    Request r;
    size_t have = 0;
    int got = 0;

    memset(w, 0, sizeof *w);
    for (;;) {
        struct pollfd pfd;
        int pr;
        ssize_t n;

        pfd.fd = fd; pfd.events = POLLIN; pfd.revents = 0;
        pr = poll(&pfd, 1, 200);
        if (pr < 0) {
            if (errno == EINTR) continue;
            snprintf(err, en, "poll failed: %s", strerror(errno));
            return -1;
        }
        if (pr == 0) {   /* timeout: is the producer still alive? */
            pid_t x = waitpid(prod, status, WNOHANG);
            if (x == prod) {
                *reaped = 1;
                snprintf(err, en, "producer exited before sending the end marker");
                return -1;
            }
            continue;
        }
        n = read(fd, (char *)&r + have, sizeof r - have);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            snprintf(err, en, "read failed: %s", strerror(errno));
            return -1;
        }
        if (n == 0) {   /* EOF: writer closed */
            snprintf(err, en, have ? "FIFO closed in the middle of a message"
                                   : "FIFO closed before the end marker arrived");
            return -1;
        }
        have += (size_t)n;
        if (have < sizeof r) continue;      /* partial message: keep reading */
        have = 0;

        if (r.pid == -1) {                  /* end marker */
            printf("  [scheduler pid=%d] received %d requests from the FIFO (end marker seen)\n",
                   (int)getpid(), got);
            return got;
        }
        if (r.pid < 1 || r.pid > MAX_PROCS || r.page < 0 || r.page > MAX_PAGE ||
            w->count[r.pid] >= MAX_REFS_PER_PROC) {
            snprintf(err, en, "corrupt request (pid=%d page=%d)", r.pid, r.page);
            return -1;
        }
        w->pages[r.pid][w->count[r.pid]++] = r.page;
        if (r.pid > w->nproc) w->nproc = r.pid;
        got++;
    }
}

/* ------------------------------------------------------------------ */
/* Stage 4: Round Robin scheduling SIMULATION                          */
/* ------------------------------------------------------------------ */

static void schedule_round_robin(PipelineState *S)
{
    int queue[MAX_PROCS + 1];
    int head = 0, qn = 0, pos[MAX_PROCS + 1];
    int p, clock = 0;

    memset(pos, 0, sizeof pos);
    S->nstream = 0;
    for (p = 1; p <= S->wl.nproc; p++) {          /* all arrive at time 0 */
        queue[(head + qn) % (MAX_PROCS + 1)] = p;
        qn++;
    }
    while (qn > 0) {
        int k = 0, remaining;
        p = queue[head];
        head = (head + 1) % (MAX_PROCS + 1);
        qn--;
        while (k < S->quantum && pos[p] < S->wl.count[p]) {
            SchedEvent *e = &S->ev[S->nstream];
            e->time = ++clock;
            e->pid = p;
            e->page = S->wl.pages[p][pos[p]];
            e->turn_pos = ++k;
            e->end_reason = 0;
            e->next_pid = 0;
            S->stream[S->nstream].pid = p;
            S->stream[S->nstream].page = e->page;
            S->nstream++;
            pos[p]++;
        }
        remaining = (pos[p] < S->wl.count[p]);
        if (remaining) {
            queue[(head + qn) % (MAX_PROCS + 1)] = p;
            qn++;
        }
        S->ev[S->nstream - 1].end_reason = remaining ? 1 : 2;
        S->ev[S->nstream - 1].next_pid = (qn > 0) ? queue[head] : 0;
    }
}

static void print_sched_trace(const PipelineState *S)
{
    int i;
    printf("Round Robin trace (time quantum = %d reference(s) per turn)\n", S->quantum);
    printf("(a SIMULATION inside this program, not the Linux kernel scheduler)\n\n");
    for (i = 0; i < S->nstream; i++) {
        const SchedEvent *e = &S->ev[i];
        printf("  t=%-3d  P%d executes reference to page %-2d   (turn %d/%d)\n",
               e->time, e->pid, e->page, e->turn_pos, S->quantum);
        if (e->end_reason == 1)
            printf("         -> quantum expired, P%d goes to the back of the ready queue; next: P%d\n",
                   e->pid, e->next_pid);
        else if (e->end_reason == 2) {
            if (e->next_pid)
                printf("         -> P%d finished all its references; next: P%d\n", e->pid, e->next_pid);
            else
                printf("         -> P%d finished all its references; no ready process left\n", e->pid);
        }
    }
}

static void print_stream(FILE *fp, const PipelineState *S)
{
    int i;
    char b[32];
    for (i = 0; i < S->nstream; i++) {
        key_str(b, sizeof b, S->stream[i].pid * PAGE_SPAN + S->stream[i].page);
        fprintf(fp, "%s%s", (i % 10 == 0) ? "  " : " ", b);
        if (i % 10 == 9 || i == S->nstream - 1) fprintf(fp, "\n");
    }
}

/* ------------------------------------------------------------------ */
/* Stage 5: page replacement algorithms (each has its OWN logic/state) */
/* ------------------------------------------------------------------ */

static int find_frame(const int *frames, int nf, int key)
{
    int j;
    for (j = 0; j < nf; j++) if (frames[j] == key) return j;
    return -1;
}

static void snapshot(AlgoResult *r, int i, const int *frames, int nf)
{
    int j;
    for (j = 0; j < MAX_FRAMES; j++) r->frames[i][j] = (j < nf) ? frames[j] : -1;
}

/* FIFO: evict the page that has been in memory the longest. Frames are
 * filled in order 0,1,2..; afterwards slot 'next' (oldest) is replaced and
 * 'next' advances circularly. Hits do NOT change the order. */
static void run_fifo(const int *keys, int n, int nf, AlgoResult *r)
{
    int frames[MAX_FRAMES], used = 0, next = 0, i, j;
    for (j = 0; j < MAX_FRAMES; j++) frames[j] = -1;
    snprintf(r->name, sizeof r->name, "FIFO");
    r->total = n; r->hits = 0; r->faults = 0;
    for (i = 0; i < n; i++) {
        r->victim[i] = -1;
        if (find_frame(frames, nf, keys[i]) >= 0) {
            r->hits++; r->event[i] = 0;
        } else {
            r->faults++;
            if (used < nf) {
                frames[used++] = keys[i]; r->event[i] = 1;
            } else {
                r->victim[i] = frames[next];
                frames[next] = keys[i];
                next = (next + 1) % nf;
                r->event[i] = 2;
            }
        }
        snapshot(r, i, frames, nf);
    }
}

/* LRU: every access (hit or fault) refreshes last_used; evict the smallest. */
static void run_lru(const int *keys, int n, int nf, AlgoResult *r)
{
    int frames[MAX_FRAMES], last_used[MAX_FRAMES], used = 0, i, j;
    for (j = 0; j < MAX_FRAMES; j++) { frames[j] = -1; last_used[j] = -1; }
    snprintf(r->name, sizeof r->name, "LRU");
    r->total = n; r->hits = 0; r->faults = 0;
    for (i = 0; i < n; i++) {
        int idx = find_frame(frames, nf, keys[i]);
        r->victim[i] = -1;
        if (idx >= 0) {
            r->hits++; r->event[i] = 0;
            last_used[idx] = i;
        } else {
            int slot;
            r->faults++;
            if (used < nf) {
                slot = used++; r->event[i] = 1;
            } else {
                slot = 0;
                for (j = 1; j < nf; j++) if (last_used[j] < last_used[slot]) slot = j;
                r->victim[i] = frames[slot]; r->event[i] = 2;
            }
            frames[slot] = keys[i];
            last_used[slot] = i;
        }
        snapshot(r, i, frames, nf);
    }
}

/* Optimal: evict the page whose NEXT use is farthest in the future (or that
 * is never used again). Needs the future => a theoretical benchmark. */
static void run_opt(const int *keys, int n, int nf, AlgoResult *r)
{
    int frames[MAX_FRAMES], used = 0, i, j, t;
    for (j = 0; j < MAX_FRAMES; j++) frames[j] = -1;
    snprintf(r->name, sizeof r->name, "Optimal");
    r->total = n; r->hits = 0; r->faults = 0;
    for (i = 0; i < n; i++) {
        r->victim[i] = -1;
        if (find_frame(frames, nf, keys[i]) >= 0) {
            r->hits++; r->event[i] = 0;
        } else {
            r->faults++;
            if (used < nf) {
                frames[used++] = keys[i]; r->event[i] = 1;
            } else {
                int best = 0, best_next = -1;
                for (j = 0; j < nf; j++) {
                    int nx = n;                       /* n = "never used again" */
                    for (t = i + 1; t < n; t++) if (keys[t] == frames[j]) { nx = t; break; }
                    if (nx > best_next) { best_next = nx; best = j; }
                }
                r->victim[i] = frames[best];
                frames[best] = keys[i];
                r->event[i] = 2;
            }
        }
        snapshot(r, i, frames, nf);
    }
}

/* Child: reads a work order from the parent, runs ONE algorithm, replies. */
static int worker_main(int in_fd, int out_fd, int pause_s)
{
    static WorkOrder order;
    static AlgoResult res;

    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
    if (pause_s > 0) sleep((unsigned)pause_s);

    if (read_full(in_fd, &order, sizeof order) != 0) {
        fprintf(stderr, "worker: could not read work order\n");
        return 1;
    }
    memset(&res, 0, sizeof res);
    switch (order.algo) {
        case ALGO_FIFO: run_fifo(order.keys, order.n, order.nframes, &res); break;
        case ALGO_LRU:  run_lru (order.keys, order.n, order.nframes, &res); break;
        case ALGO_OPT:  run_opt (order.keys, order.n, order.nframes, &res); break;
        default: fprintf(stderr, "worker: bad algorithm id\n"); return 1;
    }
    if (write_full(out_fd, &res, sizeof res) != 0) {
        perror("worker: write result");
        return 1;
    }
    return 0;
}

static void print_algo_trace(FILE *fp, const PipelineState *S, int a)
{
    const AlgoResult *r = &S->res[a];
    int i, j;
    char b[32];

    fprintf(fp, "\n--- %s page replacement trace (%d frames) ---\n", r->name, S->frames);
    fprintf(fp, " Step Request ");
    for (j = 0; j < S->frames; j++) fprintf(fp, "| Frame %-2d ", j + 1);
    fprintf(fp, "| Result\n");
    for (i = 0; i < r->total; i++) {
        key_str(b, sizeof b, S->stream[i].pid * PAGE_SPAN + S->stream[i].page);
        fprintf(fp, " %4d %-7s ", i + 1, b);
        for (j = 0; j < S->frames; j++) {
            key_str(b, sizeof b, r->frames[i][j]);
            fprintf(fp, "| %-8s ", b);
        }
        if (r->event[i] == 0) fprintf(fp, "| HIT\n");
        else if (r->event[i] == 1) fprintf(fp, "| FAULT (loaded into free frame)\n");
        else {
            key_str(b, sizeof b, r->victim[i]);
            fprintf(fp, "| FAULT (replaced %s)\n", b);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Stage 6/7: results, comparison table, report                        */
/* ------------------------------------------------------------------ */

static void print_comparison(FILE *fp, const PipelineState *S)
{
    int a;
    const char *sep =
        "+-----------+------------+--------+--------+----------+------------+\n";
    fputs(sep, fp);
    fprintf(fp, "| Algorithm | References |  Hits  | Faults | Hit Rate | Fault Rate |\n");
    fputs(sep, fp);
    for (a = 0; a < NUM_ALGOS; a++) {
        const AlgoResult *r = &S->res[a];
        fprintf(fp, "| %-9s | %10d | %6d | %6d | %7.2f%% | %9.2f%% |\n",
                r->name, r->total, r->hits, r->faults,
                pct(r->hits, r->total), pct(r->faults, r->total));
    }
    fputs(sep, fp);
}

static void print_explanation(FILE *fp)
{
    fprintf(fp, "Why the results differ:\n");
    fprintf(fp, "  * FIFO evicts the oldest loaded page, even if it is used very often.\n");
    fprintf(fp, "  * LRU evicts the page unused for the longest time (recent past predicts the near future).\n");
    fprintf(fp, "  * Optimal looks at FUTURE references, which a real OS cannot know. It is only a\n");
    fprintf(fp, "    theoretical benchmark: for a fixed reference string and frame count no algorithm\n");
    fprintf(fp, "    can have fewer faults, so it is a lower bound on page faults.\n");
}

static void print_conclusion(FILE *fp, const PipelineState *S)
{
    int a, min = S->res[0].faults;
    int f = S->res[ALGO_FIFO].faults, l = S->res[ALGO_LRU].faults, o = S->res[ALGO_OPT].faults;
    int first = 1;

    for (a = 1; a < NUM_ALGOS; a++) if (S->res[a].faults < min) min = S->res[a].faults;
    fprintf(fp, "Fewest page faults: ");
    for (a = 0; a < NUM_ALGOS; a++) {
        if (S->res[a].faults == min) {
            fprintf(fp, "%s%s", first ? "" : " and ", S->res[a].name);
            first = 0;
        }
    }
    fprintf(fp, " with %d fault(s) out of %d references.\n", min, S->res[0].total);

    if (f < l)       fprintf(fp, "Between the practical policies, FIFO (%d) beat LRU (%d) on this workload.\n", f, l);
    else if (l < f)  fprintf(fp, "Between the practical policies, LRU (%d) beat FIFO (%d) on this workload.\n", l, f);
    else             fprintf(fp, "FIFO and LRU tied with %d faults on this workload.\n", f);

    fprintf(fp, "FIFO is %d fault(s) and LRU is %d fault(s) above the Optimal benchmark (%d).\n",
            f - o, l - o, o);
    if (o > f || o > l)
        fprintf(fp, "WARNING: Optimal produced more faults than another algorithm - this should never happen!\n");
}

static int write_report(const PipelineState *S)
{
    FILE *fp;
    struct stat st;
    time_t now = time(NULL);
    struct tm tmv;
    char when[64];
    int a;

    if (!S->valid) {
        fprintf(stderr, "No results yet. Run the pipeline first.\n");
        return -1;
    }
    if (mkdir(REPORT_DIR, 0755) != 0) {
        if (errno != EEXIST) { perror("mkdir output"); return -1; }
        if (stat(REPORT_DIR, &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Error: '%s' exists but is not a directory.\n", REPORT_DIR);
            return -1;
        }
    }
    fp = fopen(REPORT_PATH, "w");
    if (!fp) { perror("fopen report"); return -1; }

    localtime_r(&now, &tmv);
    strftime(when, sizeof when, "%Y-%m-%d %H:%M:%S", &tmv);

    fprintf(fp, "==================================================================\n");
    fprintf(fp, " VIRTUAL MEMORY PAGE REPLACEMENT VISUALIZER - REPORT\n");
    fprintf(fp, "==================================================================\n");
    fprintf(fp, "Execution date/time   : %s\n", when);
    fprintf(fp, "Workload              : %s\n", S->wname);
    fprintf(fp, "Simulated processes   : %d\n", S->wl.nproc);
    fprintf(fp, "Page frames           : %d\n", S->frames);
    fprintf(fp, "Round Robin quantum   : %d reference(s) per turn\n", S->quantum);
    fprintf(fp, "Total page references : %d\n\n", S->nstream);

    fprintf(fp, "Per-process references received through the named FIFO:\n");
    print_workload(fp, &S->wl);
    fprintf(fp, "\nFinal scheduled reference stream (after Round Robin), shown as Pprocess:page:\n");
    print_stream(fp, S);

    fprintf(fp, "\nAlgorithm comparison (same stream, same %d frames):\n", S->frames);
    print_comparison(fp, S);
    for (a = 0; a < NUM_ALGOS; a++)
        fprintf(fp, "  %-8s hits=%d (%.2f%%)  faults=%d (%.2f%%)\n", S->res[a].name,
                S->res[a].hits, pct(S->res[a].hits, S->res[a].total),
                S->res[a].faults, pct(S->res[a].faults, S->res[a].total));
    fprintf(fp, "\nConclusion:\n");
    print_conclusion(fp, S);
    fprintf(fp, "\n");
    print_explanation(fp);
    fprintf(fp, "\nNote: Round Robin scheduling and virtual memory are SIMULATED in user space.\n");
    fprintf(fp, "The processes and the named FIFO are real Linux facilities.\n");

    if (ferror(fp)) {
        fprintf(stderr, "Error while writing the report.\n");
        fclose(fp);
        return -1;
    }
    if (fclose(fp) != 0) { perror("fclose report"); return -1; }
    return 0;
}

/* ------------------------------------------------------------------ */
/* The integrated pipeline                                             */
/* ------------------------------------------------------------------ */

static int run_pipeline(const Config *cfg)
{
    PipelineState *S = &g_state;
    char err[160];
    Workload src;
    int rfd = -1, prod_reaped = 0, prod_status = 0, got, a, i;
    pid_t prod = -1;
    pid_t wpid[NUM_ALGOS];
    int wrd[NUM_ALGOS];
    static WorkOrder order;

    for (a = 0; a < NUM_ALGOS; a++) { wpid[a] = -1; wrd[a] = -1; }
    memset(S, 0, sizeof *S);
    S->frames = cfg->frames;
    S->quantum = cfg->quantum;

    /* ---- Stage 1: workload generation ---- */
    banner("STAGE 1: Workload generation");
    if (build_workload(cfg, &src, S->wname, sizeof S->wname, err, sizeof err) != 0) {
        fprintf(stderr, "Workload error: %s\n", err);
        return -1;
    }
    printf("%s\nProcesses and their page references:\n", S->wname);
    print_workload(stdout, &src);
    printf("Frames = %d, Round Robin quantum = %d\n", cfg->frames, cfg->quantum);

    /* ---- Stage 2 + 3: process creation and named FIFO ---- */
    banner("STAGE 2+3: Process creation (fork) and named-FIFO IPC");
    printf("  [coordinator pid=%d] creating named FIFO %s\n", (int)getpid(), FIFO_PATH);
    if (create_fifo() != 0) return -1;
    rfd = open(FIFO_PATH, O_RDONLY | O_NONBLOCK);   /* reader first: no blocking */
    if (rfd < 0) { perror("open FIFO (read)"); goto fail; }
    if (cfg->debug) {
        printf("  [debug] running 'ls -l %s' through fork()+exec():\n  ", FIFO_PATH);
        show_fifo_with_exec();
    }
    if (cfg->pause > 0) {
        printf("  [debug pause] the producer will start in %d s. In another terminal try:\n", cfg->pause);
        printf("      ls -l %s\n      ps -ef | grep '[v]m_pipeline'\n      pstree -p\n", FIFO_PATH);
    }
    fflush(stdout);

    prod = fork();
    if (prod < 0) { perror("fork producer"); goto fail; }
    if (prod == 0) {
        int rc;
        close(rfd);
        rc = producer_main(&src, cfg->pause);
        fflush(stdout);
        _exit(rc);
    }
    printf("  [coordinator pid=%d] forked workload producer, pid=%d\n", (int)getpid(), (int)prod);

    got = receive_requests(rfd, prod, &S->wl, &prod_reaped, &prod_status, err, sizeof err);
    close(rfd);
    rfd = -1;
    if (got < 0) { fprintf(stderr, "FIFO transfer failed: %s\n", err); goto fail; }
    if (!prod_reaped) {
        if (waitpid(prod, &prod_status, 0) < 0) { perror("waitpid producer"); prod = -1; goto fail; }
        prod_reaped = 1;
    }
    if (!WIFEXITED(prod_status) || WEXITSTATUS(prod_status) != 0) {
        fprintf(stderr, "Producer did not finish successfully.\n");
        goto fail;
    }
    printf("  [coordinator] producer (pid=%d) reaped with waitpid(), exit status 0\n", (int)prod);
    if (got != total_refs(&src) || S->wl.nproc != src.nproc) {
        fprintf(stderr, "Integrity check failed: sent %d, received %d\n", total_refs(&src), got);
        goto fail;
    }
    printf("  Integrity check OK: %d requests sent == %d received\n", total_refs(&src), got);
    destroy_fifo();
    printf("  Named FIFO %s closed and removed (unlink)\n", FIFO_PATH);

    /* ---- Stage 4: Round Robin scheduling simulation ---- */
    banner("STAGE 4: Round Robin scheduling simulation (user-space model)");
    schedule_round_robin(S);
    if (cfg->show_sched) { print_sched_trace(S); printf("\n"); }
    printf("Scheduled reference stream (Pprocess:page), %d references:\n", S->nstream);
    print_stream(stdout, S);

    /* ---- Stage 5: page replacement analysis in 3 worker processes ---- */
    banner("STAGE 5: Page replacement analysis (3 worker processes)");
    order.n = S->nstream;
    order.nframes = S->frames;
    for (i = 0; i < S->nstream; i++)
        order.keys[i] = S->stream[i].pid * PAGE_SPAN + S->stream[i].page;

    fflush(stdout);
    for (a = 0; a < NUM_ALGOS; a++) {
        int to_w[2], from_w[2];
        if (pipe(to_w) != 0) { perror("pipe"); goto fail; }
        if (pipe(from_w) != 0) { perror("pipe"); close(to_w[0]); close(to_w[1]); goto fail; }
        wpid[a] = fork();
        if (wpid[a] < 0) {
            perror("fork worker");
            close(to_w[0]); close(to_w[1]); close(from_w[0]); close(from_w[1]);
            goto fail;
        }
        if (wpid[a] == 0) {
            int j, rc;
            close(to_w[1]);
            close(from_w[0]);
            for (j = 0; j < a; j++) close(wrd[j]);
            rc = worker_main(to_w[0], from_w[1], cfg->pause);
            close(to_w[0]);
            close(from_w[1]);
            _exit(rc);
        }
        close(to_w[0]);
        close(from_w[1]);
        wrd[a] = from_w[0];
        order.algo = a;
        if (write_full(to_w[1], &order, sizeof order) != 0) {
            perror("write work order");
            close(to_w[1]);
            goto fail;
        }
        close(to_w[1]);
        printf("  [coordinator] forked %s worker, pid=%d, sent it the %d-reference stream\n",
               ALGO_NAMES[a], (int)wpid[a], order.n);
    }
    if (cfg->pause > 0)
        printf("  [debug pause] workers are sleeping %d s; inspect them with ps/pstree now.\n", cfg->pause);
    fflush(stdout);

    for (a = 0; a < NUM_ALGOS; a++) {
        int st;
        if (read_full(wrd[a], &S->res[a], sizeof S->res[a]) != 0) {
            fprintf(stderr, "Could not read result from %s worker\n", ALGO_NAMES[a]);
            goto fail;
        }
        close(wrd[a]);
        wrd[a] = -1;
        if (waitpid(wpid[a], &st, 0) < 0) { perror("waitpid worker"); wpid[a] = -1; goto fail; }
        if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) {
            fprintf(stderr, "%s worker failed\n", ALGO_NAMES[a]);
            wpid[a] = -1;
            goto fail;
        }
        printf("  [coordinator] %s worker (pid=%d) finished and was reaped with waitpid()\n",
               ALGO_NAMES[a], (int)wpid[a]);
        wpid[a] = -1;
        if (S->res[a].total != S->nstream || S->res[a].hits + S->res[a].faults != S->res[a].total) {
            fprintf(stderr, "Inconsistent result from %s worker\n", ALGO_NAMES[a]);
            goto fail;
        }
    }
    S->valid = 1;
    if (cfg->show_repl)
        for (a = 0; a < NUM_ALGOS; a++) print_algo_trace(stdout, S, a);

    /* ---- Stage 6: comparison ---- */
    banner("STAGE 6: Performance comparison");
    printf("Same scheduled stream (%d references), same %d frames for every algorithm.\n\n",
           S->nstream, S->frames);
    print_comparison(stdout, S);
    printf("\n");
    print_conclusion(stdout, S);
    printf("\n");
    print_explanation(stdout);

    /* ---- Stage 7: report ---- */
    banner("STAGE 7: Report generation");
    if (write_report(S) != 0) return -1;
    printf("Report written to %s (view it with: cat %s)\n", REPORT_PATH, REPORT_PATH);
    return 0;

fail:
    if (rfd >= 0) close(rfd);
    if (prod > 0 && !prod_reaped) { kill(prod, SIGTERM); waitpid(prod, NULL, 0); }
    for (a = 0; a < NUM_ALGOS; a++) {
        if (wrd[a] >= 0) close(wrd[a]);
        if (wpid[a] > 0) { kill(wpid[a], SIGTERM); waitpid(wpid[a], NULL, 0); }
    }
    destroy_fifo();
    S->valid = 0;
    return -1;
}

/* ------------------------------------------------------------------ */
/* User interface: menu and command-line                               */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    printf("Usage: %s [options]\n", prog);
    printf("  (no options)   interactive menu\n");
    printf("  -f N           number of page frames, 1..%d (default %d)\n", MAX_FRAMES, DEFAULT_FRAMES);
    printf("  -q N           Round Robin quantum, 1..%d (default %d)\n", MAX_QUANTUM, DEFAULT_QUANTUM);
    printf("  -w N           predefined workload 1..%d (default 1)\n", NUM_PRESETS);
    printf("  -c \"TEXT\"      custom workload, e.g. -c \"1 2 3 1;2 3 4 2\"\n");
    printf("                 ';' separates processes, spaces/commas separate pages (0..%d)\n", MAX_PAGE);
    printf("  -t             show the detailed scheduling trace\n");
    printf("  -r             show the page replacement traces\n");
    printf("  -p SEC         optional debug pause (keeps processes/FIFO alive for inspection)\n");
    printf("  -d             debug: run 'ls -l' on the FIFO using fork()+exec()\n");
    printf("  -m             force the interactive menu\n");
    printf("  -h             this help\n");
}

static int read_line(const char *prompt, char *buf, size_t n)
{
    printf("%s", prompt);
    fflush(stdout);
    if (!fgets(buf, (int)n, stdin)) return -1;
    buf[strcspn(buf, "\n")] = '\0';
    return 0;
}

static int cli_main(int argc, char **argv, Config *cfg)
{
    int i;
    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        int v;
        if (!strcmp(a, "-h") || !strcmp(a, "--help")) { usage(argv[0]); return 0; }
        else if (!strcmp(a, "-t")) cfg->show_sched = 1;
        else if (!strcmp(a, "-r")) cfg->show_repl = 1;
        else if (!strcmp(a, "-d")) cfg->debug = 1;
        else if (!strcmp(a, "-f") || !strcmp(a, "-q") || !strcmp(a, "-w") ||
                 !strcmp(a, "-p") || !strcmp(a, "-c")) {
            if (i + 1 >= argc) { fprintf(stderr, "Option %s needs a value.\n", a); return 2; }
            i++;
            if (!strcmp(a, "-c")) {
                if (strlen(argv[i]) >= sizeof cfg->custom) { fprintf(stderr, "Custom workload too long.\n"); return 2; }
                snprintf(cfg->custom, sizeof cfg->custom, "%s", argv[i]);
                cfg->workload_id = 0;
            } else if (!strcmp(a, "-f")) {
                if (parse_int(argv[i], 1, MAX_FRAMES, &v)) { fprintf(stderr, "Frames must be 1..%d.\n", MAX_FRAMES); return 2; }
                cfg->frames = v;
            } else if (!strcmp(a, "-q")) {
                if (parse_int(argv[i], 1, MAX_QUANTUM, &v)) { fprintf(stderr, "Quantum must be 1..%d.\n", MAX_QUANTUM); return 2; }
                cfg->quantum = v;
            } else if (!strcmp(a, "-w")) {
                if (parse_int(argv[i], 1, NUM_PRESETS, &v)) { fprintf(stderr, "Workload must be 1..%d.\n", NUM_PRESETS); return 2; }
                cfg->workload_id = v;
            } else {
                if (parse_int(argv[i], 0, 60, &v)) { fprintf(stderr, "Pause must be 0..60 seconds.\n"); return 2; }
                cfg->pause = v;
            }
        } else {
            fprintf(stderr, "Unknown option: %s (use -h)\n", a);
            return 2;
        }
    }
    return run_pipeline(cfg) == 0 ? 0 : 1;
}

static void menu_choose_workload(Config *cfg)
{
    char line[512], err[160];   /* same size as cfg->custom */
    int i, v;
    Workload tmp;

    printf("\nWorkloads:\n");
    for (i = 0; i < NUM_PRESETS; i++) printf("  %d) %s\n       %s\n", i + 1, PRESET_NAME[i], PRESET_TEXT[i]);
    printf("  %d) Custom (type your own)\n", NUM_PRESETS + 1);
    if (read_line("Choose workload: ", line, sizeof line) != 0) return;
    if (parse_int(line, 1, NUM_PRESETS + 1, &v) != 0) { printf("Invalid choice.\n"); return; }
    if (v <= NUM_PRESETS) { cfg->workload_id = v; printf("Workload %d selected.\n", v); return; }

    printf("Enter processes separated by ';' and pages separated by spaces (pages 0..%d).\n", MAX_PAGE);
    printf("Example: 1 2 3 1 4 2;2 3 4 2 5 1\n");
    if (read_line("Custom workload: ", line, sizeof line) != 0) return;
    if (parse_workload_string(line, &tmp, err, sizeof err) != 0) {
        printf("Rejected: %s\n", err);
        return;
    }
    snprintf(cfg->custom, sizeof cfg->custom, "%s", line);
    cfg->workload_id = 0;
    printf("Custom workload accepted (%d processes, %d references).\n", tmp.nproc, total_refs(&tmp));
}

static int menu_main(Config *cfg)
{
    char line[64];
    int choice, v;

    for (;;) {
        printf("\n========== Virtual Memory Page Replacement Visualizer ==========\n");
        printf(" Current settings: frames=%d  quantum=%d  workload=%s\n", cfg->frames, cfg->quantum,
               cfg->workload_id ? PRESET_NAME[cfg->workload_id - 1] : "custom");
        printf(" 1) Run the complete integrated pipeline\n");
        printf(" 2) Select number of page frames\n");
        printf(" 3) Select Round Robin time quantum\n");
        printf(" 4) Choose predefined / custom workload\n");
        printf(" 5) Display detailed scheduling trace (last run)\n");
        printf(" 6) Display page replacement traces (last run)\n");
        printf(" 7) Generate / regenerate report\n");
        printf(" 8) Exit\n");
        if (read_line("Enter choice: ", line, sizeof line) != 0) { printf("\n"); return 0; }
        if (parse_int(line, 1, 8, &choice) != 0) { printf("Invalid choice. Enter a number 1-8.\n"); continue; }

        switch (choice) {
        case 1: run_pipeline(cfg); break;
        case 2: {
            char p[64];
            snprintf(p, sizeof p, "Frames (1-%d): ", MAX_FRAMES);
            if (read_line(p, line, sizeof line) != 0) return 0;
            if (parse_int(line, 1, MAX_FRAMES, &v) == 0) cfg->frames = v;
            else printf("Invalid frame count.\n");
            break;
        }
        case 3: {
            char p[64];
            snprintf(p, sizeof p, "Quantum (1-%d): ", MAX_QUANTUM);
            if (read_line(p, line, sizeof line) != 0) return 0;
            if (parse_int(line, 1, MAX_QUANTUM, &v) == 0) cfg->quantum = v;
            else printf("Invalid time quantum.\n");
            break;
        }
        case 4: menu_choose_workload(cfg); break;
        case 5:
            if (!g_state.valid) printf("Run the pipeline first (option 1).\n");
            else print_sched_trace(&g_state);
            break;
        case 6:
            if (!g_state.valid) printf("Run the pipeline first (option 1).\n");
            else { int a; for (a = 0; a < NUM_ALGOS; a++) print_algo_trace(stdout, &g_state, a); }
            break;
        case 7:
            if (write_report(&g_state) == 0) printf("Report written to %s\n", REPORT_PATH);
            break;
        case 8:
            printf("Goodbye.\n");
            return 0;
        }
    }
}

int main(int argc, char **argv)
{
    Config cfg;
    struct sigaction sa;

    memset(&cfg, 0, sizeof cfg);
    cfg.frames = DEFAULT_FRAMES;
    cfg.quantum = DEFAULT_QUANTUM;
    cfg.workload_id = 1;

    signal(SIGPIPE, SIG_IGN);                 /* handle EPIPE as an error code */
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;                /* remove the FIFO on Ctrl-C     */
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (argc > 1 && strcmp(argv[1], "-m") != 0) return cli_main(argc, argv, &cfg);
    return menu_main(&cfg);
}
