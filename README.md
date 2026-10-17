# Virtual Memory Page Replacement Visualizer

An Operating Systems mini-project in a single C file (`vm_pipeline.c`) for Ubuntu Linux. It compares **FIFO**, **LRU** and **Optimal** page replacement on a workload that flows through a real multi-process pipeline:

```
Workload -> fork() -> named FIFO IPC -> Round Robin simulation -> page replacement (3 worker processes) -> report
```

- **Real Linux facilities:** `fork`, `waitpid`, `mkfifo`, `open`/`read`/`write`/`close`/`unlink`, `pipe`, `poll`, file I/O.
- **Simulated in user space:** Round Robin CPU scheduling and virtual memory (pages, frames, faults). The program does not touch the kernel scheduler or real page tables.

## Requirements

- Ubuntu Linux (or any Linux with GCC and POSIX support)
- GCC. No other libraries, no internet, no extra files.

Check GCC, and install it if missing:

```bash
gcc --version
sudo apt update && sudo apt install build-essential
```

## Setup

```bash
mkdir -p ~/VirtualMemoryVisualizer
cd ~/VirtualMemoryVisualizer
nano vm_pipeline.c      # paste the code, Ctrl+O, Enter, Ctrl+X
```

(Or copy the provided `vm_pipeline.c` into this folder.)

## Compile

```bash
gcc -Wall -Wextra -std=c11 vm_pipeline.c -o vm_pipeline
```

This builds with no warnings and needs no extra flags.

## Run

### Interactive menu

```bash
./vm_pipeline
```

| Option | Action |
|---|---|
| 1 | Run the complete pipeline |
| 2 | Set number of page frames (1-10, default 3) |
| 3 | Set Round Robin time quantum (1-64, default 2) |
| 4 | Choose a predefined workload or enter a custom one |
| 5 | Show the scheduling trace of the last run |
| 6 | Show the page replacement traces of the last run |
| 7 | Generate or regenerate the report |
| 8 | Exit |

### Command-line options

```bash
./vm_pipeline -w 3 -f 3 -q 2
```

| Option | Meaning |
|---|---|
| `-w N` | Predefined workload 1-4 (default 1) |
| `-c "TEXT"` | Custom workload (format below) |
| `-f N` | Page frames, 1-10 (default 3) |
| `-q N` | Round Robin quantum, 1-64 (default 2) |
| `-t` | Show the Round Robin scheduling trace |
| `-r` | Show frame contents after every reference |
| `-p SEC` | Optional debug pause so you can inspect the FIFO and processes |
| `-d` | Debug: run `ls -l` on the FIFO using `fork()` + `exec()` |
| `-m` | Force the interactive menu |
| `-h` | Help |

Any option starts a single non-interactive run; with no options you get the menu.

### Predefined workloads

| # | Description |
|---|---|
| 1 | Three processes (P1: `1 2 3 1 4 2`, P2: `2 3 4 2 5 1`, P3: `1 4 2 5 3 2`) |
| 2 | Two processes with repeated pages (many hits) |
| 3 | Single process, textbook string `7 0 1 2 0 3 0 4 2 3 0 3 2 1 2 0 1 7 0 1` |
| 4 | Single process, Belady's anomaly string `1 2 3 4 1 2 5 1 2 3 4 5` |

### Custom workload format

Processes are separated by `;`, pages by spaces or commas. Pages are integers 0-99. Limits: 8 processes, 64 references per process, 256 in total.

```bash
./vm_pipeline -c "1 2 3 1 4 2;2 3 4 2 5 1" -f 3 -q 2
```

Invalid frame counts, quanta, pages or malformed workloads are rejected with an error message.

## Example runs

```bash
./vm_pipeline -w 3 -f 3            # textbook string: expect FIFO 15, LRU 12, Optimal 9 faults of 20
./vm_pipeline -w 2 -f 4            # repeated pages: 4 faults, 8 hits for every algorithm
./vm_pipeline -w 4 -f 3            # Belady's anomaly: FIFO has 9 faults...
./vm_pipeline -w 4 -f 4            # ...and 10 faults with one more frame
./vm_pipeline -w 1 -q 6 -f 3       # multi-process workload
./vm_pipeline -w 1 -q 2 -t         # show the Round Robin trace
./vm_pipeline -w 3 -f 3 -r         # show frame contents step by step
```

## Output

- The terminal shows each pipeline stage and a comparison table with references, hits, faults, hit rate and fault rate for each algorithm, plus the algorithm with the fewest faults.
- The report is written to `output/report.txt` in the directory you ran from:

```bash
cat output/report.txt
```

The report and the terminal table are produced from the same data, so they always agree.

## Inspecting the FIFO and processes while running

The pipeline finishes in milliseconds, so use the optional pause. Terminal 1:

```bash
./vm_pipeline -w 3 -p 20 -d
```

Terminal 2:

```bash
ls -l /tmp/vm_pipeline_fifo         # first character 'p' means a named pipe
ps -ef | grep '[v]m_pipeline'       # parent and child processes
pstree -p                           # optional: sudo apt install psmisc
```

Optional system-call trace (`strace` may need installing: `sudo apt install strace`):

```bash
strace -f -e trace=mkfifo,open,openat,read,write,close,unlink,fork,clone,wait4 ./vm_pipeline -w 3
```

The pause is only for demonstration and is never needed for correctness.

## Clean up

```bash
rm -rf output vm_pipeline
```

The program removes `/tmp/vm_pipeline_fifo` itself, including on Ctrl-C.

## Troubleshooting

| Problem | Fix |
|---|---|
| `gcc: command not found` | `sudo apt install build-essential` |
| Compile errors after pasting | A paste lost lines; copy the whole file again |
| `/tmp/vm_pipeline_fifo exists and is NOT a FIFO` | Another file is at that path; the program will not delete it. Move it yourself |
| Two copies running at once interfere | They share one FIFO path; run one at a time |
| `ps` shows nothing | The run is very fast; add `-p 20` |
| `output/report.txt` not found | It is created in the current directory; check where you ran the program |
| FIFO and LRU show 0 hits on workload 1 | Valid for small quanta (pages are per process and reuse is far apart). Try `-w 3` or `-q 6` |

## Notes

- A page is identified by *(process, page)*, so `P1:2` and `P2:2` are different pages. Frames are shared by all processes.
- Optimal needs future knowledge, so it is a theoretical benchmark and a lower bound on faults for a fixed reference string and frame count.
- The named FIFO (IPC) and FIFO page replacement are unrelated; they only share a name.
- Limits: 8 processes, 256 references, 10 frames; no TLB, dirty pages or real address translation.
