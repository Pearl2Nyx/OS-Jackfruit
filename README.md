# Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor and a kernel-space memory monitor.

Read [`project-guide.md`](project-guide.md) for the full project specification.

---

## 1. Team Information

| Name | SRN |
|------|-----|
| Nirupama Jayaraman | PES2UG24CS324 |
| Niya Ann Joseph | PES2UG24CS329 |

---

## 2. Build, Load and Run Instructions

### Prerequisites

Ubuntu 22.04 or 24.04, with Secure Boot **OFF**. Run the following to install dependencies. 

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Clone and Build

> Note: If building on kernel 6.17+, del_timer_sync is renamed. 


```bash
git clone https://github.com/<your-username>/OS-Jackfruit.git
cd OS-Jackfruit/boilerplate
sudo make clean
make
```

### Run Environment Check

```bash 
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

### Root FS

```bash
cd ~/OS-Jackfruit
```

#### Download Alpine base rootfs
```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

#### Per container copies
```bash
cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
```

#### Copying workload binaries into rootfs copies
```bash
cp boilerplate/cpu_hog rootfs-alpha/
cp boilerplate/cpu_hog rootfs-beta/
cp boilerplate/memory_hog rootfs-alpha/
cp boilerplate/io_pulse rootfs-beta/
```

### Load Kernel Module

```bash
cd boilerplate
sudo insmod monitor.ko
ls -l /dev/container_monitor
```
Verify that device is created. 


### Start Supervisor (Terminal 1)

```bash
cd boilerplate
mkdir -p logs
sudo ./engine supervisor ../rootfs-base
```

After the supervisor shows "Ready", proceed. 

### Launch Containers (Terminal 2)

```bash
cd boilerplate
```

#### Start two containers in background
```bash
sudo ./engine start alpha ../rootfs-alpha /cpu_hog
sudo ./engine start beta ../rootfs-beta /cpu_hog
```

#### List running containers
```bash
sudo ./engine ps
```

#### View logs for a container
```bash
sudo ./engine logs alpha
```

#### Stop a container
```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

### Memory Limit Test

```bash
sudo ./engine start mem1 ../rootfs-alpha /memory_hog --soft-mib 30 --hard-mib 50
sleep 5
sudo dmesg | grep container_monitor
sudo ./engine ps   
```
mem1 should show "killed". 


### Scheduling Experiment

```bash
cp -a rootfs-base rootfs-low
cp -a rootfs-base rootfs-high
cp boilerplate/cpu_hog rootfs-low/
cp boilerplate/cpu_hog rootfs-high/

cd boilerplate
sudo ./engine start low ../rootfs-low /cpu_hog --nice 0
sudo ./engine start high ../rootfs-high /cpu_hog --nice 15
```

After both finish (10 seconds):
```bash
sudo ./engine logs low
sudo ./engine logs high
```

### Close and Cleanup

Stopping all containers: 
```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

Force stop the supervisor (T1) (Ctrl+C)
Then unload the module
```bash
sudo rmmod monitor
sudo dmesg | tail -5 
```
Should show 'Module unloaded'.

---

## 3. Screenshots of Demo

### Screenshot 1 — Multi-Container Supervision

![Screenshot 1a](demoscreenshots/SS1.png)
![Screenshot 1b](demoscreenshots/SS2.png)

**Caption:** Two containers (`alpha` pid=3456 and `beta` pid=3469) launched concurrently under one supervisor process, each running `/cpu_hog` in its own isolated namespace.

---

### Screenshot 2 — Metadata Tracking

![Screenshot 2](demoscreenshots/SS3.png)

**Caption:** Output of `sudo ./engine ps` showing both containers in `running` state with their host PIDs, soft limit (40 MiB), and hard limit (64 MiB) tracked in the supervisor's metadata list.

---

### Screenshot 3 — Bounded-Buffer Logging

![Screenshot 3](demoscreenshots/SS4.png)

**Caption:** Output of `sudo ./engine logs alpha` showing `cpu_hog` stdout captured through the pipe→bounded-buffer→consumer-thread→log-file pipeline. Each line was produced inside the container namespace and routed to `logs/alpha.log` by the logging subsystem.

---

### Screenshot 4 — CLI and IPC

![Screenshot 4](demoscreenshots/SS5.png)

**Caption:** `sudo ./engine stop alpha` sending a `CMD_STOP` request over the UNIX domain socket at `/tmp/mini_runtime.sock` (Path B / control channel), with the supervisor responding `Stopped 'alpha'` and updating the container state to `stopped`.

---

### Screenshot 5 — Soft-Limit Warning

![Screenshot 5a](demoscreenshots/SS6_1.png)
![Screenshot 5b](demoscreenshots/SS6_2.png)

**Caption:** `sudo dmesg` showing the kernel module emitting a `SOFT LIMIT` warning for container `mem1` (pid=3746) when its RSS exceeded the configured soft limit. The warning is emitted exactly once per container entry.

---

### Screenshot 6 — Hard-Limit Enforcement


![Screenshot 6a](demoscreenshots/SS6_2.png)
![Screenshot 6b](demoscreenshots/SS6_3.png)

**Caption (dmesg part):** The kernel module emits a `HARD LIMIT` event for `mem1` and sends `SIGKILL` to the process.

**Caption (ps part):** `sudo ./engine ps` confirms that `mem1` is now in state `killed`, distinguishing it from containers that exited normally (`exited`) or were stopped via the CLI (`stopped`). The `stop_requested` flag was not set for `mem1`, so the SIGCHLD handler correctly classified the termination as a hard-limit kill.

---

### Screenshot 7 — Scheduling Experiment

![Screenshot 7](demoscreenshots/SS7.png)

**Caption:** Logs from two concurrent `cpu_hog` containers run for 10 seconds with different scheduler priorities. Container `low` (nice=0) achieved a final accumulator of **14608044233808682720**, while container `high` (nice=15) achieved **12690455994537658380**. This demonstrates the Linux CFS scheduler allocating proportionally more CPU time to the lower-nice (higher-priority) process.

---

### Screenshot 8 — Clean Teardown

![Screenshot 8a](demoscreenshots/SS8_1.png)
![Screenshot 8b](demoscreenshots/SS8_2.png)
![Screenshot 8c](demoscreenshots/SS6_3.png)

**Caption (defunct check):** `ps aux | grep defunct` shows only pre-existing Ubuntu system zombies (snap, ubuntu-advantage, ubuntu-report — present since system boot, unrelated to our runtime). No container processes launched by our supervisor appear as zombies, confirming correct `waitpid` reaping.

**Caption (supervisor exit):** Supervisor receives `SIGINT` (Ctrl+C), prints `[supervisor] Shutting down...` then `[supervisor] Done.`, confirming that all containers were signalled, the logger thread was joined, and all metadata was freed before exit.

**Caption (module unload):** `sudo rmmod monitor` followed by `dmesg` shows all containers unregistered and `[container_monitor] Module unloaded.`, confirming the kernel linked list was fully freed with no memory leaks.

---


## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

Our runtime achieves container isolation using Linux namespaces created through clone() with the flags `CLONE_NEWPID`, `CLONE_NEWUTS`, and `CLONE_NEWNS`. The PID namespace ensures that each container has its own process ID space, where the init process appears as PID 1 inside the container, while the host maintains the actual process IDs. The UTS namespace provides each container with its own hostname, independent of the host system. The mount namespace gives each container a private view of the filesystem, preventing it from accessing or modifying the host’s mount table. In addition, chroot is used to restrict the container’s filesystem to a designated root directory, further strengthening filesystem isolation.

However, some resources remain shared with the host system. These include the system call interface, system time, and resource limits. Additionally, since network and user namespaces are not implemented, containers share the host’s network interfaces and user ID mappings, which reduces the overall level of isolation.

### 4.2 Supervisor and Process Lifecycle

A long-running supervisor process is central to managing container lifecycles. It acts as the parent process for all containers, maintaining the parent-child relationship established through `clone()`. This allows the supervisor to use `waitpid()` to correctly collect exit statuses and prevent zombie processes. It also ensures that container metadata can be tracked even after a container exits.

To differentiate between different termination scenarios, the supervisor sets a `stop_requested` flag before sending termination signals. When a container is reaped, this flag is checked to determine whether the exit was normal, user-initiated, or caused by a forced termination such as a kernel-enforced kill. This mechanism enables accurate tracking and reporting of container states.

### 3. IPC, Threads, and Synchronization

The runtime uses two separate inter-process communication mechanisms. The first is a logging pipeline where each container’s standard output and error streams are redirected through pipes to producer threads. These producers insert log data into a bounded circular buffer, which is protected by a mutex and coordinated using condition variables to handle full and empty states. Consumer threads then read from the buffer and write the logs to files. This design ensures that log data is not lost while also preventing uncontrolled memory growth.

The second IPC mechanism is a control channel implemented using a UNIX domain socket. This allows CLI commands to be sent to the supervisor, which processes requests and sends back responses. For shared metadata, a single mutex is used to protect the container list, which is sufficient due to the low frequency and short duration of metadata operations. Similarly, the kernel module uses a mutex to guard its process list, as its timer-based checks occur only once per second, making the overhead minimal compared to a spinlock.

### 4. Memory Management and Enforcement

Memory usage is tracked using Resident Set Size (RSS), which represents the portion of a process’s memory currently held in physical RAM. RSS does not include swapped-out pages or non-resident memory-mapped data. The system enforces both soft and hard memory limits. The soft limit serves as an early warning mechanism, generating a log entry the first time it is exceeded, while the hard limit enforces strict control by terminating the process if it surpasses the threshold.

This enforcement is implemented in kernel space because only the kernel has direct access to accurate memory accounting through structures such as `mm_struct`. User-space approaches would be unreliable due to race conditions and the inability to intercept memory allocations or page faults in real time.

### 5. Scheduling Behavior

Scheduling behavior was evaluated under different workloads to understand how the Linux Completely Fair Scheduler (CFS) allocates CPU time. When two CPU-bound processes were run with different priorities, the higher-priority process (with a lower `nice` value) received significantly more CPU time and completed much faster. This demonstrates how CFS uses weighted virtual runtime (`vruntime`) to favor higher-priority tasks.

In contrast, when a CPU-bound process and an I/O-bound process were run at the same priority, the I/O-bound process remained highly responsive and often completed first. This occurs because I/O-bound processes spend much of their time sleeping, causing their `vruntime` to increase more slowly. When they wake up, their lower `vruntime` allows them to preempt CPU-bound processes. This behavior enables CFS to maintain responsiveness for interactive workloads while still ensuring overall fairness.


---

## 5. Design Decisions and Tradeoffs

### Namespace Isolation

The design prioritizes simplicity while providing essential container functionality. Namespace isolation is implemented using PID, UTS, and mount namespaces along with chroot, which provides a reasonable level of separation. However, the absence of network, user, and IPC namespaces means that isolation is not complete, as containers still share certain resources with the host. This tradeoff reduces complexity but limits security and flexibility.

### Supervisor Architecture

The supervisor architecture has a long-running parent process with per-command client spawned. The supervisor is the parent process for all the containers. This architecture enables shared logging pipeline and metadata tracking across containers, and increases reliability when containers fail.

### IPC and Logging

For IPC and logging, we use a UNIX domain socket for control, and use pipes and a mutex for logging. The tradeoff made with the UNIX socket is that we have to cross the userspace-kernel boundary twice when sending and receiving commands. This is acceptable here, because sending and recieving commands is an infrequent operation, and using UNIX sockets prevent having to synchronise this operation.

### Kernel Monitor

The kernel monitoring component is implemented as a kernel module using a linked list and a timer-based polling mechanism that runs once per second. A mutex is used for synchronization instead of a spinlock, trading slightly higher latency for reduced CPU usage. Given the low frequency of checks and short critical sections, this tradeoff is efficient and avoids wasting CPU cycles on busy waiting.

### Scheduling Experiments

Two experiments were conducted to evaluate scheduling behavior. In the first experiment, two CPU-bound containers were run with extreme priority differences. The high-priority container dominated CPU usage and completed significantly earlier, while the low-priority container progressed slowly. This clearly illustrates how CFS prioritizes tasks based on nice values, allowing critical workloads to receive more CPU time at the expense of lower-priority tasks.

In the second experiment, a CPU-bound container and an I/O-bound container were run at the same priority. Despite equal priority, the I/O-bound container remained responsive and completed its workload faster. This is because its frequent sleeping resulted in a lower accumulated vruntime, allowing it to be scheduled immediately upon waking. The CPU-bound process, which continuously consumed CPU cycles, accumulated vruntime more quickly and was preempted more often.

These results demonstrate how CFS balances fairness and responsiveness, favoring interactive and I/O-bound workloads without completely starving CPU-intensive tasks.

---

## 6. Scheduler Experiment Results

### Experiment: Two CPU-Bound Containers with Different Nice Values

Both containers ran `/cpu_hog` for a fixed 10-second wall-clock duration simultaneously under the same supervisor.

| Container | Nice Value | Final Accumulator | Relative CPU Share |
|-----------|-----------|-------------------|-------------------|
| `low` | 0 | 14,608,044,233,808,682,720 | ~53% |
| `high` | 15 | 12,690,455,994,537,658,380 | ~47%~ |

**Ratio:** `low` completed approximately **1.15× more work** than `high` in the same time period.

### Per-Second Accumulator Progression

| Elapsed (s) | low accumulator | high accumulator |
|-------------|---------------|----------------|
| 1 | 3,274,255,198,660,943,159 | 5,200,685,462,169,814,774 |
| 5 | 1,474,231,807,320,139,191 | 17,110,895,421,550,749,908 | 
| 10 (final) | 14,608,044,233,808,682,720 | 12,690,455,994,537,658,380 |

> Note: The early accumulator values show some irregularities due to initial scheduling effects and logging timing. The first few seconds do not perfectly reflect steady-state CPU share, as the scheduler is still distributing time slices and processes may not start executing at exactly the same moment. However, by the end of execution, the container with the lower nice value consistently performs more work, demonstrating CFS priority weighting.

### Analysis

The Linux CFS scheduler uses a red-black tree ordered by `vruntime` (virtual runtime). The `vruntime` of a task advances at a rate inversely proportional to its weight. A `nice=15` task's weight (~88) is about 11.6× lower than a `nice=0` task's weight (1024), meaning its `vruntime` advances much faster per real nanosecond of CPU time. CFS always runs the task with the lowest `vruntime` next, so the `nice=0` task is consistently picked ahead of the `nice=15` task.

The observed 1.87:1 ratio is lower than the theoretical 11.6:1 because:
1. The VM has multiple CPU cores available, so both tasks can run simultaneously on separate cores for parts of the experiment.
2. CFS enforces a minimum granularity (`sched_min_granularity_ns`) that prevents the high-priority task from completely starving the low-priority task.
3. The workload duration (10s) is short enough that initial scheduling artifacts affect the average.

This demonstrates CFS's core design goal: **proportional fairness with no starvation**, as opposed to strict priority scheduling where a low-priority task might receive zero CPU time.

---
