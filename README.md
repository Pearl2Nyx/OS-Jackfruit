# Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor and a kernel-space memory monitor.

Read [`project-guide.md`](project-guide.md) for the full project specification.

---

## Team Information

| Name | SRN |
|------|-----|
| Nirupama Jayaraman | PES2UG24CS324 |
| Niya Ann Joseph | PES2UG24CS329 |

---

## Instructions

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
ls -l /dev/container_monitor   # verify device created
```

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
sudo dmesg | tail -10
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








