# DORADD

DORADD is a high-performance deterministic parallel runtime.

Deterministic parallel execution guarantees that given exactly the same input, it will produce the same output via parallel execution.
This property is essential for building robust distributed and fault-tolerant systems, as well as simplifying testing and debugging processes.
For instance, 
each node in state machine replication needs to execute a pre-agreed sequence of operations where deterministic parallelism can mitigate the single-threaded bottleneck without incurring states divergence;
deterministically parallel log replay can be applied to fast failure recovery and live migration;
deterministic databases scale efficiently via bypassing the need of two-phase commit across partitions.

## References
```
@inproceedings{doradd,
  title={DORADD: Deterministic Parallel Execution in the Era of Microsecond-Scale Computing},
  author={Liu, Zhengqing and Unal, Musa and Parkinson, Matthew J and Kogias, Marios},
  booktitle={Proceedings of the 30th ACM SIGPLAN Annual Symposium on Principles and Practice of Parallel Programming},
  pages={282--296},
  year={2025}
}
```

## Building

1. Clone and pull the submodules
```
git clone https://github.com/doradd-rt/doradd
cd doradd
git submodule update --init
```

2. Install dependencies on Linux (tested on Ubuntu 22.04)

```
sudo apt-get update
sudo apt-get install -y pkg-config libssl-dev clang libclang-dev build-essential ninja-build cmake cpufrequtils htop python3-pip
pip install numpy
```

3. Build the YCSB and TPCC-NP benchmark

```
cd app/
mkdir -p build && cd build
mkdir -p results # collect stats
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY"
ninja
```

## Example workloads

### 1. YCSB

```
# prepare the input log
pushd app/ycsb/gen-log
g++ -o generator -O3 generate_ycsb_zipf.cc 
./generator -d uniform -c no_cont
popd

# run
cd app/build
sudo taskset -c 4-11 ./ycsb -n 8 ../ycsb/gen-log/ycsb_uniform_no_cont.txt -i exp:4000

# -n: core counts
# -i: the request arrival pattern (fixed or exponential) and interval (in nanoseconds) 
```

### 2. TPCC-NP

```
# prepare the input log
pushd app/tpcc/gen-log
python generate_tpcc.py --warehouses 23 
popd

# run
cd app/build
sudo taskset -c 4-11 ./tpcc -n 8 ../tpcc/gen-log/tpcc.txt -i exp:4000
```
