#!/bin/bash -l

# Use this sbatch script to start the bash-bench benchmarks whilst using
# slurm nodes for load generation (calling basho-bench.sh directly will use
# external load generators as defined by config/basho-bench.cfg)

#SBATCH -J load_gen_basho
#SBATCH -p CUMU1
#SBATCH -N 2
#SBATCH --nodelist=cumu01-[00-01]
#SBATCH -A csr
#SBATCH -t 2-12:00
#SBATCH --exclusive
#SBATCH -o example.out

# override of parameters in config/basho-bench.cfg
export LOAD_GENERATORS=2 # load generator are distributed over nodelist

export KIND='load'
export DURATION=5 # single run benchmark duration
export TIMEOUT=60 # timeout when scalaris jobs in killed in minutes
export NODES=3 # number of scalaris nodes
export VMS_PER_NODE=1
export WORKERS_PER_LG_SERIES="1 2 4 8 16 32 64 128 256 512"
export OPERATIONS_SERIES="[{put,0},{get,1}] [{put,1},{get,19}] [{put,1},{get,9}] [{put,1},{get,1}] [{put,1},{get,0}]"

export WD="$CUMUSCRATCH/$USER/rbrpaper_rbrcseq_basic_counter"
export RESULT_DIR="/local/$USER/rbrpaper_rbrcseq_basic_counter"

# What kind of benchmark
# basho_bench_driver_scalaris           - rbrcseq
# basho_bench_driver_scalaris_leader    - rbrcseq where all requests are sent to same replica
# basho_bench_driver_scalaris_crdt      - CRDT Paxos
export BASHO_BENCH_DRIVER="basho_bench_driver_scalaris_leader"

$(pwd)/basho-bench.sh
