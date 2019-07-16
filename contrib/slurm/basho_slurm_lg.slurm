#!/bin/bash -l

# Use this sbatch script to start the bash-bench benchmarks whilst using
# slurm nodes for load generation (calling basho-bench.sh directly will use
# external load generators as defined by config/basho-bench.cfg)

#SBATCH -J load_gen_basho
#SBATCH -p CUMU1
#SBATCH -N 1
#SBATCH --nodelist=cumu01-[00]
#SBATCH -A csr
#SBATCH -t 3-10:00:00
#SBATCH --exclusive
#SBATCH -o example.out

# override of parameters in config/basho-bench.cfg
export LOAD_GENERATORS=1 # load generator are distributed over nodelist

export KIND='load'
export DURATION=10 # single run benchmark duration
export TIMEOUT=60 # timeout when scalaris jobs in killed in minutes
export NODES=5 # number of scalaris nodes
export VMS_PER_NODE=1

# used for load series
if [[ $KIND == "load" ]]; then
    export WORKERS_PER_LG_SERIES="2 3 5 9 17 33 65 129 257 513 1025"
    export VALUE_SIZE="0"
elif [[ $KIND == "value" ]]; then
    main_value
    export WORKERS_PER_LG="256"
    export VALUE_SIZES="65536"
fi

#export OPERATIONS_SERIES="[{put,0},{get,1}] [{put,1},{get,19}] [{put,1},{get,9}]"
export OPERATIONS_SERIES="[{put,1},{get,1}]"

export WD="$CUMUSCRATCH/$USER/rbr_single_writer_r5_x5"
export RESULT_DIR="/local/$USER/rbr_single_writer_r5_x5"

# What kind of benchmark
# basho_bench_driver_scalaris           - rbrcseq
# basho_bench_driver_scalaris_leader    - rbrcseq where all requests are sent to same replica
# basho_bench_driver_scalaris_crdt      - CRDT Paxos
export BASHO_BENCH_DRIVER="basho_bench_driver_scalaris"

$(pwd)/basho-bench.sh
