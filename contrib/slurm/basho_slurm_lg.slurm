#!/bin/bash -l

# Use this sbatch script to start the bash-bench benchmarks whilst using
# slurm nodes for load generation (calling basho-bench.sh directly will use
# external load generators as defined by config/basho-bench.cfg)

#SBATCH -J load_gen_basho
#SBATCH -p CUMU
#SBATCH -N 1
#SBATCH --nodelist=cumu02-03
#SBATCH -A csr
#SBATCH -t 2-12:00
#SBATCH --exclusive
#SBATCH -o example.out

# override of parameters in config/basho-bench.cfg
export LOAD_GENERATORS=1 # load generator are distributed over nodelist

export KIND='load'
export DURATION=10 # single run benchmark duration
export TIMEOUT=60 # timeout when scalaris jobs in killed in minutes
export NODES=3 # number of scalaris nodes
export VMS_PER_NODE=1
export WORKERS_PER_LG_SERIES="128"
export OPERATIONS_SERIES="[{put,1},{get,19}]"

export WD="$CUMUSCRATCH/$USER/crdt_paper/crdt_paxos_crash_batching_read"
export RESULT_DIR="/local/$USER/crdt_paper/crdt_paxos_crash_batching_read"


$(pwd)/basho-bench.sh
