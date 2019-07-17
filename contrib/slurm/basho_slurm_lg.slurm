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

#export OPERATIONS_SERIES="[{put,0},{get,1}] [{put,1},{get,19}] [{put,1},{get,9}]"
export OPERATIONS_SERIES="[[{1,[{put,1},{get,0}]},{1,[{put,0},{get,1}]}]]" # single writer

# basho bench generation of keys. Change this for single key or multi-key workloads
export KEY_GENERATOR="{int_to_str, {function, basho_bench_fixed_keygen, fixed, [12345]}}"

# What kind of benchmark
# basho_bench_driver_scalaris           - rbrcseq
# basho_bench_driver_scalaris_leader    - rbrcseq where all requests are sent to same replica
# basho_bench_driver_scalaris_crdt      - CRDT Paxos
export BASHO_BENCH_DRIVER="basho_bench_driver_scalaris"

# locations for data collections - must be empty for bench to succeed
export WD="$CUMUSCRATCH/$USER/rbr_single_writer_r$NODES_x5"
export RESULT_DIR="/local/$USER/rbr_single_writer_r$NODES_x5"

# location of scalaris configuration file - used to set replication factor
SCALARIS_CONF_FILE="$HOME/scalaris/bin/scalaris.cfg"

# Define multiple benchmarks with a single parameter changing in scalaris config
# ATTENTION: WD and RESULT_DIR must change in each benchmark run (see below)
# If this is empty, then only a single benchmark is started
# This is necessary, as queueing multiple benchmarks with different scalaris config is not possible
# right now, as each iteration in each benchmark is using a fresh scalaris instance (including the config)
SCALARIS_CONF_SERIES_PAR="read_attempts_without_progress"
SCALARIS_CONF_SERIES="0 1 2 5 10"

exec_bench(){
	# set replication degree
	sed -i "s/\s*{replication_factor\s*,\s*[[:digit:]]*\s*}./{replication_factor, $NODES}./g" $SCALARIS_CONF_FILE
	# used for load series
	if [[ $KIND == "load" ]]; then
	    export WORKERS_PER_LG_SERIES="2 3 5 9 17 33 65 129 257 513 1025"
	    export VALUE_SIZE="0"
	elif [[ $KIND == "value" ]]; then
	    export WORKERS_PER_LG="256"
	    export VALUE_SIZES="65536"
	fi
	$(pwd)/basho-bench.sh
}

if [ -z ${SCALARIS_CONF_SERIES_PAR+x} ]; then
	exec_bench
else 
	for VAL in $SCALARIS_CONF_SERIES; do
		sed -i "s/\s*{$SCALARIS_CONF_SERIES_PAR\s*,\s*[[:digit:]]*\s*}./{$SCALARIS_CONF_SERIES_PAR, $VAL}./g" $SCALARIS_CONF_FILE
		export WD="$CUMUSCRATCH/$USER/rbr_single_writer_r${NODES}_x${VAL}"
		export RESULT_DIR="/local/$USER/rbr_single_writer_r${NODES}_x${VAL}"
    	exec_bench
    done
fi