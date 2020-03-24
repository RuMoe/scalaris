#!/bin/bash -l

# Use this sbatch script to start the bash-bench benchmarks whilst using
# slurm nodes for load generation (calling basho-bench.sh directly will use
# external load generators as defined by config/basho-bench.cfg)

#SBATCH -J load_gen_basho
#SBATCH -p CUMU2
#SBATCH -N 1
#SBATCH -A csr
#SBATCH -t 3-10:00:00
#SBATCH --exclusive
#SBATCH -o example2.out
#SBATCH -w cumu02-00

# override of parameters in config/basho-bench.cfg
export LOAD_GENERATORS=$SLURM_JOB_NUM_NODES # load generator are distributed over nodelist

export KIND='load'
export DURATION=10 # single run benchmark duration
export TIMEOUT=20 # timeout when scalaris jobs in killed in minutes
export NODES=5 # number of scalaris nodes
export VMS_PER_NODE=1

#export OPERATIONS_SERIES="[{put,1},{get,19}]"
export OPERATIONS_SERIES="[{1,[{put,1},{get,0}]},{1,[{put,0},{get,1}]}]" # single writer

# basho bench generation of keys. Change this for single key or multi-key workloads
export KEY_GENERATOR="{int_to_str, {function, basho_bench_fixed_keygen, fixed, [12345]}}"

# What kind of benchmark
# basho_bench_driver_scalaris           - rbrcseq
# basho_bench_driver_scalaris_leader    - rbrcseq where all requests are sent to same replica
# basho_bench_driver_scalaris_crdt      - CRDT Paxos
export BASHO_BENCH_DRIVER="basho_bench_driver_scalaris"

# locations for data collections - must be empty for bench to succeed
export WD="$CUMUSCRATCH/$USER/leader_system_size"
export RESULT_DIR="/local/$USER/leader_system_size"

# location of scalaris configuration file - used to set replication factor
SCALARIS_CONFIG_FILE="$HOME/scalaris/bin/scalaris.cfg"

OUTER_SERIES="size" # none | keygen | config | size

# Define multiple benchmarks with a single parameter changing in scalaris config
# ATTENTION: WD and RESULT_DIR must change in each benchmark run (see below)
# If this is empty, then only a single benchmark is started
# This is necessary, as queueing multiple benchmarks with different scalaris config is not possible
# right now, as each iteration in each benchmark is using a fresh scalaris instance (including the config)
SCALARIS_CONFIG_SERIES_PAR="replication_factor"
SCALARIS_CONFIG_SERIES="10"

#KEYGEN_SERIES="1000000 100000 1000 10"
KEYGEN_SERIES="10000"
NODE_SERIES="3 5 7 9 11"

exec_bench(){
	# set replication degree
	echo "Settings replication_factor to ${NODES}"
	sed -i "s/\s*{replication_factor\s*,\s*[[:digit:]]*\s*}./{replication_factor, $NODES}./g" $SCALARIS_CONFIG_FILE
	# used for load series
	if [[ $KIND == "load" ]]; then
	    export WORKERS_PER_LG_SERIES="64" #32 64 128"
	    export VALUE_SIZE="1"
	elif [[ $KIND == "value" ]]; then
	    export WORKERS_PER_LG="256"
	    export VALUE_SIZES="65536"
	fi
	$(pwd)/basho-bench.sh
}

if [ "$OUTER_SERIES" == "none" ]; then
	exec_bench
elif [ "$OUTER_SERIES" == "keygen" ]; then
	for KEY in $KEYGEN_SERIES; do
		export KEY_GENERATOR="{int_to_str, {pareto_int, $KEY}}"
		#export WD="$CUMUSCRATCH/$USER/rbr_multi_reg_r${NODES}_k${KEY}"
		#export RESULT_DIR="/local/$USER/rbr_multi_reg_r${NODES}_k${KEY}"
		exec_bench
	done
elif [ "$OUTER_SERIES" == "size" ]; then
	for N in $NODE_SERIES; do
		export NODES=$N
		#export KEY_GENERATOR="{int_to_str, {pareto_int, $KEYGEN_SERIES}}"
		export WD="$CUMUSCRATCH/$USER/size_singlewriter_${NODES}"
		export RESULT_DIR="/local/$USER/size_singlewriter_${NODES}"
		exec_bench
	done
elif [ "$OUTER_SERIES" == "config" ]; then
	for VAL in $SCALARIS_CONFIG_SERIES; do
		sed -i "s/\s*{$SCALARIS_CONFIG_SERIES_PAR\s*,\s*[[:digit:]]*\s*}./{$SCALARIS_CONFIG_SERIES_PAR, $VAL}./g" $SCALARIS_CONFIG_FILE
		export WD="$CUMUSCRATCH/$USER/rbr_hanging_writer_r${NODES}_x${VAL}"
		export RESULT_DIR="/local/$USER/rbr_hanging_writer_r${NODES}_x${VAL}"
    	exec_bench
    done
fi
