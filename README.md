# Threeway Matching Documentation

## Introduction

This is an OpenMPI implementation of a solution to the threeway match problem. The largest dataset size tested was 1M rows by 40 columns. It is designed to run on Sharcnet. The threeway matching problem is a variant of the 3SUM problem. It searches three files to find where a row from each file when summed equals a row of constants. We are seeking to improve on the naieve implementation of O(n^3) which would not scale when the files are materially large.



## Alogrithim

Similar to SIlicon Valley's famous scene to end Season 1, we will be implementing a middle out algorithim.

Suppose there are three files (A, B and C), each with `mxn` rows and columns and there is a `TARGET` value that is being searched for.

1. Loop through the first two files, A and B, calculating the columnwise sum for each (i.e. sum column 1 of row a with column 1 of row b where row a is from file A and row B is from file B). Store each pair in a dictionary.
2. Loop through the remaining file, C. Calculate the vector: TARGET - row for each

## Implementation



## Usage

### On Graham/Cedar

```bash
module avail python
module load python/3.6.3
# Install required python packages
pip3 install --user numpy mpi4py cython
# Can either submit the compile as a job or do it in home directory.
python3 setup.py build_ext --inplace

# Modify the file `graham_run.sh` to set mem_per_process and number of procs
sbatch graham_run.sh

# Get currently runinng jobs
squeue -u $USER

```

### On Orca

```bash
# Load the proper modules. Must be in this order as there are dependencies that must be 
# removed in order to be successful.
module unload intel 12.1.3
module unload intel/tbb/18.0.1
module unload mkl/10.3.9
module load intel/15.0.3
module load python/intel/3.6.0

# Tip: I have this aliased in bash as `load_modules`
alias load_modules='module unload intel 12.1.3 && module unload intel/tbb/18.0.1 && module unload mkl/10.3.9 && module load intel/15.0.3  && module load python/intel/3.6.0'

pip3 install --user numpy mpi4py cython

# The Makefile has entries for compiling and running but here's the commands:

# Build -- logs into setup.log, will warn you about running time but it's ignorable
sqsub --mpp=1G -q serial -r5m -o setup.log python3 setup.py build_ext --inplace

# Run -- replace {NUM_OF_PROCS} with the number of processors and replace {MEM_PER_PROC} with the memory per process. I typically use values 8 proc's and 12G.
sqsub -r 1d -o 3sum.log -q mpi --nompirun  -n{NUM_OF_PROCS} --mpp={MEM_PER_PROC}G mpiexec -n {NUM_OF_PROCS} python3 run.py
```

