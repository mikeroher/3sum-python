![](https://www.wlu.ca/images/general/desktop_logo.png)

# Threeway Matching Documentation

## Introduction

This is an OpenMPI implementation of a solution to the threeway match problem. The largest dataset size tested was 1M rows by 40 columns. It is designed to run on Sharcnet. The threeway matching problem is a variant of the 3SUM problem. It searches three files to find where a row from each file when summed equals a row of constants. Mathematically, this would be represented as *A[i] + B[j] + C[k] = TARGET*. We are seeking to improve on the naieve implementation of O(n^3) which would not scale when the files are materially large.

## Alogrithim

Similar to SIlicon Valley's famous scene to end Season 1, we will be implementing a middle out algorithm. The middle out algorithm has a time complexity of O(n^2) + O(n). 

Suppose there are three files (A, B and C), each with `mxn` rows and columns and there is a `TARGET` value that is being searched for.

1. Loop through the first two files, A and B, calculating the columnwise sum for each (i.e. sum column 1 of row a with column 1 of row b where row a is from file A and row B is from file B). Store each pair in a dictionary.
2. Loop through the remaining file, C. Calculate the vector: `TARGET - row` for each row, call it *delta*. Then, search for the *delta* in the dictionary created in step 1. If the key exist, then it is a match. 

## Implementation

The implementation was completed in Python with mpi4py, Cython and numpy. Optimizations were made to make the code as efficient as possible. 

* Cython is a static compiler for Python that is designed to give C-like performance with code mostly written in Python. 
* mpi4py is a Python wrapper around MPI (Message Passing Interface)

Thus, the code can be split into two sections: we have the library (`threesum.pyx`) and the main (`run.py`). 

Note: `setup.py` is used solely to compile `threesum.pyx` and `_timing.py` is used solely for timing. Changes to these files are likely not required.

## Library (`threesum.pyx`)

While this file is independent and can be used outside of this project, it is not reccommended. Essentially, this file contains all the functions necessary for the `run.py` to execute. It is compiled into a static library and imported by the `run.py` file. 

### RowPair class

The `RowPair` class holds each pair of rows (stored as numpy array's, `np.ndarray`) from file A and file B. The `__slots__` call is used to reduce memory usage to just the instance variables defined. Thus, you cannot add additional instance variables to the class without modifying the `__slots__` call. The RowPair class is meant to be as lightweight as possible as thousands are stored in the dictionary.

The `row_sum()` function calculates the columnwise row sum of the two rows. It arranges the rows on top of each other and sums downward returning a `1xm` vector.

### chunk_dataframe

The `chunk_dataframe()` method accepts a 2D numpy array and chunks it by rows. The parameter `n` specifies the number of chunks to chunk the rows. If the value `n` does not evenly split the dataframe (i.e. len(df) % n != 0) then the last chunk will be smaller than the others. This method is used for scattering a numpy array across processes with MPI. By splitting it up, each chunk can be sent to a different node to be processed with the results being gathered.

### sum_each_of_first_two_files

TODO: fill in

### find_differences_in_third_file

TODO: fill in

## Main (`run.py`)

### Constants

| Name              | Description                                                  | Example Value           |
| ----------------- | ------------------------------------------------------------ | ----------------------- |
| DATA_PATH         | The directory (no trailing slash) of the inputs (i.e. the three files). | "/Users/mikeroher/3sum" |
| FILE1_NAME        | The filename of the first file. Must be in the `DATA_PATH` directory. | "A.txt"                 |
| FILE2_NAME        | The filename of the second file. Must be in the `DATA_PATH` directory. | "B.txt"                 |
| FILE3_NAME        | The filename of the third file. Must be in the `DATA_PATH` directory. | "C.txt"                 |
| NUMBER_OF_COLUMNS | The number of columns in each of the three files             | 40                      |
| LAMBDA            | The target value                                             | 180                     |
| OUTPUT_FILENAME   | The log file to store the matches                            | f"{DATA_PATH}/3sum.txt" |


## Usage

### On Graham/Cedar

* As of right now (July 31st, 2018), the Graham project directory has reached its maximum file count. Thus, all data needs to be stored in the Home directory for the time being.

```bash
module avail python
module load python/3.6.3
# Install required python packages -- only needs to be done once
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

# Only needs to be done once
pip3 install --user numpy mpi4py cython

# The Makefile has entries for compiling and running but here's the commands:

# Build -- logs into setup.log, will warn you about running time but it's ignorable
sqsub --mpp=1G -q serial -r5m -o setup.log python3 setup.py build_ext --inplace

# Run -- replace {NUM_OF_PROCS} with the number of processors and replace {MEM_PER_PROC} with the memory per process. I typically use values 8 proc's and 12G.
sqsub -r 1d -o 3sum.log -q mpi --nompirun  -n{NUM_OF_PROCS} --mpp={MEM_PER_PROC}G mpiexec -n {NUM_OF_PROCS} python3 run.py
```

## Results

