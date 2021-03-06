<p align="center" float="left">
  <img src="https://www.wlu.ca/images/general/desktop_logo.png" alt="WLU" width="45%" />
  <img src="https://i.imgur.com/rewIBVN.jpg" alt="CARGO Lab" width="45%" /> 
</p>

# Threeway Matching Documentation

TL;DR Important files are `threesum.pyx` and `run.py`. Skip to the [Usage](#usage) to see how to run it on Sharcnet.

## Table of Contents

* [Introduction](#introduction)
* [Algorithm](#algorithm)
* [Implementation](#implementation)
* [Usage](#usage)
* [Results](#results)
* [Areas For Improvement](#areas-for-improvement)

## Introduction

This is an OpenMPI implementation of a solution to the threeway match problem. The largest dataset size tested was 1M rows by 37 columns. It is designed to run on Sharcnet. The threeway matching problem is a variant of the 3SUM problem. It searches three files to find where a row from each file when summed equals a row of constants. Mathematically, this would be represented as *A[i] + B[j] + C[k] = LAMBDA*. We are seeking to improve on the naieve implementation of O(n^3) which would not scale when the files are large.

## Algorithm

We will be implementing a middle out algorithm. The middle out algorithm has a time complexity of O(n^2) + O(n). 

Suppose there are three files (A, B and C), each with `mxn` rows and columns and there is a `LAMBDA` value that is being searched for.

1. Loop through the third file C, Calculate the vector: `LAMBDA - row` for each row, call it *delta*.  Store each delta in a Set() data structure.
2. Loop through the other two files, A and B. calculating the columnwise sum for each (i.e. sum column 1 of row a with column 1 of row b where row a is from file A and row B is from file B).Then, search for the *delta* in the *Set()* created in the step above. If the key exist, then it is a match. If not, then continue looping.

### Advantages

1. Can be parallelized - The approach is parallelized by the MapReduce approach of chunking, scattering and then gathering.
2. Very low memory usage - a million rows occupies roughly 1GB, most of which is just the Python interpreter
3. Fast - O(n^2) + O(n) time complexity. The Set in Python is very fast as it has an underlying hashtable with an O(1) lookup.
4. Handles different lengths of files (i.e. number of rows) natively

### Disadvantages

1. Does not scale to four way matching
2. Scalability past a million rows is questionable.
3. Still n^2. However, there are only special cases of the original 3SUM problem being solved in subquadratic time.

## Implementation

The implementation was completed in Python with mpi4py, Cython and numpy. Optimizations were made to make the code as efficient as possible. 

* Cython is a static compiler for Python that is designed to give C-like performance with code mostly written in Python. 
  * The implementation makes heavy usage of memoryviews. These are Cython's approach to effecient access to memory buffers (i.e. those underlying numpy arrays). 
    * See this for an excellent introduction to them: https://cython.readthedocs.io/en/latest/src/userguide/memoryviews.html
  * In Cython, variables are typed. The values from the rows are stored as `short`'s throughout the implementation as that was the smallest `int` size that safely fit the data.
    * **Make sure to specify data types whereever you mutate the data**
      * Numpy changes the data type under the hood if you don't specify it, resulting in floating point errors.
* mpi4py is a Python wrapper around MPI (Message Passing Interface)

Thus, the code can be split into three sections: we have the compiler (`setup.py`), library (`threesum.pyx`) and the main (`run.py`). 

Note:`_timing.py` is used solely for timing. Changes to these files are likely not required.

> **Tip**: Please refer to the code itself for more extensive documentation. The notes here are simply an overview of the more in-depth comments in the code.

### Compiler (`setup.py`)

This compiles the `threesum.pyx` file into a binary. The line: `include_dirs=[numpy.get_include()` will generate warnings that can be safely ignored. 

> **Tip:** You can also run `make annotate`. This will create an HTML file showing which lines of `threesum.pyx` are natively compiled as C code and which are compiled as Python code. 

### Library (`threesum.pyx`)

While this file is independent and can be used outside of this project, it is not recommended. Essentially, this file contains all the functions necessary for the `run.py` to execute. It is compiled into a static library and imported by the `run.py` file. 

#### chunk_dataframe

The `chunk_dataframe()` method accepts a 2D Numpy array and chunks it by rows. The parameter `n` specifies the number of chunks to chunk the rows. If the value `n` does not evenly split the dataframe (i.e. len(df) % n != 0) then the last chunk will be smaller than the others. This method is used for scattering a numpy array across processes with MPI. By splitting it up, each chunk can be sent to a different node to be processed with the results being gathered.

#### generate_differences_set

The `generate_differences_set()` implements step 1 of the [Algorithm](#algorithm). The method accepts a 2D Numpy array and a LAMBDA value. It loops through each row of the Numpy array, and subtracts the row from the LAMBDA array.

#### find_threeway_match

The `find_threeway_match()` implements step 2 of the [Algorithm](#algorithm). The method accepts two 2D Numpy arrays and the differences set generated above. It loops through the first file, then loops through the second file. It calculates the sum of the row from the first file and the sum of the second file. Then, it searches for that sum in the differences set. If the sum is found, then we have found a match.

### Main (`run.py`)

+ The `File1_Name`, `File2_Name` and `File3_Name` are placeholders for filenames. However, note that you can load your files in any order. I recommend the largest file as file 1 and the smallest file as file 3. This is due to the fact that file 1 gets looped through as the outer loop so you want it to be the largest and file 3 is stored in memory so you want it to be the smallest.

#### Constants

| Name              | Description                                                  | Example Value           |
| ----------------- | ------------------------------------------------------------ | ----------------------- |
| DATA_PATH         | The directory (no trailing slash) of the inputs (i.e. the three files). | "/Users/mikeroher/3sum" |
| FILE1_NAME        | The filename of the first file. Must be in the `DATA_PATH` directory. | "A.txt"                 |
| FILE2_NAME        | The filename of the second file. Must be in the `DATA_PATH` directory. | "B.txt"                 |
| FILE3_NAME        | The filename of the third file. Must be in the `DATA_PATH` directory. | "C.txt"                 |
| NUMBER_OF_COLUMNS | The number of columns in each of the three files             | 40                      |
| LAMBDA            | The target value                                             | 180                     |
| OUTPUT_FILENAME   | The log file to store the matches                            | f"{DATA_PATH}/3sum.txt" |

The main file follows the structure below:

1. Initialize constants based on system (i.e. local or Sharcnet).
2. Read in file C and chunk it into N parts where N is the number of processes.
3. Scatter the C file chunks to each process.
4. Each process, calls `generate_differences_set` on the chunk it received.
5. Gather the `Set` from each process into a list of `Sets`.
6. Merge the list of `Sets` into a master Set and save it to file for future use if needed.
7. Scatter the master Set to each process. Each process now has the same master Set.
8. Read in file A and chunk it into N parts where N is the number of processes.
9. Scatter the A file chunks to each process.
10. Read in file B.
11. Each process, calls `find_threeway_match`, on the file A chunk it received
12. If the user wants to exit early, once a process finds a threeway match, it will
   quit once it finds a match and skip the remaining steps.
13. Gather the matches from each process into a list
14. Loop through the matches and print the results

### Early Termination Option

The user has the option to terminate after the first match. The option has serious implications on the timing of the algorithm, making it significantly dependent on the first file's line number. The first file is split into N chunks where N is the number of processors. The file is split every L/N rows ("RPP") where L is the number of lines in the file (i.e. the first processor will receive [0, RPP], the second will receive [RPP+ 1, RPP * 2], etc.). This is best explained through an example.

For example, if there are 64 processors and 1,000,000 lines then there are 15,625 rows per processor (1M / 64). Suppose the first match occurs on the 15,626 line of the first file. This would be found very quickly by the second processor as it would be the first line it checked. However, if the first match occurs on the 15,624 line of the first file, then it would take just over six days to find the match. While this is not ideal, this is the cost of parallelizing the problem with MPI as we have to split the files somewhere. If the user knows where the first match is, then they could use a number of processors that would split it nicely.

> **Tip:** An interactive running time calculator is included in the repository. See the file Algorithm Timing.xlsx.

Mathematically speaking, to calculate the running time of the program, we first calculate the Multiple, that is, the processor which would be responsible for finding the match. This would also allow us to scale the *First Index* to the [0, RPP] scale. 

<p align="center">
<img src="http://latex2png.com/output//latex_2f36688c68bc8061511c3e0a0af29b85.png" height="50px" />
</p>

To scale the *First Index* to the [0, RPP] scale:


<p align="center"><em style="font-family: serif;">Scaled First Index = Orig First Index - Multiple x RPP</em></p>

Then, we can divide the Scaled First Index by the RPP to calculate the first index as a percentage of the total number of rows per process. This percentage can be looked up in a table to get the running time. 

<p align="center"><em style="font-family: serif;">Percentage = Scaled First Index /  RPP</em></p>

| Time (in   hours) | Max Row | Cum. Time |
| ----------------- | ------- | --------- |
| < 9.6 hours       | 1000    | 0.000     |
| [9.6, 19.2]       | 2000    | 0.128     |
| [19.3, 28.8]      | 3000    | 0.192     |
| [28.9, 38.4]      | 4000    | 0.256     |
| [38.5, 48]        | 5000    | 0.320     |
| [48.1, 57.6]      | 6000    | 0.384     |
| [57.7, 67.2]      | 7000    | 0.448     |
| [67.3, 76.8]      | 8000    | 0.512     |
| [76.9, 86.4]      | 9000    | 0.576     |
| [86.5, 96]        | 10000   | 0.640     |
| [96.1, 105.6]     | 11000   | 0.704     |
| [105.7, 115.2]    | 12000   | 0.768     |
| [115.3, 124.8]    | 13000   | 0.832     |
| [124.9, 134.4]    | 14000   | 0.896     |
| [134.5, 144]      | 15000   | 0.960     |
| > 144 hours       | 15625   | 1.000     |

## Usage

### On Local Machine

* You may need to change the number of processors in the Makefile.
* Sample data is provided in the `sample_data/` directory. The filename indicates how many rows are in the file (i.e. 1000_A.txt has 1000 rows). The `LAMBDA` for these files is 180.

```bash
make local
make run
```



### On Graham/Cedar

* As of right now (July 31st, 2018), the Graham project directory has reached its maximum file count. Thus, all data needs to be stored in the Home directory for the time being.

```bash
module avail python
module load python/3.6.3
# Install required python packages -- only needs to be done once
pip3 install --user numpy mpi4py cython
# Can either submit the compile as a job or do it in home directory.
python3 setup.py build_ext --inplace

# Modify the file `graham_run.sh` to set mem_per_process and number of procs if 64 processes and 2G of memory per is not desired.

# Submit the job to the Graham queue

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

# Run -- replace {NUM_OF_PROCS} with the number of processors and replace {MEM_PER_PROC} with the memory per process. I typically use values 8 proc's and 2G.
sqsub -r 1d -o 3sum.log -q mpi --nompirun  -n{NUM_OF_PROCS} --mpp={MEM_PER_PROC}G mpiexec -n {NUM_OF_PROCS} python3 run.py
```

### Makefile

| Make Command                    | Description                                                  |
| ------------------------------- | ------------------------------------------------------------ |
| `make`/`make all`/ `make local` | Compile `threesum.pyx`                                       |
| `make run`                      | Run `run.py` with 2 proc's                                   |
| `make annotate`                 | Annotate `threesum.pyx` to view an annotated version of `threesum.pyx`. The yellow lines indicate the ineffecient Python operations while the white lines indicate the natively compiled to C operations. It then opens the file in a web browser (OSX only) |
| `make clean`                      | Remove all existing builds. Useful after redeployment    |
| `make sharcnet_build`                      | Submit job to build on Sharcnet Orca          |
| `make sharcnet_run`                      | Submit job to run on Sharcnet Orca. There are config variables at the top of the Makefile to specify the number of processes and memory per process. |



## Results

Here are the results I have generated thus far. Each file has the exact same number of rows and 40* columns.

| File Size          | \# of Proc's | Time      |
| ------------------ | ------------ | --------- |
| 100 lines          | 2            | 0.22 secs |
| 1,000 lines         | 2            | 22 secs   |
| 100,000 lines      | 4            | 24 hours  |
| 1,000,00,000 lines | 8            | 6.25 days |

^\* A million rows was done on 37 columns based on the data provided.

## Areas For Improvement

1. Scale to 100,000,000 rows
2. Unnecessary rows are still checked - once we know that there are no matches where the first column's value is X, could we skip all future X's?
3. Implement in native C. Cython is very very close to native C but there may exist marginal effencies by reimplementing in native C.
