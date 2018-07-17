#!/usr/bin/env bash
echo "THIS WILL SETUP SHARCNET WITH THE NECESSARY MODULES"
module unload intel 12.1.3
module unload intel/tbb/18.0.1
module unload mkl/10.3.9
module load intel/15.0.3
module load python/intel/3.6.0

#alias load_modules='module unload intel 12.1.3 && module unload intel/tbb/18.0.1 && module unload mkl/10.3.9 && module load intel/15.0.3  && module load python/intel/3.6.0'


pip3 install --user pandas # numpy and itertools already installed
pip3 install --user cython

#scp -p A.txt  sharcnet:/project/rohe8957
#sqjobs


#sqsub -q serial -r5m -o setup.log python3 setup.py build_ext --inplace
#sqsub -r 20 -o 3sum.log -q threaded  -n8  --mpp=2G python3 run.py
