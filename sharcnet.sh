echo "THIS WILL SETUP SHARCNET WITH THE NECESSARY MODULES"
module unload intel 12.1.3
module unload intel/tbb/18.0.1
module unload mkl/10.3.9
module load intel/15.0.3
module load python/intel/3.6.0

pip3 install --user pandas # numpy and itertools already installed


#scp -p A.txt  sharcnet:/project/rohe8957
#sqjobs
