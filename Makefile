PYTHON?=python
NUM_OF_PROCS=12#gigabytes

all:    local

local:
	${PYTHON} setup.py build_ext --inplace

run:
	#${PYTHON} run.py
	mpiexec -n 2 ${PYTHON} run.py

annotate:
	cythonize -a threesum.pyx
	open threesum.html

clean:
	@echo Cleaning Prevoius Builds
	@rm -rf ./mikeroher/
	@rm -rf ./build/
	@rm -rf *.so

sharcnet_build:
	sqsub --mpp=1G -q serial -r5m -o setup.log python3 setup.py build_ext --inplace
sharcnet_run:
	#sqsub -r 1d -o 3sum.log -q mpi --nompirun  -n${NUM_OF_PROCS} --mail-end  --mpp=8G mpiexec -n ${NUM_OF_PROCS} python3 run.py
	sqsub -r 2d -o 3sum.log -q mpi -n 240 -N 10 --mpp=8G python3 run.py