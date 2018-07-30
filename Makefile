PYTHON?=python

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

sharcnet_build:
	sqsub --mpp=1G -q serial -r5m -o setup.log python3 setup.py build_ext --inplace
sharcnet_run:
	sqsub -r 20 -o 3sum.log -q threaded  -n8  --mpp=2G python3 run.py