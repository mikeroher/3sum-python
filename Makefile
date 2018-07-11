PYTHON?=python

all:    local

local:
	${PYTHON} setup.py build_ext --inplace

run:
	${PYTHON} run.py

clean:
	@echo Cleaning Prevoius Builds
	@rm -rf ./mikeroher/
	@rm -rf ./build/