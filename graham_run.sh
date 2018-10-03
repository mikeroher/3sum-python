#!/bin/bash
#SBATCH --account=def-ikotsire
#SBATCH --ntasks 64
#SBATCH --mem-per-cpu 4G       # memory; default unit is megabytes
#SBATCH --time=6-0:0
mpiexec -np 64 python3 run.py