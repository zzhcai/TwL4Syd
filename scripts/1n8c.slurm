#!/bin/bash

#SBATCH -o output/1n8c.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH -t 10:00

cd ${SLURM_SUBMIT_DIR}

time mpiexec -n 8 python3 count.py bigTwitter.json sydGrid.json
