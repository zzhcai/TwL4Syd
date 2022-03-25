#!/bin/bash

#SBATCH -o output/2n8c.out
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH -t 1:00

cd ${SLURM_SUBMIT_DIR}

srun -n 8 python3 count.py bigTwitter.json sydGrid.json

my-job-stats -a -n -s

