#!/bin/bash

echo ''
echo '=== Linking files ================================='
if [ ! -f "sydGrid.json" ]
then
    ln -s $1/sydGrid.json
fi
if [ ! -f "bigTwitter.json" ]
then
    ln -s $1/bigTwitter.json
fi
#if [ ! -f "smallTwitter.json" ]
#then
#    ln -s $1/smallTwitter.json
#fi
#if [ ! -f "tinyTwitter.json" ]
#then
#    ln -s $1/tinyTwitter.json
#fi
echo 'Done.'
echo ''

echo '=== Loading modules ==============================='
module load foss/2021a
module load mpi4py/3.0.2-timed-pingpong
module load python/3.9.5

echo '=== Installing dependencies ======================='
pip3 install --user mpi4py
echo ''

echo '=== Run 1 node 1 core ============================='
sbatch scripts/1n1c.slurm
echo ''

echo '=== Run 1 node 8 core ============================='
sbatch scripts/1n8c.slurm
echo ''

echo '=== Run 2 node 8 core ============================='
sbatch scripts/2n8c.slurm
echo ''

echo 'It might take some time, the results can be found in /out.'
echo ''
