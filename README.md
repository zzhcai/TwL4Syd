# Summary

This project implements a simple, parallelized application leveraging the University of Melbourne HPC facility SPARTAN. We analyses, using a large Twitter dataset and a grid/mesh (shown below) for Sydney, to identify the insights on languages used in making Tweets.

<img src="mesh.png" width="300">

Our application should be run once to search the `bigTwitter.json` file on each of the following resources:

- 1 node and 1 core;
- 1 node and 8 cores;
- 2 nodes and 8 cores (with 4 cores per node).

Return the final results and the time to run the job itself.

# Repo Structure

```
├── data
├──── bigTwitter.json       # not uploaded
├──── sydGrid.json
├── output                  # output result from SLURM job
├────
├── scripts
├──── 1n1c.slurm            # 1 node 1 core
├──── 1n8c.slurm            # 1 node 8 core
├──── 2n8c.slurm            # 2 node 8 cores
├── .gitattributes
├── .gitignore
├── LICENSE
├── README.md
├── count.py                # main
├── utils.py
├── mesh.png
├── requirements.txt
├── run.sh                  # application entrypoint
└── setup.sh                # prerequisite
```

# Run

```
sudo chmod +x setup.sh run.sh
sudo ./setup.sh
./run.sh
```
