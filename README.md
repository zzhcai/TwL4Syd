# Summary

This project implements a simple, parallelized application leveraging the University of Melbourne HPC facility SPARTAN. We analyses, using a large Twitter dataset and a grid/mesh (shown below) for Sydney, to identify the insights on languages used in making Tweets.

![](mesh.png)

Our application should be run once to search the `bigTwitter.json` file on each of the following resources:
- 1 node and 1 core;
- 1 node and 8 cores;
- 2 nodes and 8 cores (with 4 cores per node).

Return the final results and the time to run the job itself.

# Run
```
sudo chmod +x setup.sh run.sh
./setup.sh
./run.sh
```
