# Scalable Distributed Query Execution

In this assignment, we implement distributed query execution in a shared-nothing environment.
We start with a single-node execution (see: `coordinator.cpp`), that we want to scale to multiple workers (`worker.cpp`).

Our data is stored externally and partitioned into 100 small chunks, accessible via HTTP.

The worker processes represent elastic ephemeral compute resources, and the coordinator is responsible for managing them and ensuring proper load balancing between them.

## Setup:

Install dependencies (Ubuntu):

```bash
sudo apt update
sudo apt install cmake g++ libcurl4-openssl-dev
```

Build executables:

```bash
mkdir -p cmake-build-debug
cd cmake-build-debug
cmake ..
make
```

## Your Tasks:
In order to solve the assignment, you should edit the following files

`coordinator.cpp (currently works as a single compute node)`

Requirements:
- Establish connection with workers (TCP/IP sockets).
- Distribute files among workers - balanced load
- Collect results, aggregate

`worker.cpp (currently does nothing)`

Requirements:
- Connect to coordinator specified by host and port
- Receive work from coordinator
- Process files (copy code from coordinator)
- Report result

Before starting to implement the solution, consider the following design questions.

## 1.Design Questions:

* How does the leader process know the number of alive workers?
* How can we distribute work "fairly" among workers?
* With what messages do leader - worker communicate?
* How can we detect failed / crashed workers?
* How do we recover when a worker fails?

After implementing your solution, you should also check its scalability.

## 2.Scalability Questions:

Configure the test scripts to spawn 1,2,4,8,16 workers - draw a diagram visualizing how execution time is affected by adding workers.
* What is the limit in scaling? (network, CPU-bound)
* Measure each worker’s load - how is load balancing affected by scale? 
* Could you think of a case when the coordinator would become a bottleneck in such a system?

## 3.Deploy to Azure:

In the next assignment, we will be using Microsoft Azure. We have updated a [tutorial](AZURE_TUTORIAL.md) with detailed instructions on how to deploy and run your solution in Azure.

You can also choose to deploy your solution end-to-end to cloud services, by uploading the assignment data on Azure Blob Storage (like S3) and access it through there. We will soon upload an example of managing blobs on GitLab.

## 4.Tests & GitLab Pipeline

In order to succeed in the assignment, you have to pass the GitLab pipeline, which consists of the three tests you can also find locally.

Basic small-scale test

```bash
./runTest.sh data/test.csv
# should print 5
```

Run a larger workload with:

```bash
./testWorkload.sh https://db.in.tum.de/teaching/ws2324/clouddataprocessing/data/filelist.csv
# should print 275625
```
Simulating a worker failure:

```bash
./testResilience.sh https://db.in.tum.de/teaching/ws2324/clouddataprocessing/data/filelist.csv
# should print 275625
```