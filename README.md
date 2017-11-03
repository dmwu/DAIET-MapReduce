## DAIET: Data Aggregation In nETwork
### Overview:
Many scalable data center applications follow a partition-aggregate pattern where data and computations are distributed among many servers and their partial results are exchanged over the network and aggregated to produce the final output. For these workloads, the network communication costs are often one of the dominant scalability bottlenecks.

To improve performance, we present \DAIET, a system that performs data aggregation along network paths by leveraging emerging programmable network devices. By offloading part of the aggregation task to the networking hardware, \DAIET not only reduces the traffic at each network device, but also allows to reduce the computation done by the CPU, effectively decreasing job completion times.
We describe a preliminary prototype of \DAIET using P4 to perform in-network aggregation for MapReduce-based applications.
Experimental results show that our approach achieves around 88\% data reduction ratio and a similar decrease in the reducer running time, without requiring severe application-level modifications.

### Resources
* DAIET project website: [sands.kaust.edu.sa/daiet/](http://sands.kaust.edu.sa/daiet/)
* HotNets 2017 paper: 
  
  *Amedeo Sapio, Ibrahim Abdelaziz, Abdulla Aldilaijan, Marco Canini, and Panos Kalnis <br>
  **In-Network Computation is a ~~Dumb~~ Idea Whose Time Has Come** <br>
  In Proceedings of the 16th ACM Workshop on Hot Topics in Networks. ACM, 2017.*

### Changes
The main changes from the original [simpleMR](https://github.com/wangchangli/simplemr) are:
* Communication Layer (TCP to UDP)
* Message Serialization 
* Sorting intermediate results

### How to build
```bash
cd simplemr
mvn package
chmod +x dist/bin/*
cd ..
```
### How to run
See Detailed Steps: [https://github.com/AmedeoSapio/DAIET](https://github.com/AmedeoSapio/DAIET)


