#Subscriber Ranker
The program accepts a TSV file in the following format 
service\_type subscriber\_id service\_name  timestamp(yyyymmddhhmm)
and calculates the number of unique time stamps for a day for every subscriber id , service type and service name 
and ranks them according to the number of unique timestamps in a day. The rank is represented as fraction of the rank to 
the number of subscribers in a single day. 

The program uses hadoop and is divided into three map-reduce phases. 

###Phase 1 
The TSV file is accepted as input and the number of non-unique records for every subscriber id, service name, service type and timestamp(yyyymmddhhmm) are determined. Output is stored in the intermediate1 directory.

### Phase  2 
From the output of the previous phase, the number of unique records for every subscriber id, service name, service type and day(yyyymmdd) is calculated. Output is stored in the intermediate2 directory.

### Phase 3 
In phase 3 all the records of the same day are group together in the map function and are then sorted according to the the number of unique subscribers in the reduce operation. The rank is also generated during this phase. The output is stored in the output directory. 

##Requirements, Installation and Execution
Hadoop version 2.6.0 
* Add the hadoop bin directory to your PATH.
* Point JAVA\_HOME to the jdk directory. 
* Add tools.jar in JAVA\_HOME/lib/ to HADOOP\_CLASSPATH. 

To compile and run the program, run the shell file compile.sh and pass the input and output folders as arguments. For example 
```
./compile.sh input/ output/ 
```


