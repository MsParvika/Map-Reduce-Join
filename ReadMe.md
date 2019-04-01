# Set-up:
1: Download the Hadoop 2.7.3 Package
  Command: wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz
  Command: tar -xvf hadoop-2.7.3.tar.gz
  
2: Add the Hadoop Path in the bash file (/home/user/.bashrc)
  Command to Check Installation: hadoop version
  
3: Edit the Hadoop Configuration files.
  Command: cd hadoop-2.7.3/etc/hadoop/
  
4. Edit hadoop-env.sh and add the Java Path


# Command to execute:
1.Create jar of the java file

2.sudo -u <username> <path_of_hadoop> jar <name_of_jar> <class_name_with_main_function> <HDFSinputFile> <HDFSoutputFile>
  
```
sudo -u hduser /home/user/Downloads/hadoop-2.7.3/bin/hadoop jar abs.jar mapReduce hdfs://localhost:50007/input/sample.txt hdfs://localhost:50007/output
```

# What is what?
MapReduce is a computation that decomposes large manipulation jobs into individual tasks that can be executed in parallel across a cluster of servers. The results of tasks can be joined together to compute final results.

Driver
(Public, void, static, or main; this is the entry point.)
In this class a job named "mapReduce" is created and run. 

Map
Mapper gets the entire input file as input and converts it line-by-line into another set of data, where individual elements are broken down into tuples (Key-Value pair).
Each field in the tuple is separated by ", ". First field gives name of table and second gives the join column value, which is used as key where the value for it is the entire line.

Reduce
Reducer takes the output(key and list of values/tuples) from Mapper as an input and combines those data tuples into a list. It separates out tuples related to each table and place them in 2 separate lists. The combinations of elements from these two lists are then written to the output (rows in equijoin).
