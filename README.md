# hadoop-B-Tree

This is a project to handle Big Data queries using Hadoop Mapreduce:

Implemented the indexing of B+ Tree on Hadoop Distributed File System on top of HBase database which basically is efficient in handling queries such as:
 Search by primary key. Time Complexity: BigO(log(n)/(number of clusters)
 Search by a non-primary key. Time Complexity:  BigO(n/(number of cluster))
 Insertion and deletion of a single row in the database. Time Complexity: BigO(log(n))

The indexing is very effective in the scenarios where huge datasets can’t be loaded on RAM so in those cases this method of indexing load just a single node of the dataset at a time. Also, as the size of a single node is also large so it takes fewer memory checks to perform queries.
