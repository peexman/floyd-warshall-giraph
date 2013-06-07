floyd-warshall-giraph
=====================

Floyd-Warshall basic implementation for Apache Giraph / Hadoop 

from HADOOP_HOME:

bin/hadoop jar floydwarshall.jar net.graph.shortestpath.FloydWarshall [<path_to_fw_properties>]

fw.properties:
in_edges=hdfs path to edges, ex: /user/<user>/...
in_vertices=hdfs path to vertices, ex: /user/<user>/...
min_worker=min worker to start 
max_worker=max worker to use
num_thread=threads per worker
zk_list=comma separated list of <server:port>
out_path=hdfs path to output /user/<user>/...
key_space=key space, depends on the number of vertices