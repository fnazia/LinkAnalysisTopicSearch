# Hypertext Induced Title Search for Wikipedia Topic Links
This is an implementation of link analysis algorithm Hypertext Induced Title Search to search Wikipedia topics and rank the links in order according to both authority score and hub score.
The program is run on Apache Spark with 10 worker nodes. The input files are loaded in and fetched from HDFS and YARN has been used for resource management.
