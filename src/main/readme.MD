1.  启动virtualbox虚拟机 
1). hdfs namenode -format   选N
    2). start-dfs.sh @ spark1
    第一次启动失败？fail?: 1. copy ClusterID from /usr/local/hadoop/logs/hadoop-root-datanode-spark2.log  to /usr/local/data/datanode/current/VERSION  
          clusterID=
                (spark1 spark2 spark3都要)
		3）、验证启动是否成功：jps、50070端口
		spark1：namenode、datanode、secondarynamenode
		spark2：datanode
		spark3：datanode

2. 1). 启动yarn集群：start-yarn.sh   @ spark1
   2). 验证启动是否成功：jps、8088端口
		spark1：resourcemanager、nodemanager
		spark2：nodemanager
		spark3：nodemanager

3. 1). 分别在三台机器上执行：zkServer.sh start。
   2). 检查ZooKeeper状态：zkServer.sh status。  (先在spark1 spark2 spark3执行上面命令)

4. kafka 
   1). cd /usr/local/kafka/
   1)、在三台机器上分别执行以下命令：nohup bin/kafka-server-start.sh config/server.properties &

5.spark
	1)、在spark目录下的sbin目录
	2)、执行./start-all.sh
	3)、使用jsp和8080端口可以检查集群是否启动成功
	4)、进入spark-shell查看是否正常


6. @spark1
cd /usr/local/
hadoop fs -put test.txt