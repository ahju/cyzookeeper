# cyzookeeper
使用C++对zookeeper的C API进行了简单的封装，仅封装了同步API

## 文件说明
1.cyzookeeper.h     封装zookeeper API的头文件<br>
2.cyzookeeper.cpp   封装zookeeper API的实现文件<br>
3.test_conf.cpp     测试配置文件同步的实现文件<br>
4.test_cluster.cpp  测试集群管理的实现文件，文件中根据ID选择master<br>
5.test_zk.cpp       测试cyzookeeper的工程实现文件<br>
6.zookeeper_DEBUG.mk 测试工程的makefile文件<br>
7.zookeeper.prj     测试cyzookeeper的工程文件<br>
8.adsvr.conf        测试配置文件同步的配置文件<br>

## 环境说明
1.zookeeper的基本概念可以网上搜，非常多，最简单的理解即为zookeeper可以维护一棵树，数的每个节点都可以有值，可以添加删除节点，并监控节点值的变化。<br>
  1）对于配置文件同步功能可以将整个配置文件的内容作为一个节点的值，也可每一项分别作为不同节点的值，监控或者获取对应节点的值即可实现同步；<br>  
  2）对于集群管理可以每一个机器都在zookeeper的某个节点下建立临时节点，节点存在即集群的服务器存在，通过监控节点的变化获得集群状态<br>
2.编译及使用前需要编译安装zookeeper C API作为支撑库，详细可网上查询<br>

## 使用说明
1.创建cyzookeeper对象，传入zookeper服务器地址及端口<br>
2.使用setvalue设置节点的值<br>
3.使用getvalue或者wgetvalue获取节点的值值<br>
4.使用getchildren或者wgetchildren获取节点的字节点<br>
5.编译时头文件需要添加-I/usr/local/include/c-client-src路径，使用的库需要添加/usr/local/lib/libzookeeper_mt.so，其中“/usr/local/”路径为使用默认路径编译和安装zookeeper 的C API情况下的，若安装时有修改则使用修改的路径
