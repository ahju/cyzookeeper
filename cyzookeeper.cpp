/******************************************************************* 
 *  Copyright(c) 2001-2016 Changyou 17173
 *  All rights reserved. 
 *   
 *  文件名称: cyzookeeper.cpp
 *  简要描述: 封装zookeeper设置、获取数据的API，使得简单使用
 *            更加容易。目前封装的均为同步的API，异步的API可以直接
 *            使用zookeeper提供的官方API
 *  
 *  
 *   
 *  创建日期: 2016-01-22
 *  作者: 阿祖
 *  说明: 创建文件
 *	1.可以用于同步配置文件
 *  2.更新配置文件时可设置通知
 *  3.用于集群设置，选出master
 *	
 *  存在问题: 
 *	1.当前版本对于回调函数及回调后的重新设置部分没有进行封装，使得
 *    使用起来的学习曲线仍然有些陡
 *  2.对于session过期时没有进行判断
 *
 *  
 *   
 ******************************************************************/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "cyzookeeper.h"


#define MAX_CONF_SIZE 131070
#define START_SIZE 256

/*************************************************
  Function:       cyzookeeper
  Description:    构造函数
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        host:zookeeper服务器列表，形式为ip:port,ip:port
        recv_timeout:超时时间，默认为4000
        log_level:zookeeper日志级别，默认为DEBUG
  Output:         
  Return:         
  Others:         创建的zookeeper句柄不设置回调函数
*************************************************/
cyzookeeper::cyzookeeper(const char *host, int recv_timeout, ZooLogLevel log_level)
{
	if((log_level >= ZOO_LOG_LEVEL_ERROR) && (log_level <= ZOO_LOG_LEVEL_DEBUG))
	{
		zoo_set_debug_level(log_level); 
	}
	m_zh = zookeeper_init(host,NULL, recv_timeout, 0, NULL, 0);  
    if(m_zh ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }
    else
    {
    	int i = 50;
    	while(zoo_state(m_zh) != ZOO_CONNECTED_STATE)
	    {
	        i--;
	        if(0 == i)
	        {
	        	break;
	        }
	        sleep(1);
	    }
    	if(i > 0)
    	{
    		m_state = true;
    	}
    	else
    	{
    		m_state = false;
    	}
    }
}

/*************************************************
  Function:       cyzookeeper
  Description:    构造函数
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        host:zookeeper服务器列表，形式为ip:port,ip:port
        fn:回调函数
        recv_timeout:超时时间，默认为4000
        clientid:参见zookeeper的说明文档
        context:参见zookeeper的说明文档
        flags:参见zookeeper的说明文档
        log_level:zookeeper日志级别，默认为DEBUG
  Output:         
  Return:         
  Others:         创建的zookeeper句柄并设置回调函数
                  使用此方法创建时，需要保证对象一直存在
*************************************************/
cyzookeeper::cyzookeeper(const char *host, watcher_fn fn,
                         int recv_timeout, const clientid_t *clientid, void *context, 
                         int flags, ZooLogLevel log_level)
{
  	if((log_level >= ZOO_LOG_LEVEL_ERROR) && (log_level <= ZOO_LOG_LEVEL_DEBUG))
	{
		zoo_set_debug_level(log_level); 
	}
	m_zh = zookeeper_init(host,fn, recv_timeout, clientid, context, flags);  
    if(m_zh ==NULL)  
    {  
        fprintf(stderr, "Error when connecting to zookeeper servers...\n");  
        exit(EXIT_FAILURE);  
    }
    else
    {
    	int i = 50;
    	while(zoo_state(m_zh)!=ZOO_CONNECTED_STATE)
	    {
	        i--;
	        if(0 == i)
	        {
	        	break;
	        }
	        sleep(1);
	    }
    	if(i > 0)
    	{
    		m_state = true;
    	}
    	else
    	{
    		m_state = false;
    	}
    }
}

/*************************************************
  Function:       ~cyzookeeper
  Description:    析构函数
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
  Output:         
  Return:         
  Others:         
*************************************************/
cyzookeeper::~cyzookeeper()
{
	zookeeper_close(m_zh);	
}

/*************************************************
  Function:       setvalue
  Description:    设置指定路径的值
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
        buffer:路径对应的值
        flags:设置路径即节点的类型，如自编号、临时节点等
  Output:         
  Return:         true-设置成功；false-设置失败
  Others:         若路径不存在则会先创建
*************************************************/
bool cyzookeeper::setvalue(const char *path, const char *buffer, int flags)
{
	string value = "";
	if(NULL != buffer)
	{
		value = buffer;
	}
	//whether exist
	int flag = zoo_exists(m_zh, path, 1, NULL);  
    if (flag == ZNONODE)  
    {
    	char path_buffer[64] = {0};
    	int bufferlen = 64;
    	flag = zoo_create(m_zh, path, value.c_str(), value.length(),  
                          &ZOO_OPEN_ACL_UNSAFE,flags,  
                          path_buffer,bufferlen);  
    }
    else
    {
    	//set value
		flag = zoo_set(m_zh, path, value.c_str(), value.length(), -1);
    }
    
    
	if (flag ==ZOK)  
    {  
        return true;
    } 
    else
    {
    	char msg[64] = {0};
    	sprintf(msg, "%s setvalue failed", path);
    	print_error(msg, flag);
    	return false;
    }
}

/*************************************************
  Function:       setvalue_file
  Description:    设置指定路径的值，值为指定文件的内容
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
        filename:文件名，若文件不在当前路径则需要文件的全路径
  Output:         
  Return:         true-设置成功；false-设置失败
  Others:         若路径不存在则会先创建
*************************************************/
bool cyzookeeper::setvalue_file(const char *path, const char *filename)
{
	struct stat stbuf;
	if(lstat(filename,&stbuf)<0)
	{
		return false;	//文件不存在
	}
	if(stbuf.st_size>MAX_CONF_SIZE)
	{
		return false;	//文件太大
	}
	int fd=open(filename,O_RDONLY);
	if(fd<0)
	{
		return false;	//打开失败
	}
	char *buf=NULL;
	if((buf=(char *)mmap(buf,stbuf.st_size,PROT_READ,MAP_PRIVATE,fd,0))<0)
	{
		close(fd);
		return false;	//映射失败
	}
	return setvalue(path,buf);
}

/*************************************************
  Function:       getvalue
  Description:    获取指定路径的值
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
  Output:         返回路径的值，若无或者异常则返回""
  Return:         
  Others:         不设置回调函数，一次调用返回
*************************************************/
string cyzookeeper::getvalue(const char *path)
{
	if(NULL == path)
	{
		return "";
	}
    int cur_len = START_SIZE;
    string value = "";
    while(true)
    {
    	int get_len = cur_len;
    	char *buf = new char[get_len];
    	memset((void*)buf, 0, get_len);
    	int flag = zoo_get(m_zh,path,0, buf,&get_len,NULL);  
	    if (flag ==ZOK)  
	    {  
	        if(get_len < cur_len)
	        {
	        	value = buf;
	        	delete []buf;
	        	buf = NULL;
	        	return value;
	        }
	        else
	        {
	        	cur_len = cur_len + cur_len;
	        }
	    } 
	    else
	    {
	    	char msg[64] = {0};
    		sprintf(msg, "%s getvalue failed", path);
	    	print_error(msg, flag);
	    	if(NULL != buf)
	    	{
	    		delete []buf;
	    		buf = NULL;
	    	}
	    	return value;
	    }
	    if(cur_len >= MAX_CONF_SIZE)
	    {
	    	if(NULL != buf)
	    	{
	    		delete []buf;
	    		buf = NULL;
	    	}
	    	return value;
	    }
	    if(NULL != buf)
    	{
    		delete []buf;
    		buf = NULL;
    	}
    }
}

/*************************************************
  Function:       wgetvalue
  Description:    获取指定路径的值，并设置回调函数
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        watcher:回调函数
        watcherCtx:回调时传入的值
        path:zookeeper的路径
  Output:         
  Return:         返回路径的值，若无或者异常则返回""
  Others:         使用获取时，需要保证对象一直存在
*************************************************/
string cyzookeeper::wgetvalue(watcher_fn watcher,void* watcherCtx,const char *path)
{
	if(NULL == path)
	{
		return "";
	}
    int cur_len = START_SIZE;
    string value = "";
    while(true)
    {
    	int get_len = cur_len;
    	char *buf = new char[get_len];
    	memset((void*)buf, 0, get_len);
    	int flag = zoo_wget(m_zh, path, watcher, watcherCtx, buf,&get_len,NULL);   
	    if (flag ==ZOK)  
	    {  
	        if(get_len < cur_len)
	        {
	        	value = buf;
	        	delete []buf;
	        	buf = NULL;
	        	return value;
	        }
	        else
	        {
	        	cur_len = cur_len + cur_len;
	        }
	    } 
	    else
	    {
	    	char msg[64] = {0};
    		sprintf(msg, "%s wgetvalue failed", path);
	    	print_error(msg, flag);
	    	if(NULL != buf)
	    	{
	    		delete []buf;
	    		buf = NULL;
	    	}
	    	return value;
	    }
	    if(cur_len >= MAX_CONF_SIZE)
	    {
	    	if(NULL != buf)
	    	{
	    		delete []buf;
	    		buf = NULL;
	    	}
	    	return value;
	    }
	    if(NULL != buf)
    	{
    		delete []buf;
    		buf = NULL;
    	}
    }
}

/*************************************************
  Function:       getchildren
  Description:    获取指定路径的子节点
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
  Output:         
  Return:         返回所有的字节点，若无或者异常返回空结构
  Others:         无回调
*************************************************/
vector<string> cyzookeeper::getchildren(const char *path)
{
	vector<string> children;
	children.empty();
	
	if(NULL == path)
	{
		return children;
	}
	
	struct String_vector strings;  
    struct Stat stat;  
    int flag = zoo_wget_children2(m_zh,path, NULL,NULL, &strings,&stat);  
    if (flag==ZOK)  
    {  
        int32_t i=0;  
        for (;i<strings.count;++i) 
        {
        	children.push_back(strings.data[i]);
        }
        return children;
    }
    else
    {
    	char msg[64] = {0};
    	sprintf(msg, "%s getchildren failed", path);
    	print_error(msg, flag);
    	return children;
    }
}

  
/*************************************************
  Function:       wgetchildren
  Description:    获取指定路径字节点
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
        watcher:回调函数
        watcherCtx:回调时传入的值
  Output:         
  Return:         返回所有的字节点，若无或者异常返回空结构
  Others:         使用获取时，需要保证对象一直存在
*************************************************/
vector<string> cyzookeeper::wgetchildren(const char *path, watcher_fn watcher, void* watcherCtx)
{
	vector<string> children;
	children.empty();
	
	if(NULL == path)
	{
		return children;
	}
	
	struct String_vector strings;  
    struct Stat stat;  
    int flag = zoo_wget_children2(m_zh,path, watcher,watcherCtx, &strings,&stat);  
    if (flag==ZOK)  
    {  
        int32_t i=0;  
        for (;i<strings.count;++i) 
        {
        	children.push_back(strings.data[i]);
        }
        return children;
    }
    else
    {
    	char msg[64] = {0};
    	sprintf(msg, "%s wgetchildren failed", path);
    	print_error(msg, flag);
    	return children;
    }
}

/*************************************************
  Function:       deletenode
  Description:    删除指定节点
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper的路径
  Output:         
  Return:         true-成功；flase-失败
  Others:         
*************************************************/
bool cyzookeeper::deletenode(const char *path)
{
	int flag = zoo_delete(m_zh, path, -1);  
    if(flag==ZOK)  
    {  
        return true;  
    } 
    else
    {
    	char msg[64] = {0};
    	sprintf(msg, "%s delete node failed", path);
    	print_error(msg, flag);
    }
}

  
/*************************************************
  Function:       getstate
  Description:    获取与zookeeper服务器连接状态
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
  Output:         
  Return:         true-连接成功；false-连接失败
  Others:         若路径不存在则会先创建
*************************************************/
bool cyzookeeper::getstate()
{
	return m_state;
}

/*************************************************
  Function:       print_type
  Description:    打印zookeeper消息类型
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:消息提示
        type:zookeeper消息
  Output:         
  Return:         
  Others:         
*************************************************/
void cyzookeeper::print_type(char *msg, int type)
{
	if(ZOO_CREATED_EVENT == type)
	{
		printf("%s:ZOO_CREATED_EVENT\n", msg);
	}
	else if(ZOO_DELETED_EVENT == type)
	{
		printf("%s:ZOO_DELETED_EVENT\n", msg);
	}
	else if(ZOO_CHANGED_EVENT == type)
	{
		printf("%s:ZOO_CHANGED_EVENT\n", msg);
	}
	else if(ZOO_CHILD_EVENT == type)
	{
		printf("%s:ZOO_CHILD_EVENT\n", msg);
	}
	else if(ZOO_SESSION_EVENT == type)
	{
		printf("%s:ZOO_SESSION_EVENT\n", msg);
	}
	else if(ZOO_NOTWATCHING_EVENT == type)
	{
		printf("%s:ZOO_NOTWATCHING_EVENT\n", msg);
	}
	else
	{
		printf("%s:UNKNOW\n", msg);
	}
}

/*************************************************
  Function:       print_state
  Description:    打印zookeeper状态
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:消息提示
        state:zookeeper状态
  Output:         
  Return:         
  Others:         
*************************************************/
void cyzookeeper::print_state(char *msg, int state)
{
	if(ZOO_EXPIRED_SESSION_STATE == state)
	{
		printf("%s:ZOO_EXPIRED_SESSION_STATE\n", msg);
	}
	else if(ZOO_AUTH_FAILED_STATE == state)
	{
		printf("%s:ZOO_AUTH_FAILED_STATE\n", msg);
	}
	else if(ZOO_CONNECTING_STATE == state)
	{
		printf("%s:ZOO_CONNECTING_STATE\n", msg);
	}
	else if(ZOO_ASSOCIATING_STATE == state)
	{
		printf("%s:ZOO_ASSOCIATING_STATE\n", msg);
	}
	else if(ZOO_CONNECTED_STATE == state)
	{
		printf("%s:ZOO_CONNECTED_STATE\n", msg);
	}
	else
	{
		printf("%s:UNKNOW\n", msg);
	}
}

/*************************************************
  Function:       print_error
  Description:    打印zookeeper返回的错误消息
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:消息提示
        flag:zookeeper错误标志
  Output:         
  Return:         
  Others:         
*************************************************/
void cyzookeeper::print_error(char *msg, int flag)
{
	if(ZSYSTEMERROR == flag)
	{
		printf("%s:ZSYSTEMERROR\n", msg);
	}
	else if(ZRUNTIMEINCONSISTENCY == flag)
	{
		printf("%s:A runtime inconsistency was found\n", msg);
	}
	else if(ZDATAINCONSISTENCY == flag)
	{
		printf("%s:A data inconsistency was found\n", msg);
	}
	else if(ZCONNECTIONLOSS == flag)
	{
		printf("%s:Connection to the server has been lost\n", msg);
	}
	else if(ZMARSHALLINGERROR == flag)
	{
		printf("%s:Error while marshalling or unmarshalling data\n", msg);
	}
	else if(ZUNIMPLEMENTED == flag)
	{
		printf("%s:Operation is unimplemented\n", msg);
	}
	else if(ZOPERATIONTIMEOUT == flag)
	{
		printf("%s:Operation timeout\n", msg);
	}
	else if(ZBADARGUMENTS == flag)
	{
		printf("%s:Invalid arguments\n", msg);
	}
	else if(ZINVALIDSTATE == flag)
	{
		printf("%s:Invliad zhandle state\n", msg);
	}
	else if(ZAPIERROR == flag)
	{
		printf("%s:ZAPIERROR\n", msg);
	}
	else if(ZNONODE == flag)
	{
		printf("%s:Node does not exist\n", msg);
	}
	else if(ZNOAUTH == flag)
	{
		printf("%s:Not authenticated\n", msg);
	}
	else if(ZBADVERSION == flag)
	{
		printf("%s:Version conflict\n", msg);
	}
	else if(ZNOCHILDRENFOREPHEMERALS == flag)
	{
		printf("%s:Ephemeral nodes may not have children\n", msg);
	}
	else if(ZNODEEXISTS == flag)
	{
		printf("%s:The node already exists\n", msg);
	}
	else if(ZNOTEMPTY == flag)
	{
		printf("%s:The node has children\n", msg);
	}
	else if(ZSESSIONEXPIRED == flag)
	{
		printf("%s:The session has been expired by the server\n", msg);
	}
	else if(ZINVALIDCALLBACK == flag)
	{
		printf("%s:Invalid callback specified\n", msg);
	}
	else if(ZINVALIDACL == flag)
	{
		printf("%s:Invalid ACL specified\n", msg);
	}
	else if(ZAUTHFAILED == flag)
	{
		printf("%s:Client authentication failed\n", msg);
	}
	else if(ZCLOSING == flag)
	{
		printf("%s:ZooKeeper is closing\n", msg);
	}
	else if(ZNOTHING == flag)
	{
		printf("%s:(not error) no server responses to process\n", msg);
	}
	else if(ZSESSIONMOVED == flag)
	{
		printf("%s:session moved to another server, so operation is ignored\n", msg);
	}
	else
	{
		printf("%s:UNKNOW\n", msg);
	}
}
