/******************************************************************* 
 *  Copyright(c) 2001-2016 Changyou 17173
 *  All rights reserved. 
 *   
 *  �ļ�����: cyzookeeper.cpp
 *  ��Ҫ����: ��װzookeeper���á���ȡ���ݵ�API��ʹ�ü�ʹ��
 *            �������ס�Ŀǰ��װ�ľ�Ϊͬ����API���첽��API����ֱ��
 *            ʹ��zookeeper�ṩ�Ĺٷ�API
 *  
 *  
 *   
 *  ��������: 2016-01-22
 *  ����: ����
 *  ˵��: �����ļ�
 *	1.��������ͬ�������ļ�
 *  2.���������ļ�ʱ������֪ͨ
 *  3.���ڼ�Ⱥ���ã�ѡ��master
 *	
 *  ��������: 
 *	1.��ǰ�汾���ڻص��������ص�����������ò���û�н��з�װ��ʹ��
 *    ʹ��������ѧϰ������Ȼ��Щ��
 *  2.����session����ʱû�н����ж�
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
  Description:    ���캯��
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        host:zookeeper�������б���ʽΪip:port,ip:port
        recv_timeout:��ʱʱ�䣬Ĭ��Ϊ4000
        log_level:zookeeper��־����Ĭ��ΪDEBUG
  Output:         
  Return:         
  Others:         ������zookeeper��������ûص�����
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
  Description:    ���캯��
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        host:zookeeper�������б���ʽΪip:port,ip:port
        fn:�ص�����
        recv_timeout:��ʱʱ�䣬Ĭ��Ϊ4000
        clientid:�μ�zookeeper��˵���ĵ�
        context:�μ�zookeeper��˵���ĵ�
        flags:�μ�zookeeper��˵���ĵ�
        log_level:zookeeper��־����Ĭ��ΪDEBUG
  Output:         
  Return:         
  Others:         ������zookeeper��������ûص�����
                  ʹ�ô˷�������ʱ����Ҫ��֤����һֱ����
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
  Description:    ��������
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
  Description:    ����ָ��·����ֵ
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
        buffer:·����Ӧ��ֵ
        flags:����·�����ڵ�����ͣ����Ա�š���ʱ�ڵ��
  Output:         
  Return:         true-���óɹ���false-����ʧ��
  Others:         ��·������������ȴ���
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
  Description:    ����ָ��·����ֵ��ֵΪָ���ļ�������
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
        filename:�ļ��������ļ����ڵ�ǰ·������Ҫ�ļ���ȫ·��
  Output:         
  Return:         true-���óɹ���false-����ʧ��
  Others:         ��·������������ȴ���
*************************************************/
bool cyzookeeper::setvalue_file(const char *path, const char *filename)
{
	struct stat stbuf;
	if(lstat(filename,&stbuf)<0)
	{
		return false;	//�ļ�������
	}
	if(stbuf.st_size>MAX_CONF_SIZE)
	{
		return false;	//�ļ�̫��
	}
	int fd=open(filename,O_RDONLY);
	if(fd<0)
	{
		return false;	//��ʧ��
	}
	char *buf=NULL;
	if((buf=(char *)mmap(buf,stbuf.st_size,PROT_READ,MAP_PRIVATE,fd,0))<0)
	{
		close(fd);
		return false;	//ӳ��ʧ��
	}
	return setvalue(path,buf);
}

/*************************************************
  Function:       getvalue
  Description:    ��ȡָ��·����ֵ
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
  Output:         ����·����ֵ�����޻����쳣�򷵻�""
  Return:         
  Others:         �����ûص�������һ�ε��÷���
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
  Description:    ��ȡָ��·����ֵ�������ûص�����
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        watcher:�ص�����
        watcherCtx:�ص�ʱ�����ֵ
        path:zookeeper��·��
  Output:         
  Return:         ����·����ֵ�����޻����쳣�򷵻�""
  Others:         ʹ�û�ȡʱ����Ҫ��֤����һֱ����
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
  Description:    ��ȡָ��·�����ӽڵ�
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
  Output:         
  Return:         �������е��ֽڵ㣬���޻����쳣���ؿսṹ
  Others:         �޻ص�
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
  Description:    ��ȡָ��·���ֽڵ�
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
        watcher:�ص�����
        watcherCtx:�ص�ʱ�����ֵ
  Output:         
  Return:         �������е��ֽڵ㣬���޻����쳣���ؿսṹ
  Others:         ʹ�û�ȡʱ����Ҫ��֤����һֱ����
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
  Description:    ɾ��ָ���ڵ�
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        path:zookeeper��·��
  Output:         
  Return:         true-�ɹ���flase-ʧ��
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
  Description:    ��ȡ��zookeeper����������״̬
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
  Output:         
  Return:         true-���ӳɹ���false-����ʧ��
  Others:         ��·������������ȴ���
*************************************************/
bool cyzookeeper::getstate()
{
	return m_state;
}

/*************************************************
  Function:       print_type
  Description:    ��ӡzookeeper��Ϣ����
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:��Ϣ��ʾ
        type:zookeeper��Ϣ
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
  Description:    ��ӡzookeeper״̬
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:��Ϣ��ʾ
        state:zookeeper״̬
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
  Description:    ��ӡzookeeper���صĴ�����Ϣ
  Calls:          
  Called By:      
  Table Accessed: 
  Table Updated:  
  Input:          
        msg:��Ϣ��ʾ
        flag:zookeeper�����־
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
