#include <iostream>
#include <stdio.h>  
#include <string.h> 
#include <time.h>
#include <unistd.h>
#include <semaphore.h>
//#include <signal.h>
#include <queue>
#include "zookeeper.h"
#include "zookeeper_log.h"
#include "cyzookeeper.h"
	
using namespace std;

cyzookeeper *cyzk;
sem_t global_job_sem;
pthread_mutex_t global_job_queue_mux = PTHREAD_MUTEX_INITIALIZER;
queue<string> global_job_queue;


void print_type(int type)
{
	if(ZOO_CREATED_EVENT == type)
	{
		printf("type=ZOO_CREATED_EVENT\n");
	}
	else if(ZOO_DELETED_EVENT == type)
	{
		printf("type=ZOO_DELETED_EVENT\n");
	}
	else if(ZOO_CHANGED_EVENT == type)
	{
		printf("type=ZOO_CHANGED_EVENT\n");
	}
	else if(ZOO_CHILD_EVENT == type)
	{
		printf("type=ZOO_CHILD_EVENT\n");
	}
	else if(ZOO_SESSION_EVENT == type)
	{
		printf("type=ZOO_SESSION_EVENT\n");
	}
	else if(ZOO_NOTWATCHING_EVENT == type)
	{
		printf("type=ZOO_NOTWATCHING_EVENT\n");
	}
	else
	{
		printf("type=UNKNOW\n");
	}
}

void print_state(int state)
{
	if(ZOO_EXPIRED_SESSION_STATE == state)
	{
		printf("state=ZOO_EXPIRED_SESSION_STATE\n");
	}
	else if(ZOO_AUTH_FAILED_STATE == state)
	{
		printf("state=ZOO_AUTH_FAILED_STATE\n");
	}
	else if(ZOO_CONNECTING_STATE == state)
	{
		printf("state=ZOO_CONNECTING_STATE\n");
	}
	else if(ZOO_ASSOCIATING_STATE == state)
	{
		printf("state=ZOO_ASSOCIATING_STATE\n");
	}
	else if(ZOO_CONNECTED_STATE == state)
	{
		printf("state=ZOO_CONNECTED_STATE\n");
	}
	else
	{
		printf("state=UNKNOW\n");
	}
}

void print_error(int flag)
{
	if(ZSYSTEMERROR == flag)
	{
		printf("error=ZSYSTEMERROR\n");
	}
	else if(ZRUNTIMEINCONSISTENCY == flag)
	{
		printf("error=A runtime inconsistency was found\n");
	}
	else if(ZDATAINCONSISTENCY == flag)
	{
		printf("error=A data inconsistency was found\n");
	}
	else if(ZCONNECTIONLOSS == flag)
	{
		printf("error=Connection to the server has been lost\n");
	}
	else if(ZMARSHALLINGERROR == flag)
	{
		printf("error=Error while marshalling or unmarshalling data\n");
	}
	else if(ZUNIMPLEMENTED == flag)
	{
		printf("error=Operation is unimplemented\n");
	}
	else if(ZOPERATIONTIMEOUT == flag)
	{
		printf("error=Operation timeout\n");
	}
	else if(ZBADARGUMENTS == flag)
	{
		printf("error=Invalid arguments\n");
	}
	else if(ZINVALIDSTATE == flag)
	{
		printf("error=Invliad zhandle state\n");
	}
	else if(ZAPIERROR == flag)
	{
		printf("error=ZAPIERROR\n");
	}
	else if(ZNONODE == flag)
	{
		printf("error=Node does not exist\n");
	}
	else if(ZNOAUTH == flag)
	{
		printf("error=Not authenticated\n");
	}
	else if(ZBADVERSION == flag)
	{
		printf("error=Version conflict\n");
	}
	else if(ZNOCHILDRENFOREPHEMERALS == flag)
	{
		printf("error=Ephemeral nodes may not have children\n");
	}
	else if(ZNODEEXISTS == flag)
	{
		printf("error=The node already exists\n");
	}
	else if(ZNOTEMPTY == flag)
	{
		printf("error=The node has children\n");
	}
	else if(ZSESSIONEXPIRED == flag)
	{
		printf("error=The session has been expired by the server\n");
	}
	else if(ZINVALIDCALLBACK == flag)
	{
		printf("error=Invalid callback specified\n");
	}
	else if(ZINVALIDACL == flag)
	{
		printf("error=Invalid ACL specified\n");
	}
	else if(ZAUTHFAILED == flag)
	{
		printf("error=Client authentication failed\n");
	}
	else if(ZCLOSING == flag)
	{
		printf("error=ZooKeeper is closing\n");
	}
	else if(ZNOTHING == flag)
	{
		printf("error=(not error) no server responses to process\n");
	}
	else if(ZSESSIONMOVED == flag)
	{
		printf("error=session moved to another server, so operation is ignored\n");
	}
	else
	{
		printf("error=UNKNOW\n");
	}
}


void watcher_path_change(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx)  
{  
    print_type(type);
	print_state(state);
    printf("watcher_path_change something change\n");  
    printf("path:%s\n",path);  
    printf("watcherCtx:%s\n",(char*)watcherCtx);
    
    pthread_mutex_lock(&global_job_queue_mux);
	global_job_queue.push(path);
	pthread_mutex_unlock(&global_job_queue_mux);
	sem_post(&global_job_sem);
}



void *pathholder1(void *)
{
	sleep(3);
	
	int i = 1;
	while(1)
	{
		if(sem_wait(&global_job_sem)<0)
		{
			continue;
		}
		
		//有活干了,我去取数据...
		string path = "";
		pthread_mutex_lock(&global_job_queue_mux);
		if(!global_job_queue.empty())
		{
			path=global_job_queue.front();
			global_job_queue.pop();
		}
		pthread_mutex_unlock(&global_job_queue_mux);
		
		
		long ti = 0;
	    time(&ti);
	    char value[64];
	     
	    memset((void*)value, 0, 64);
	    sprintf(value,"path wget value at %ld",ti);
		string value1 = cyzk->wgetvalue(watcher_path_change,(void*)value, path.c_str());
    	printf("path:%s values=%s\n",path.c_str(),value1.c_str());
		
		sleep(2);
	}
	return NULL;
}

void test_conf()
{
	if(sem_init(&global_job_sem,0,0)<0)
	{
		exit(1);
	}
	
    const char* host = "10.6.104.211:2181,10.6.104.215:2181,10.6.104.182:2181";  
    int timeout = 3000;
     
    cyzk = new cyzookeeper(host,timeout);
    
  	if(!cyzk->getstate())
  	{
  		printf("init zookeeper failed!\n");
  		return;
  	}
  	
  	char adsystem[] = "/adsystem";
  	char adsvr[] = "/adsystem/adsvr";
  	char dbsvr[] = "/adsystem/dbsvr";
  	char logsvr[] = "/adsystem/logsvr";
  	
  	
  	cyzk->setvalue(adsystem, "this is adsystem");
  	cyzk->setvalue_file(adsvr, "adsvr.conf");
  	cyzk->setvalue(dbsvr, "this is dbsvr");
  	cyzk->setvalue(logsvr, "this is logsvr");
  	
  	string adsystem_value = cyzk->getvalue(adsystem);
  	printf("adsystem value:%s\n", adsystem_value.c_str());
  	
  	string adsvr_value = cyzk->getvalue(adsvr);
  	printf("adsvr value:%s\n", adsvr_value.c_str());
  	
  	char tmp[] = {"get dbsvr data"};
  	string dbsvr_value = cyzk->wgetvalue(watcher_path_change,tmp,dbsvr);
  	printf("dbsvr value:%s\n", adsvr_value.c_str());
  	
  	char tmp1[] = {"get logsvr data"};
  	string logsvr_value = cyzk->wgetvalue(watcher_path_change,tmp1,logsvr);
  	printf("adsystem value:%s\n", logsvr_value.c_str());
  	
  	vector<string> children = cyzk->getchildren(adsystem);
  	printf("%s children:",adsystem);
  	for(int i=0; i<children.size(); i++)
  	{
  		printf("%s\t",children[i].c_str());
  	}
  	printf("\n");
  	
  	
  	pthread_t thread1_pid;
	if(pthread_create(&thread1_pid,NULL,pathholder1,NULL))
	{
		exit(1);
	}
  	
  	for(int i=0; i<100; i++)
  	{
  		long ti = 0;
	    time(&ti);
	    char value[64];
	     
	    memset((void*)value, 0, 64);
	    sprintf(value,"dbsvr%d",i);
	    cyzk->setvalue(dbsvr,value);
	    
	    sleep(10);
	    
	    time(&ti);
	    memset((void*)value, 0, 64);
	    sprintf(value,"logsvr%d",i);
	    cyzk->setvalue(logsvr,value);
	    
	    sleep(10);
  	}

}