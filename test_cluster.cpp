/******************************************************************* 
 *  Copyright(c) 2001-2016 Changyou 17173
 *  All rights reserved. 
 *   
 *  文件名称: test_cluster.cpp
 *  简要描述: 使用封装的cyzookeeper进行集群测试，选出master
 *  
 *  
 *   
 *  创建日期: 2016-01-22
 *  作者: 阿祖
 *  说明: 创建文件
 *	1.仅通过ID选择master
 *  
 *	
 *  存在问题: 
 *	
 *   
 ******************************************************************/
#include <iostream>
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
	

cyzookeeper *cyzk1;
sem_t global_job_sem1;
pthread_mutex_t global_job_queue_mux1 = PTHREAD_MUTEX_INITIALIZER;
queue<string> global_job_queue1;

cyzookeeper *cyzk2;
sem_t global_job_sem2;
pthread_mutex_t global_job_queue_mux2 = PTHREAD_MUTEX_INITIALIZER;
queue<string> global_job_queue2;

cyzookeeper *cyzk3;
sem_t global_job_sem3;
pthread_mutex_t global_job_queue_mux3 = PTHREAD_MUTEX_INITIALIZER;
queue<string> global_job_queue3;



void watcher_node_change1(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx)  
{  
    printf("watcher_node_change1 something change\n");  
    
    pthread_mutex_lock(&global_job_queue_mux1);
	global_job_queue1.push(path);
	pthread_mutex_unlock(&global_job_queue_mux1);
	sem_post(&global_job_sem1);
}

void watcher_node_change2(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx)  
{  
    printf("watcher_node_change2 something change\n");  
    
    pthread_mutex_lock(&global_job_queue_mux2);
	global_job_queue2.push(path);
	pthread_mutex_unlock(&global_job_queue_mux2);
	sem_post(&global_job_sem2);
}

void watcher_node_change3(zhandle_t *zh,int type,int state,const char *path,void *watcherCtx)  
{  
    printf("watcher_node_change3 something change\n");  
    
    pthread_mutex_lock(&global_job_queue_mux3);
	global_job_queue3.push(path);
	pthread_mutex_unlock(&global_job_queue_mux3);
	sem_post(&global_job_sem3);
}





void *holder1(void *)
{
	//sleep(3);
	
	int num = 1;
	printf("start holder %d\n", num);
	while(1)
	{
		if(sem_wait(&global_job_sem1)<0)
		{
			continue;
		}
		
		//有活干了,我去取数据...
		string path = "";
		pthread_mutex_lock(&global_job_queue_mux1);
		if(!global_job_queue1.empty())
		{
			path=global_job_queue1.front();
			global_job_queue1.pop();
		}
		pthread_mutex_unlock(&global_job_queue_mux1);
		
		
		if(NULL == cyzk1)
		{
			printf("++++++++++cyzk1 is NULL ++++++++++++\n");
			continue;
		}
		
		vector<string> children = cyzk1->wgetchildren("/session", watcher_node_change1, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
		
		sleep(2);
	}
	return NULL;
}

void *holder2(void *)
{
	//sleep(3);
	
	int num = 2;
	printf("start holder %d\n", num);
	while(1)
	{
		if(sem_wait(&global_job_sem1)<0)
		{
			continue;
		}
		
		//有活干了,我去取数据...
		string path = "";
		pthread_mutex_lock(&global_job_queue_mux2);
		if(!global_job_queue2.empty())
		{
			path=global_job_queue2.front();
			global_job_queue2.pop();
		}
		pthread_mutex_unlock(&global_job_queue_mux2);
		
		
		if(NULL == cyzk2)
		{
			printf("++++++++++cyzk2 is NULL ++++++++++++\n");
			continue;
		}
		vector<string> children = cyzk2->wgetchildren("/session", watcher_node_change2, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
		
		sleep(2);
	}
	return NULL;
}

void *holder3(void *)
{
	//sleep(3);
	
	int num = 3;
	printf("start holder %d\n", num);
	while(1)
	{
		if(sem_wait(&global_job_sem3)<0)
		{
			continue;
		}
		
		//有活干了,我去取数据...
		string path = "";
		pthread_mutex_lock(&global_job_queue_mux3);
		if(!global_job_queue3.empty())
		{
			path=global_job_queue3.front();
			global_job_queue3.pop();
		}
		pthread_mutex_unlock(&global_job_queue_mux3);
		
		if(NULL == cyzk3)
		{
			printf("++++++++++cyzk3 is NULL ++++++++++++\n");
			continue;
		}
		vector<string> children = cyzk3->wgetchildren("/session", watcher_node_change3, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
		
		sleep(2);
	}
	return NULL;
}



void *thread1(void *)
{
	sleep(3);
	int num = 1;
	printf("thread num is %d\n", num);
	while(1)
	{
		const char* host = "10.6.104.211:2181,10.6.104.215:2181,10.6.104.182:2181";  
	    int timeout = 3000;
	     
	    cyzk1 = new cyzookeeper(host,timeout);
	    char path[16] = {0};
	    sprintf(path,"/session/%d", num);
	    cyzk1->setvalue(path,"session1",ZOO_EPHEMERAL);
	    
	    
	    vector<string> children = cyzk1->wgetchildren("/session", watcher_node_change1, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
	    
	    sleep(60);
	    delete cyzk1;
	    cyzk1 = NULL;
	    sleep(60);
	}
}

void *thread2(void *)
{
	sleep(3);
	int num = 2;
	printf("thread num is %d\n", num);
	
	while(1)
	{
		const char* host = "10.6.104.211:2181,10.6.104.215:2181,10.6.104.182:2181";  
	    int timeout = 3000;
	     
	    cyzk2 = new cyzookeeper(host,timeout);
	    char path[16] = {0};
	    sprintf(path,"/session/%d", num);
	    cyzk2->setvalue(path,"session2",ZOO_EPHEMERAL);
	    
	    vector<string> children = cyzk2->wgetchildren("/session", watcher_node_change2, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
	    sleep(60);
	    delete cyzk2;
	    cyzk2 = NULL;
	    sleep(60);
	}
}

void *thread3(void *)
{
	sleep(3);
	int num = 3;
	printf("thread num is %d\n", num);
	
	while(1)
	{
		const char* host = "10.6.104.211:2181,10.6.104.215:2181,10.6.104.182:2181";  
	    int timeout = 3000;
	     
	    cyzk3 = new cyzookeeper(host,timeout);
	    char path[16] = {0};
	    sprintf(path,"/session/%d", num);
	    cyzk3->setvalue(path,NULL,ZOO_EPHEMERAL);
	    
	    vector<string> children = cyzk3->wgetchildren("/session", watcher_node_change3, NULL);
	    bool issmallest = true;
	    for(int i=0; i<children.size(); i++)
	    {
	    	string strnum = children[i];
	    	if(atoi(strnum.c_str()) < num)
	    	{
	    		issmallest = false;
	    		break;
	    	}
	    }
	    if(issmallest)
	    {
	    	printf("*************thread %d is the smallest and be the master***************\n", num);
	    }
	    sleep(60);
	    delete cyzk3;
	    cyzk3 = NULL;
	    sleep(60);
	}
}


void test_cluster()
{
	const char* host = "10.6.104.211:2181,10.6.104.215:2181,10.6.104.182:2181";  
    int timeout = 30000;
     
    cyzookeeper *cyzk = new cyzookeeper(host,timeout);
    
  	if(!cyzk->getstate())
  	{
  		printf("init zookeeper failed!\n");
  		return;
  	}
  	
  	char adsystem[] = "/session";
  	
  	cyzk->setvalue(adsystem, "this is adsystem");
	
	pthread_t holder1_pid;
	if(pthread_create(&holder1_pid,NULL,holder1,NULL))
	{
		exit(1);
	}
	sleep(3);
	pthread_t holder2_pid;
	if(pthread_create(&holder2_pid,NULL,holder2,NULL))
	{
		exit(1);
	}
	sleep(3);
	pthread_t holder3_pid;
	if(pthread_create(&holder3_pid,NULL,holder3,NULL))
	{
		exit(1);
	}
	//sleep(3);
	pthread_t thread3_pid;
	if(pthread_create(&thread3_pid,NULL,thread3,NULL))
	{
		exit(1);
	}
	sleep(3);
	
	pthread_t thread2_pid;
	if(pthread_create(&thread2_pid,NULL,thread2,NULL))
	{
		exit(1);
	}
	sleep(3);
	pthread_t thread1_pid;
	if(pthread_create(&thread1_pid,NULL,thread1,NULL))
	{
		exit(1);
	}
	pthread_join(thread3_pid, NULL);
	pthread_join(thread2_pid, NULL);
	pthread_join(thread2_pid, NULL);
	delete cyzk;
	cyzk = NULL;
}