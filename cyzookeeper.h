#ifndef __CYZOOKEEPER_H
#define __CYZOOKEEPER_H

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <vector>
#include "zookeeper.h"
#include "zookeeper_log.h"

using namespace std;


class cyzookeeper
{
private:
	zhandle_t* m_zh;
	bool m_state;
	
private:
	void print_type(char *msg, int type);
	void print_state(char *msg, int state);
	void print_error(char *msg, int flag);
public:
	//无回调的初始化
	cyzookeeper(const char *host, int recv_timeout=40000, ZooLogLevel log_level=ZOO_LOG_LEVEL_DEBUG);
	
	//可设置回调的初始化
	cyzookeeper(const char *host, watcher_fn fn,
                int recv_timeout, const clientid_t *clientid, void *context, 
                int flags, ZooLogLevel log_level=ZOO_LOG_LEVEL_DEBUG);
	~cyzookeeper();
	
	//给指定路径设置值
	bool setvalue(const char *path, const char *buffer, int flags=0);

	//给指定路径设置值，值为文件的内容
	bool setvalue_file(const char *path, const char *filename);
	
	//获取指定路径的值
	string getvalue(const char *path);
	
	//获取指定路径的值，并设置回调函数
	string wgetvalue(watcher_fn watcher, void* watcherCtx, const char *path);
	
	//或者指定路径的子节点
	vector<string> getchildren(const char *path);
	
	//获取指定路径的字节点，并设置回调函数
	vector<string> wgetchildren(const char *path, watcher_fn watcher, void* watcherCtx);
	
	//删除指定路径的节点
	bool deletenode(const char *path);
	
	//获取初始化的状态
	bool getstate();

};

#endif
