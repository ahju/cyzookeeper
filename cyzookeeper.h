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
	//�޻ص��ĳ�ʼ��
	cyzookeeper(const char *host, int recv_timeout=40000, ZooLogLevel log_level=ZOO_LOG_LEVEL_DEBUG);
	
	//�����ûص��ĳ�ʼ��
	cyzookeeper(const char *host, watcher_fn fn,
                int recv_timeout, const clientid_t *clientid, void *context, 
                int flags, ZooLogLevel log_level=ZOO_LOG_LEVEL_DEBUG);
	~cyzookeeper();
	
	//��ָ��·������ֵ
	bool setvalue(const char *path, const char *buffer, int flags=0);

	//��ָ��·������ֵ��ֵΪ�ļ�������
	bool setvalue_file(const char *path, const char *filename);
	
	//��ȡָ��·����ֵ
	string getvalue(const char *path);
	
	//��ȡָ��·����ֵ�������ûص�����
	string wgetvalue(watcher_fn watcher, void* watcherCtx, const char *path);
	
	//����ָ��·�����ӽڵ�
	vector<string> getchildren(const char *path);
	
	//��ȡָ��·�����ֽڵ㣬�����ûص�����
	vector<string> wgetchildren(const char *path, watcher_fn watcher, void* watcherCtx);
	
	//ɾ��ָ��·���Ľڵ�
	bool deletenode(const char *path);
	
	//��ȡ��ʼ����״̬
	bool getstate();

};

#endif
