#ifndef SQLHANDLER_H
#define SQLHANDLER_H

#include <iostream>
#include <ctime>
#include <unistd.h>
#include <stdio.h>      
#include <stdlib.h>     
#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>
#include <utility> 

struct Message_info{

  Message_info(zmq::message_t in_message){
    message=in_message;
    tries=0;
  }

  zmq::message_t message;
  boost::posix_time::ptime sent_time;
  unsigned int tries;

}

struct SQLThread_args{
  
  SQLThread_args(zmq::context_t* context){ ///< Simple constructor
    kill=false;
    m_context=context;  
    send_timeout=1000;
    recv_timeout=1000;
    identity_length=32; // between 1-255
    resend_period=1;
    retries=60;
    request_tiemout=60;
  }

  virtual ~Thread_args(){ ///< virtual constructor
    kill=true;
    delete sock;
    sock=0;
  }

  zmq::context_t *m_context; ///< ZMQ context used for ZMQ socket creation
  bool kill; ///< Bool flay used to kill the thread
  int send_timeout;
  int recv_timeout;
  int identity_length;
  int resend_period;
  int retries;
  int request_timeout;

};

class SQLHandler{
  
  
 public:
  
  
  SQLHandler(zmq::context_t* context, std::vector<std::string> servers);
  ~SQLHandler();
  bool Write_forget(std::string table, std::string data, std::string fields="", std::string conditions="");
  bool Write_asyncronous(std::string table, std::string data, std::string fields="", std::string conditions="");
  bool Write_syncronous(std::string table, std::string data, std::string fields="", std::string conditions="");
  bool Read(std::string table, Store& store, std::string fields="*", std::string conditions="");
  
 private:


  bool SendToThread(std::string message, std::string id);  
  static void* Thread(void *arg); ///< Thread with args
  
  zmq::context_t* m_context; 
  zmq::socket_t* sock;
  pthread_t thread; ///< Simple constructor underlying thread that interface is built ontop of  
  SQLThread_args* args;  
  
};


#endif
