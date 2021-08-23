#ifndef SQLHANDLER_H
#define SQLHANDLER_H

struct SQLThread_args{
  
  SQLThread_args(zmq::context_t* context){ ///< Simple constructor
    kill=false;
    m_context=context;  
  }

  virtual ~Thread_args(){ ///< virtual constructor
    running =false;
    kill=true;
    delete sock;
    sock=0;
  }
  
  zmq::context_t *m_context; ///< ZMQ context used for ZMQ socket creation
  bool running; ///< Bool flag to tell the thread to run (if not set thread goes into wait cycle
  bool kill; ///< Bool flay used to kill the thread
  
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
  
  zmq::context_t* m_context; 
  zmq::socket_t* sock;
  pthread_t thread; ///< Simple constructor underlying thread that interface is built ontop of  
  static void* Thread(void *arg); ///< Thread with args
  SQLThread_args* args;  

};


#endif
