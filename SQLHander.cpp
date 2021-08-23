#include <SQLHandler.h>


SQLHandler(zmq::context_t* context){

  context=m_context;
  sock=new zmq::socket_t(m_context, ZMQ_PAIR);
  sock->bind("inproc://SQLThread");

  args = new SQLThread_args(context);
  pthread_create(&(args->thread), NULL, SQLHandler::Thread, args);
  args->running=true;


}


void *SQLHandler::Thread(void *arg){

  SQLThread_args *args = static_cast<SQLThread_args *>(arg);

  zmq::socket_t sock(m_context, ZMQ_PAIR);
  sock.connect("inproc://SQLThread");
  
  zmq::socket_t sock dealer(m_context, ZMQ_DEALER);
  dealer.bind("tcp://*:77777");
  
  zmq::socket_t pub(m_context, ZMQ_PUB);
  pub.bind("tcp://*:77778");
  
  zmq::pollitem_t initems[2]={{sock,0,ZMQ_POLLIN,0},{dealer,0,ZMQ_POLLIN,0}};
  zmq::pollitem_t outitems[3]={{sock,0,ZMQ_POLLOUT,0},{dealer,0,ZMQ_POLLOUT,0}, {pub,0,ZMQ_POLLOUT,0}};
  
  std::map<unsigned long, zmq::message_t> message_queue;
  std::map<unsigned long, zmq::message_t> message_queue_akn;
  std::map<unsigned long, zmq::message_t> sent_messages;

  unsigned long msgid=0;

  while (!args->kill){
    
    if(args->running){
 
      if(mesage_queue.size()>0 || message_queue_akn.size()>0){ //pub messages out
	zmq::poll(&outitems[2], 1, 0);
	if(outitems[2].revents & ZMQ_POLLOUT) {
	  //send pub message from queues
	}

      }//end pub messages out
      
      zmq::poll(&initems[0], 2, 100);


      if(initems[1].revents & ZMQ_POLLIN){ //received asynronous akn


      }

      if(initems[0].revents & ZMQ_POLLIN) { //received new message


	zmq::message_t id;
	zmq::message_t message;

	sock.recv(&id);

	std::istringstream iss(static_cast<char*>(id.data()));

	sock.recv(&message);
	msgid++;
	if(iss.str()=="wf") message_queue[msgid]=message;
	else if(iss.str()=="wa") message_queue_akn[msgid]=message;
	else if(iss.str()=="ws"){

	  pub.send(id, ZMQ_SNDMORE);
	  pub.send(message);

	  zmq::poll(&initems[1], 1, 10000);


	  if(initems[1].revents & ZMQ_POLLIN) {

	    ///receive messages until you get the right one

	  }


	}


	//if message is write syncronous or read do omething straight away else stick on queue;


      }


      for(int i=0; i<sent_messages.size(); i++){ /// loop over sent messages to add to send queue again


      }

    } //end of running

    else usleep(100);

  } // end of while

  pthread_exit(NULL);

}

bool SQLHandler::SendToThread(std::string message, std::string id){

  zmq::message_t msg(message.length()+1);   
  snprintf(msg, message.length()+1 , "%s" , message.c_str());

  zmq::message_t mid(id.length()+1);
  snprintf(mid, id.length()+1 , "%s" , id.c_str());

  bool ret=true;
  ret*=sock->send(mid, ZMQ_SNDMORE);  
  ret*=sock->send(msg);   // need to add timeout fail

  return ret;
}


bool SQLHandler::Write_forget(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  return SendToThread(tmp.str(), "wf"); // need to add timeout fail
}

bool SQLHandler:: Write_asyncronous(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  return SendToThread(tmp.str(), "wa"); //need to add timeoutfial

}

bool SQLHandler:: Write_syncronous(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  if(SendToThread(tmp.str(), "ws") && AknFromThread() ) // need to add timeout fail
    return true;
  }
  else return false;


}

bool SQLHandler:: Read(std::string table, Store& store, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"SELECT "<<fields<<" FROM "<<table;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  if(SendToThread(tmp.str(), "r")){

    zmq::message_t reply;
    sock->recv(msg); //need to add timeout fail
    


  return true;
  
  }
  
  else return false;

}






/*

Thread_args* Utilities::CreateThread(std::string ThreadName,  void (*func)(std::string)){
  Thread_args *args =0;

  if(Threads.count(ThreadName)==0){

    args = new Thread_args(context, ThreadName, func);
    pthread_create(&(args->thread), NULL, Utilities::String_Thread, args);
    args->sock=0;
    args->running=true;
    Threads[ThreadName]=args;
  }

  return args;
}


bool Utilities::KillThread(Thread_args* &args){

  bool ret=false;

  if(args){

    args->running=false;
    args->kill=true;

    pthread_join(args->thread, NULL);
    //delete args;
    //args=0;


  }

  return ret;

}
*/
