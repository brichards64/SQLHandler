#include <SQLHandler.h>


SQLHandler(zmq::context_t* context){

  context=m_context;
  sock=new zmq::socket_t(m_context, ZMQ_PAIR);
  sock->bind("inproc://SQLThread");
  sock.setsockopt(ZMQ_RCVTIMEO, 1000);
  sock.setsockopt(ZMQ_SNDTIMEO, 1000);

  args = new SQLThread_args(context);
  pthread_create(&(args->thread), NULL, SQLHandler::Thread, args);
  

}


void *SQLHandler::Thread(void *arg){

  SQLThread_args *args = static_cast<SQLThread_args *>(arg);

  boost::posix_time::time_duration resen_period=boost::posix_time::seconds(args->resend_period);
  boost::posix_time::time_duration requet_timeout=boost::posix_time::seconds(args->reques_timeout);

  char identity[srgs->identity_length];
  srand( (unsigned) time(NULL) * getpid());
  char alphanum[] = "qwertyuiopasdfghjklzxcvbnm1234567890QWERTYUIOPASDFGHJKLZXCVBNM"
  for (int i = 0; i < identity_length; ++i) identity[i]= alphanum[rand() % (sizeof(alphanum) - 1)];
    
  zmq::socket_t sock(args->m_context, ZMQ_PAIR);
  sock.connect("inproc://SQLThread");
  sock.setsockopt(ZMQ_RCVTIMEO, args->recv_timeout);
  sock.setsockopt(ZMQ_SNDTIMEO, args->send_timeout);
  
  zmq::socket_t sock dealer(args->m_context, ZMQ_DEALER);
  dealer.bind("tcp://*:77777");
  dealer.setsockopt(ZMQ_RCVTIMEO, args->recv_timeout);
  dealer.setsockopt(ZMQ_SNDTIMEO, args->send_timeout);
  dealer.setsockopt(ZMQ_IDENTITY, identity);
  
  zmq::socket_t pub(args->m_context, ZMQ_PUB);
  pub.bind("tcp://*:77778");
  pub.setsockopt(ZMQ_RCVTIMEO, args->recv_timeout);
  pub.setsockopt(ZMQ_SNDTIMEO, args->send_timeout);
 
  zmq::pollitem_t initems[2]={{sock,0,ZMQ_POLLIN,0},{dealer,0,ZMQ_POLLIN,0}};
  zmq::pollitem_t outitems[3]={{sock,0,ZMQ_POLLOUT,0},{dealer,0,ZMQ_POLLOUT,0}, {pub,0,ZMQ_POLLOUT,0}};
  
  std::map<unsigned int, zmq::message_t> message_queue;
  std::map<unsigned int, Message_info> message_queue_akn;
  std::map<unsigned int, Message_info> sent_messages;

  unsigned int msgid=0;

  while (!args->kill){
    
      if(mesage_queue.size()>0 || message_queue_akn.size()>0){ //pub messages out
	zmq::poll(&outitems[2], 1, 0);
	if(outitems[2].revents & ZMQ_POLLOUT) {  //sending out write pub messages

	  if(mesage_queue.size()>0){
	    zmq::message_t msg_identity(sizeof(identity));
	    memcpy(msg_identity.data(), identity, sizeof(identity));
	    zmq::message_t id(sizeof(unsigned int));
	    std::map<unsigned int, zmq::message_t>::iterator it=mesage_queue.begin();
	    memcpy(id.data(), &(it->first), sizeof(unsigned int));
	    if(pub.send(msg_identity, ZMQ_SNDMORE) && pub.send(id, ZMQ_SNDMORE) && pub.send(it->second)) message_queue.erase(it);
	    else std::cout<<"error sending message"<<std::endl;
	  }


	  if(mesage_queue_akn.size()>0){
	    zmq::message_t msg_identity(sizeof(identity));
	    memcpy(msg_identity.data(), identity, sizeof(identity));
	    zmq::message_t id(sizeof(unsigned int));
	    std::map<unsigned int, Message_info>::iterator it=mesage_queue_akn.begin();
	    memcpy(id.data(), &(it->first), sizeof(unsigned int));
	    if(pub.send(msg_identity, ZMQ_SNDMORE) && pub.send(id, ZMQ_SNDMORE) && pub.send(it->second.message)){
	      it->second.tries++;
	      it->second.sent_time=boost::posix_time::second_clock::universal_time();
	      sent_messages[it->first]= it->second;
	      message_queue_akn.erase(it);
	    }
	    else std::cout<<"error sending message"<<std::endl;
	  }
	  
	}	  
      }//end pub messages out
      
      
      zmq::poll(&initems[0], 2, 100);
      
      
      if(initems[1].revents & ZMQ_POLLIN){ //received asynronous akn
	
	zmq::message_t id;
	if(dealer.recv(&id) && !(id.more())){
	  unsigned int tmpid=*(reinterpret_cast<unsigned int*>(id.data()));
	  if(sent_messages.cout(tmpid>0)) sent_messages.erase(tmpid);
	}
	else if(id.more()){
	  std::cout<<"receiving unexpected multipart message"<<std::endl;
	  zmq::message_t tmp;
	  dealer.recv(&tmp);
	  while(tmp.more()) dealer.recv(&tmp);
	}
	else std::cout<<"error receiving aknowledgement"<<std::endl;

      }



      if(initems[0].revents & ZMQ_POLLIN) { //received new message from DAQ
		
	zmq::message_t type;
	zmq::message_t message;
     
	if(sock.recv(&type) && type.more()){	  

	  std::istringstream iss(static_cast<char*>(type.data()));
       
	  if(sock.recv(&message) && !(message.more())){
	    msgid++;
	    if(iss.str()=="wf") message_queue[msgid]=message;
	    else if(iss.str()=="wa") message_queue_akn[msgid]=Message_info(message);
	    else if(iss.str()=="ws" || iss.str()=="r"){ //write syncronous so send straight away
	      
	      //poll? wont block on pub but might fall over? probably worth adding
	      zmq::message_t id(sizeof(unsigned int));
	      memcpy(id.data(), &msgid, sizeof(unsigned int));

	      if(pub.send(id, ZMQ_SNDMORE) && pub.send(message)){
		
		boost::posix_time::ptime start = boost::posix_time::second_clock::universal_time();
		boost::posix_time::time_duration lapse= boost::posix_time::seconds(10);

		while (!(lapse.is_negative())){   ///receive messages until you get the right one
		  
		  lapse=request_timeout - (boost::posix_time::second_clock::universal_time() - start);
		  
		  zmq::poll(&initems[1], 1, 100);
		  
		  if(initems[1].revents & ZMQ_POLLIN) { //receiving akn
		    
		    zmq::message_t aknid;
		    if(dealer.recv(&aknid)){
		      
		      if(iss.str()=="ws" && !(aknid.more())){
			unsigned int tmpid=*(reinterpret_cast<unsigned int*>(aknid.data()));
			if(aknid==msgid) waiting=false;
			else if(sent_messages.cout(tmpid>0)) sent_messages.erase(tmpid);
		      }
		      else if(iss.str()=="r" && aknid.more()){
			unsigned int tmpid=*(reinterpret_cast<unsigned int*>(aknid.data()));
			if(aknid==msgid){

			  //receive a message in some format

			}
			else{
			  std::cout<<"receiving unexpected multipart message"<<std::endl;
			  zmq::message_t tmp;
			  deler.recv(&tmp);
			  while(tmp.more()) dealer.recv(&tmp);
			}
		      }
		      else if(aknid.more()){
			std::cout<<"receiving unexpected multipart message"<<std::endl;
			zmq::message_t tmp;
			deler.recv(&tmp);
			while(tmp.more()) dealer.recv(&tmp);
		      }
		      else std::cout<<"error receiving aknowledgement"<<std::endl;
		    }
		    else std::cout<<"error receiving aknowledgement"<<std::endl;
		  }
		}
			
	      }
	      else std::cout<<"error sending message"<<std::endl;
	    }
	    else std::cout<<"unkonwn message type"<<std::endl;
	    
	  }
	  else if(message.more()){
	    std::cout<<"receiving unexpected multipart message"<<std::endl;
	    zmq::message_t tmp;
	    sock.recv(&tmp);
	    while(tmp.more()) sock.recv(&tmp);
	  }
	  else std::cout<<"error receiving aknowledgement"<<std::endl;
	  
	}
	else if(type.more()){
	  std::cout<<"receiving unexpected multipart message"<<std::endl;
	  zmq::message_t tmp;
	  sock.recv(&tmp);
	  while(tmp.more()) sock.recv(&tmp);
	}
	else std::cout<<"error receiving aknowledgement"<<std::endl;
	
	
	
      }


      std::vector<unsigned int> del_list;
      for(std::map<unsigned int, std::pair <boost::posix_time::ptime, zmq::message_t> >::iterator it= sent_messages.begin(); it!=sent_messages.end(); it++){ /// loop over sent messages to add to send queue again or drop
	
	boost::posix_time::time_duration lapse= resend_period - (boost::posix_time::second_clock::universal_time()- it->second.sent_time);

	if(lapse.is_negative())
	  if(it->second.tries <= args->retries) message_queue_akn[it->first]=it->second;	    
	del_list.push_back(it->first);
      }

      for(int i=0; i<del_list.size(); i++) sent_list.erase(del_list.at(i));
      
      
   
    } // end of while


  delete args;
  args=0;

  pthread_exit(NULL);

}


bool SQLHandler::SendToThread(std::string message, std::string id){

  zmq::message_t msg(message.length()+1);   
  snprintf(msg, message.length()+1 , "%s" , message.c_str());

  zmq::message_t mid(id.length()+1);
  snprintf(mid, id.length()+1 , "%s" , id.c_str());

  bool ret=true;
  ret*=sock->send(mid, ZMQ_SNDMORE);  
  ret*=sock->send(msg);   // could poll here but may be overkill. likely not as thread could be busy and timeout of poll wihtout sending would be a better return value;

  return ret;
}


bool SQLHandler::Write_forget(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  return SendToThread(tmp.str(), "wf"); 
}

bool SQLHandler:: Write_asyncronous(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  return SendToThread(tmp.str(), "wa"); 

}

bool SQLHandler:: Write_syncronous(std::string table, std::string data, std::string fields, std::string conditions){

  std::stringstream tmp;
  
  tmp<<"INSERT INTO "<<table<<" ";
  if(fields!="") tmp<<fields<<" ";
  tmp<<" VALUES "<<data;
  if(conditions!="") tmp<<" Where "<<conditions;
  tmp<<";";

  if(SendToThread(tmp.str(), "ws") && AknFromThread() )  // need to impliment akn from thread
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
    sock->recv(msg); //need to add more check and probably poll
    


  return true;
  
  }
  
  else return false;

}



~SQLHandler::SQLHandler(){

  args->kill=true;
  pthread_join(args->thread, NULL);
  args=0;

}

