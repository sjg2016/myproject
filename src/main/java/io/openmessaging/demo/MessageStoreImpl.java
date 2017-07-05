package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;

import io.openmessaging.Message;

public class MessageStoreImpl {

    private static final MessageStoreImpl INSTANCE = new MessageStoreImpl();
    
    public static Map<String,TopicWriteThread> resultMap = new HashMap<>();
    public static int MODEL = 1;
    public static MessageStoreImpl getInstance() {
        return INSTANCE;
    }
    private String filePath;

    public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	private MessageStoreImpl(){ 
    	//每100条message存放到一个file，named topic|queue_name_0000000
    	
    }

    public static void main(String [] args){
    }
//    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

//    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
  
    public static Map<String,TopicWriteThread> queueNameAndWriterMap = new HashMap<>();
    public void putMessage(String bucket, Message message) {
    	if(queueNameAndWriterMap.get(bucket) == null){
    		synchronized(queueNameAndWriterMap){
    			if(queueNameAndWriterMap.get(bucket) == null){
    				queueNameAndWriterMap.put(bucket,new TopicWriteThread(filePath,bucket));
    			}
    		}
    	}
    	queueNameAndWriterMap.get(bucket).write(message);
    }

   private Map<String,TopicReadThread> queueNameAndReaderMap = new HashMap<>();
	public Message pullMessage(String queue, String bucket) {
	
    	if(queueNameAndReaderMap.get(bucket) == null){
	    	synchronized(queueNameAndReaderMap){
	    		if(queueNameAndReaderMap.get(bucket) == null){		
	 	    		queueNameAndReaderMap.put(bucket,new TopicReadThread(filePath,bucket));
	    		}
	    	}	
    	}

 	    return queueNameAndReaderMap.get(bucket).read();
	
	}
	public void flushWriter(){
//		System.out.println("queueNameAndWriterMap.size:"+queueNameAndWriterMap.size());
		
		for(TopicWriteThread t: queueNameAndWriterMap.values()){
			t.flush();
		}
	}
}
