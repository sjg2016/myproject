package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class TopicWriteThread {
	private AtomicInteger count = new AtomicInteger(0);
	private String topic;
	private String filePath;
	private FileWriter fw;
    private ThreadPoolExecutor pool = new ThreadPoolExecutor(MessageStoreImpl.MODEL, MessageStoreImpl.MODEL, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    private Map<Integer,FileWriter> map = new HashMap<>();
    private CountDownLatch synCdl = new CountDownLatch(MessageStoreImpl.MODEL);
	public TopicWriteThread(String filePath,String topic){
		this.topic = topic;
		this.filePath = filePath;

		for(int i = 0;i<MessageStoreImpl.MODEL;i++){
			map.put(i, new FileWriter(topic,filePath,topic+"_"+i,synCdl));
			pool.submit(map.get(i));
//			new Thread(map.get(i)).start();
		}

//		System.out.println("init topicWriterThread cost:"+(System.currentTimeMillis()-starttime));
	}
	public void write(Message message){
		try {	
			map.get(count.addAndGet(1)%MessageStoreImpl.MODEL).addMessage(message);		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



    private long allWriterRecordCount(){
    	long totalCount = 0L;
    	for(FileWriter writer:map.values()){
    		totalCount += writer.getRecordNum();
    	}
    	return totalCount;
    }
    private void flushAllWriter(){
    	for(FileWriter writer:map.values()){
    		writer.flush();
    	}
    }
    public void flush(){
//    	System.out.println("flush called."+",totalCount"+count);

    	while(true){

    		if(allWriterRecordCount() == count.get()){
    			flushAllWriter();
    			try {
					synCdl.await();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    			pool.shutdown();
    			try {
					pool.awaitTermination(100, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			return;
    		}
    		try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
    	}
    }
}

