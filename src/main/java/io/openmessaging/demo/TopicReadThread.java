package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class TopicReadThread{
	private Map<Integer,FileReader> map = new HashMap<>();
	private ThreadPoolExecutor pool = new ThreadPoolExecutor(MessageStoreImpl.MODEL, MessageStoreImpl.MODEL, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	private AtomicInteger count = new AtomicInteger(0);
    public TopicReadThread(String filePath,String topic){
		for(int i = 0;i<MessageStoreImpl.MODEL;i++){
			map.put(i, new FileReader(filePath,topic+"_"+i));
			pool.submit(map.get(i));
		}
//		System.out.println("init topicReaderThread cost:"+(System.currentTimeMillis()-starttime));
	}
    

	public synchronized Message read(){	
		try {
			FileReader reader = map.get((count.addAndGet(1))%MessageStoreImpl.MODEL);
			if(reader == null){
				return null;
			}
			DefaultBytesMessage message = (DefaultBytesMessage)reader.readMessage();
			if(message.isbEOF()){
				map.clear();
				pool.shutdown();
    			try {
					pool.awaitTermination(100, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
			return message;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
			
	}
}


