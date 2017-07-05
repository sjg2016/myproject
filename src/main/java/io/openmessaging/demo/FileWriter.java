package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.Message;

public class FileWriter implements Runnable {
	private CountDownLatch synCdl;
	private BlockingQueue<Message> messageList = new  java.util.concurrent.LinkedBlockingQueue<>(20000);
	private static final int parserMaxNum = 5;
	private static final int msgListMaxNum = 2000;
//	private List<Parser> parserList = new ArrayList<>();
	private StringBuffer msgSb = new StringBuffer();
    private ThreadPoolExecutor pool = new ThreadPoolExecutor(parserMaxNum, parserMaxNum, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	public BlockingQueue<Message> getMessageList() {
		return messageList;
	}
//	private List<DefaultBytesMessage> tmpMsgList = new ArrayList<>();
	private String fileName;
	private String path;
	private AtomicInteger recordNum = new AtomicInteger(0);
	private java.io.Writer oos;
//	private java.io.FileOutputStream fos;
//	private java.nio.channels.FileChannel fc;
	private String topic;
	private File realFile = null;
//	private int maxsize = 1024*1024;
//	private ByteBuffer bb;
	public String getTopic(){
		return topic;
	}
//	public void setFileSize(int fileSize) {
//		this.fileSize = fileSize;
//	}
	public void addMessage(Message msg) throws Exception{
		messageList.put(msg); //117
//		recordNum++;
	}
	public FileWriter(String topic,String path,String fileName,CountDownLatch synCdl){
		Monitor monitor = Monitor.getInstance();
		monitor.addWriter(this);
//		long start = System.currentTimeMillis();
		this.topic = topic;
		this.fileName = fileName;
		this.path = path;
		realFile = new File(path+"/"+fileName);
		this.synCdl = synCdl;
	
//		System.out.println("init FileWriter cost:"+(System.currentTimeMillis()-start)+"ms");
//		new FileMonitor2(this).start();

	}
	public String getFileName() {
		return fileName;
	}
//	public long getFileSize() {
//		return fileSize;
//	}

    public void writeObjectToFile(String message){
		try {		
			oos.write(message);			
		} catch (Exception e) {
			// TODO Auto-generated catch block
//			System.err.println(this.getFileName());
			e.printStackTrace();
		}
  
    }
    public void close(){
    	
		if(oos != null){
			try {
				oos.flush();
				oos.close();
				oos = null;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }
	public int getRecordNum(){
		return recordNum.get();
	}
	@Override
	public void run() {
//		String lastMessage = null;
//		long start = System.currentTimeMillis();
		try {
//			fos = new java.io.FileOutputStream(realFile);
//			fc = fos.getChannel();		
			oos = new java.io.BufferedWriter(new java.io.OutputStreamWriter(new java.io.FileOutputStream(realFile),"UTF-8"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try{
				while(true){
					DefaultBytesMessage msg = (DefaultBytesMessage)messageList.take();
					if(msg.isbEOF()){						
						doEofForMultiParser();						
						this.close();						
						synCdl.countDown();
						return;						
					}
					String message = ((DefaultBytesMessage)msg).toJsonString();
					doForMultiParser(message);		
					

//					writeObjectToFile(message);			
					recordNum.addAndGet(1);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	private void waitFirstParserDown(){
		if(this.currentParser == null){
			return;
		}
		Parser preParser = null;
		for(int i = 0;i<parserMaxNum;i++){
			preParser = currentParser.preParser;
			if(preParser == null){
				return;
			}
		}
		try {
			preParser.selfCdl.await();
			preParser.preParser = null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private Parser currentParser = null;
	private void doForMultiParser(String msg) throws InterruptedException, IOException {
		msgSb.append(msg);
		if(msgSb.length()>=10*128*1024){
//		if(recordNum.get()%msgListMaxNum == 0){
			Parser parser = new Parser(msgSb.toString(),oos,currentParser);
			currentParser = parser;
			msgSb.delete(0, msgSb.length());
			waitFirstParserDown();
			pool.execute(parser);

		}
	}

	private void doEofForMultiParser() throws InterruptedException, IOException {
		////////*****************
		if(msgSb.length() >0){
			Parser parser = new Parser(msgSb.toString(),oos,currentParser);
			currentParser = parser;
			waitFirstParserDown();
			pool.execute(parser);			
		}
		currentParser.selfCdl.await();
		pool.shutdown();
		pool.awaitTermination(100, TimeUnit.SECONDS);
		////////*****************
	}
	public void flush(){
		try {	
			DefaultBytesMessage eof = new DefaultBytesMessage(null);
			eof.setbEOF(true);
			this.addMessage(eof);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
class Parser implements Runnable{
	private String msgs;
	private java.io.Writer oos;
	public CountDownLatch selfCdl = new CountDownLatch(1);
	Parser preParser;
	
	public Parser(String msgs,java.io.Writer oos,Parser preParser){
		this.msgs = msgs;
		this.oos = oos;
		this.preParser = preParser;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		if(preParser != null){
			try {
				preParser.selfCdl.await();
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		try {
//			oos.write(msgs);
			
//			oos.flush();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		selfCdl.countDown();
	}
	
}


