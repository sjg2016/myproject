package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;

import io.openmessaging.Message;

public class FileReader implements Runnable {
	private BlockingQueue<Message> messageList = new  java.util.concurrent.LinkedBlockingQueue<>(10000);
	private String fileName;
	private java.io.BufferedReader ois;
	public BlockingQueue<Message> getMessageList() {
		return messageList;
	}
	public FileReader(String path,String fileName){
		Monitor monitor = Monitor.getInstance();
		monitor.addReader(this);
		this.fileName = fileName;
		File file = new File(path+"/"+fileName);	
		try {
			ois = new java.io.BufferedReader(new java.io.InputStreamReader(new java.io.FileInputStream(file),"UTF-8"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	public String getFileName() {
		return fileName;
	}
	
	public Message readMessage() throws InterruptedException {
		return messageList.take();

	}

    public String readNextObjectFromFile(){
    	String msg = null;
		try {		
			msg = ois.readLine();//第一行是size，然后是内容
			if(msg == null){
				return null;
			}
			int length = Integer.parseInt(msg);
			char[] cbuf = new char[length];
			ois.read(cbuf);
			msg = new String(cbuf);
//		    currentReadNo++; 	
		  	return msg;
		}  catch (Exception e) {
			e.printStackTrace();
//			System.out.println("reach file end:"+fileName);
			return null;

		}
  
    }
    public void close(){
    	try{
	    	if(ois != null){
	    		ois.close();
	    		ois = null;
	    	}
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
	@Override
	public void run() {
		// TODO Auto-generated method stub
		String message = null;
		try{
		while(true){
			message = readNextObjectFromFile();
			if(message == null){
				DefaultBytesMessage eofMsg = new DefaultBytesMessage(null);
				eofMsg.setbEOF(true);
				messageList.put(eofMsg);
				close();
//				System.out.println("to the end of file:"+this.getFileName());
				return;
			 }
			messageList.put(DefaultBytesMessage.parseJsonString(message));
		}
		}catch(Exception e){
			System.err.println(message);
			e.printStackTrace();
		}
		
	}
}
