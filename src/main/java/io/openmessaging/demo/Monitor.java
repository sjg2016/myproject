package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Monitor extends Thread{
	List<FileWriter> list1 = new java.util.concurrent.CopyOnWriteArrayList<>();
	List<FileReader> list2 = new java.util.concurrent.CopyOnWriteArrayList<>();
	private static Monitor ss = new Monitor();
	public void addWriter(FileWriter w){
		list1.add(w);
	}
	private Monitor(){
		this.start();
	}
	public synchronized static Monitor getInstance(){
		return ss;
	}
	public void addReader(FileReader r){
		list2.add(r);
	}

	public void run(){
		while(true){
			
			try {
				getWStatistics();
				sleep(2000);
				
				getRStatistics();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void getRStatistics(){
		Map<Integer,String> buffsizeAndMsgMap = new HashMap<>();
		Map<Integer,Integer> buffsizeAndMsgMapCount = new HashMap<>();
		for(FileReader w:list2){
			int currentSize = w.getMessageList().size()/10*10;
			if(!buffsizeAndMsgMapCount.containsKey(currentSize)){
				buffsizeAndMsgMapCount.put(currentSize,1);			
			}else{
				buffsizeAndMsgMapCount.put(currentSize,buffsizeAndMsgMapCount.get(currentSize)+1);
			}
			if(!buffsizeAndMsgMap.containsKey(currentSize)){
				buffsizeAndMsgMap.put(currentSize,w.getFileName());
			}else{
				buffsizeAndMsgMap.put(currentSize,buffsizeAndMsgMap.get(currentSize)+","+w.getFileName());
			}
			
		}
		List<Integer> sortList = new ArrayList<>();
		sortList.addAll(buffsizeAndMsgMapCount.keySet());
		java.util.Collections.sort(sortList);
		java.util.Collections.reverse(sortList);
		StringBuffer sb = new StringBuffer("RCount:");
		int count = 0;
		for(int key:sortList){
			count++;
			if(count >5){
				break;
			}
			sb.append("["+key+":"+buffsizeAndMsgMapCount.get(key)+"]");
		}
		sb.append("\nRDetail");
		count = 0;
		for(int key:sortList){
			count++;
			if(count >5){
				break;
			}
			sb.append("["+key+":"+buffsizeAndMsgMap.get(key)+"]");
		}
		System.out.println(sb.toString());
	}
	private void getWStatistics(){
		Map<Integer,String> buffsizeAndMsgMap = new HashMap<>();
		Map<Integer,Integer> buffsizeAndMsgMapCount = new HashMap<>();
		for(FileWriter w:list1){
			int currentSize = w.getMessageList().size()/10*10;
			if(!buffsizeAndMsgMapCount.containsKey(currentSize)){
				buffsizeAndMsgMapCount.put(currentSize,1);			
			}else{
				buffsizeAndMsgMapCount.put(currentSize,buffsizeAndMsgMapCount.get(currentSize)+1);
			}
			if(!buffsizeAndMsgMap.containsKey(currentSize)){
				buffsizeAndMsgMap.put(currentSize,w.getTopic());
			}else{
				buffsizeAndMsgMap.put(currentSize,buffsizeAndMsgMap.get(currentSize)+","+w.getFileName());
			}
			
		}
		List<Integer> sortList = new ArrayList<>();
		sortList.addAll(buffsizeAndMsgMapCount.keySet());
		java.util.Collections.sort(sortList);
		java.util.Collections.reverse(sortList);
		StringBuffer sb = new StringBuffer("WCount:");
		int count = 0;
		for(int key:sortList){
			count++;
			if(count >5){
				break;
			}
			sb.append("["+key+":"+buffsizeAndMsgMapCount.get(key)+"]");
		}
		sb.append("\nWDetail");
		count = 0;
		for(int key:sortList){
			count++;
			if(count >5){
				break;
			}
			sb.append("["+key+":"+buffsizeAndMsgMap.get(key)+"]");
		}
		System.out.println(sb.toString());
	}
}