package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.List;

public class MyBlockedQueue<T>{
	private int size = 1000;
	private int arraySize = 1;
	private List<List<T>> listList = new ArrayList<>();
	public MyBlockedQueue(int size){
		this.size = size;
		if(size <= 1000){
			this.arraySize = 1;
			listList.add(new ArrayList<>());

		}else{
			this.arraySize = (size/1000)+(size%1000>0?1:0);		
			for(int i = 0;i<arraySize;i++){
				listList.add(new ArrayList<>());
			}
		}

	}
	private int currentArrayIndex = -1;
	private int currentIndexInList = -1;
	public synchronized T take(){
		return null;
	}

}
