package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.openmessaging.KeyValue;

public class DefaultKeyValue implements KeyValue {

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((kvs == null) ? 0 : kvs.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultKeyValue other = (DefaultKeyValue) obj;
		if (kvs == null) {
			if (other.kvs != null)
				return false;
		} else if (!kvs.equals(other.kvs))
			return false;
		return true;
	}
	public static DefaultKeyValue parseJsonString(String json){
		DefaultKeyValue kv = new DefaultKeyValue();
		int start = 0;
		int index = json.indexOf(":",start);
		while(index > 0){
			int length = Integer.parseInt(json.substring(start,index));
			start = index+1;
			String unit = json.substring(start,start+length);
			start = start+length;
			parseUnit(unit,kv);
			index = json.indexOf(":",start);
		}
		return kv;
	}
	private static void parseUnit(String unit,DefaultKeyValue kv){
		int start = 0;
		int index = unit.indexOf(":",start);
		String key = unit.substring(0,index);
		start = index+1;
		
		String type = unit.substring(start,start+1);
		start = start+1;
		index = unit.indexOf(",",start);
		int length =  Integer.parseInt(unit.substring(start,index));
		start = index+1;
		String value = unit.substring(start,start+length);

		if(type.equals("i")){
			kv.put(key, Integer.parseInt(value));
			return;
		}
		if(type.equals("l")){
			kv.put(key, Long.parseLong(value));
			return;
		}
		if(type.equals("d")){
			kv.put(key, Double.parseDouble(value));
			return;
		}
		kv.put(key, value);
	}
	public String toJsonString(){
		StringBuffer buff = new StringBuffer();
		
		for(String key:kvs.keySet()){	
			StringBuffer subBuff = new StringBuffer();
			Object val = kvs.get(key);
			subBuff.append(key).append(":").append(getValueByType(val));
			buff.append(subBuff.length()).append(":").append(subBuff.toString());
//			subBuff.delete(0, subBuff.length());
		}
		return buff.toString();
	}

	private static String getValueByType(Object val){
		if(val instanceof Integer){
			String value = String.valueOf(val);
			StringBuffer sb = new StringBuffer().append("i").append(value.length()).append(",").append(value);
			return sb.toString();
		}
		if(val instanceof Long){
			String value = String.valueOf(val);
			StringBuffer sb = new StringBuffer().append("l").append(value.length()).append(",").append(value);
			return sb.toString();
		}
		if(val instanceof Double){
			String value = String.valueOf(val);
			StringBuffer sb = new StringBuffer().append("d").append(value.length()).append(",").append(value);
			return sb.toString();
		}
		String value = String.valueOf(val);
		StringBuffer sb = new StringBuffer().append("s").append(value.length()).append(",").append(value);
		return sb.toString();
	}
	public static void main(String[] args){
		DefaultKeyValue d = new DefaultKeyValue();
		d.put("1", "111");
		d.put("2", 200);
		d.put("3", 66666666666L);
		d.put("4", 66666666666d);
		System.out.println(d.toJsonString());
		System.out.println(DefaultKeyValue.parseJsonString(d.toJsonString()).toJsonString());
	}
	/**
	 * 
	 */
	private final Map<String, Object> kvs = new HashMap<String, Object>();
    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return (Integer)kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long)kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double)kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}
