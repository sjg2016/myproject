package io.openmessaging.demo;

import java.util.Arrays;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

    /**
	 * 
	 */
	private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties = new DefaultKeyValue();
    private byte[] body;
    private boolean bEOF = false;
    public String toJsonString(){
    	StringBuffer buff = new StringBuffer();
    	if(headers == null){
    		buff.append("H0:");
    	}else{
    		String headersContent = ((DefaultKeyValue)headers).toJsonString();
    		buff.append("H");
    		buff.append(headersContent.length());
    		buff.append(":");
    		buff.append(headersContent);
    	}
    	if(properties == null){
    		buff.append("P0:");
    	}else{
    		String propsContent = ((DefaultKeyValue)properties).toJsonString();
    		buff.append("P");
    		buff.append(propsContent.length());
    		buff.append(":");
    		buff.append(propsContent);
    	}
    	String bodyContent = new String(body);
    	buff.append("B");
    	buff.append(bodyContent.length());
    	buff.append(":");
    	buff.append(bodyContent);
    	long buffLength = buff.length();
    	buff.insert(0, "\n");
    	buff.insert(0, buffLength);
    	return buff.toString();
//    	return buff.length()+"\n"+buff.toString();
    }
    
    public static void main(String[] args){
//    	DefaultBytesMessage d = new DefaultBytesMessage("12345".getBytes());
//    	d.putHeaders("1", 100);
//    	d.putProperties("2", 200L);
//    	String toJson = d.toJsonString();
//    	System.out.println(toJson);
//    	int index = toJson.indexOf("\n");
//    	int length = Integer.parseInt(toJson.substring(0,index));
//    	System.out.println("length:"+length);
//    	System.out.println(parseJsonString(toJson.substring(index+1, index+1+length)).toJsonString());
    	
    	DefaultBytesMessage d = new DefaultBytesMessage("12345".getBytes());
    	d.putHeaders("1", 100);
    	d.putProperties("2", 200L);
    	long starttime = System.currentTimeMillis();
    	for(int i = 0;i<1000*1000*40;i++){
    		d.toJsonString();
    	}
    	System.out.println("cost:"+(System.currentTimeMillis()-starttime));//39s
    	
    	
    }
    public static DefaultBytesMessage parseJsonString(String json){
    	DefaultBytesMessage d = new DefaultBytesMessage(null);
    	int start = 0;
    	int index = json.indexOf(":",start);
    	int headerLength = Integer.parseInt(json.substring(1, index));
    	if(headerLength >0){
    		d.headers = DefaultKeyValue.parseJsonString(json.substring(index+1,index+1+headerLength));
    	}
    	start = index+1+headerLength;
    	index = json.indexOf(":", start);
    	int propLength = Integer.parseInt(json.substring(start+1, index));
    	if(propLength >0){
    		d.properties = DefaultKeyValue.parseJsonString(json.substring(index+1,index+1+propLength));
    	}
    	
    	start = index+1+propLength; 
    	index = json.indexOf(":", start);
    	int bodyLength = Integer.parseInt(json.substring(start+1, index));
    	if(bodyLength >0){
    		
    		d.body = json.substring(index+1,index+1+bodyLength).getBytes();
    	}
    	return d;
    }
    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(body);
		result = prime * result + ((headers == null) ? 0 : headers.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
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
		DefaultBytesMessage other = (DefaultBytesMessage) obj;
		if (!Arrays.equals(body, other.body))
			return false;
		if (headers == null) {
			if (other.headers != null)
				return false;
		} else if (!headers.equals(other.headers))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		return true;
	}
	@Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

	public boolean isbEOF() {
		return bEOF;
	}

	public void setbEOF(boolean bEOF) {
		this.bEOF = bEOF;
	}
}
