package com.gigaspaces.storm.spout;

import java.io.Serializable;

/**
 * Configuration class for {@link com.gigaspaces.storm.spout.XAPTridentSpout}.
 * 
 * @author DeWayne
 *
 */
public class XAPConfig implements Serializable{
	private static final long serialVersionUID = 1L;
	private int batchSize;
	private String url;
	private String streamName;
	
	public XAPConfig(){}
	
	public XAPConfig(String url, String streamName, int batchSize){
		this.batchSize=batchSize;
		this.url=url;
		this.streamName=streamName;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}
	
}
