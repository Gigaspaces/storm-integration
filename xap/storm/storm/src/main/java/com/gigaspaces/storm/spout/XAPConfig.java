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
	private String xapHost;
	private String streamName;
	private String[] fields;
	private boolean collectStats=false;
	
	public XAPConfig(){}
	
	public XAPConfig(String xapHost, String streamName, int batchSize){
		this.batchSize=batchSize;
		this.xapHost=xapHost;
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

	public void setXapHost(String xapHost) {
		this.xapHost = xapHost;
	}

	public String getXapHost() {
		return xapHost;
	}

	public void setFields(String... fields) {
		this.fields=fields;
	}
	public String[] getFields(){
		return fields;
	}
	
	public boolean isCollectStats() {
		return collectStats;
	}

	public void setCollectStats(boolean collectStats) {
		this.collectStats = collectStats;
	}

}
