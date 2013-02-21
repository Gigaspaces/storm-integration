package com.gigaspaces.streaming.model;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a batch of entries.  
 * @author DeWayne
 *
 * @param <T>
 */
public class XAPStreamBatch<T> {
	private BatchInfo info;
	private List<T> entries;
	
	public List<T> getEntries() {
		return entries;
	}

	public void setEntries(List<T> entries) {
		this.entries = entries;
	}

	public void setInfo(BatchInfo info) {
		this.info = info;
	}

	public BatchInfo getInfo() {
		return info;
	}

	/**
	 * Meta data about the batch that is sufficient
	 * to either replay or delete the batch.
	 * 
	 * @author DeWayne
	 *
	 */
	public static class BatchInfo implements Serializable{
		private static final long serialVersionUID = 1L;
		
		public String entryTypeName;
		public int qty;
		
		public BatchInfo(){}
		public BatchInfo(String typeName, int quantity){
			this.entryTypeName=typeName;
			this.qty=quantity;
		}
	}
	
}
