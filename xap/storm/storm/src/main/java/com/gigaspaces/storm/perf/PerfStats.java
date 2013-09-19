package com.gigaspaces.storm.perf;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.streaming.util.FixedNumberQueue;

/**
 * Aggregator for performance stats. Only useful for trivial single spout
 * single state scenario (aka demos and benchmarking)
 * 
 * @author DeWayne
 *
 */
@SpaceClass
public class PerfStats {
	private FixedNumberQueue<Integer> readLatency;
	private FixedNumberQueue<Integer> writeLatency;
	private int readBatchSize=0;
	
	public PerfStats(){}
	
	public PerfStats(int sampleLimit,int batchSize){
		readLatency=new FixedNumberQueue<Integer>(sampleLimit);
		writeLatency=new FixedNumberQueue<Integer>(sampleLimit);
		readBatchSize=batchSize;
	}
	
	/**
	 * Singleton object
	 */
	@SpaceId(autoGenerate=false)
	@SpaceRouting
	public Integer getId() {
		return 0;
	}
	public void setId(Integer id) {
	}
	public FixedNumberQueue<Integer> getReadLatency() {
		return readLatency;
	}
	public void setReadLatency(FixedNumberQueue<Integer> readLatency) {
		this.readLatency = readLatency;
	}
	public FixedNumberQueue<Integer> getWriteLatency() {
		return writeLatency;
	}
	public void setWriteLatency(FixedNumberQueue<Integer> writeLatency) {
		this.writeLatency = writeLatency;
	}
	
	public int getReadBatchSize() {
		return readBatchSize;
	}

	public void setReadBatchSize(int readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

}
