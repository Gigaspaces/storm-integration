package com.gigaspaces.storm.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPStreamBatch;
import com.gigaspaces.streaming.model.XAPStreamBatch.BatchInfo;
import com.gigaspaces.streaming.model.XAPTuple;

/**
 * A Trident batching spout implementation that reads from
 * a @see com.gigaspaces.streaming.client.XAPTupleStream
 * 
 * @author DeWayne
 *
 */
public class XAPTridentSpout implements ITridentSpout<BatchInfo> {
	private static final long serialVersionUID = 1L;
	private static final Logger log=Logger.getLogger(XAPTridentSpout.class);
	XAPConfig cfg;
	transient Admin admin;
	transient XAPTupleStream stream;

	public XAPTridentSpout(XAPConfig cfg){
		if(cfg==null)throw new IllegalArgumentException("null config supplied");
		this.cfg=cfg;
		this.admin=new AdminFactory().create();
		log.info("XAP spout created");
	}

	public Fields getOutputFields() {
		return new Fields(getStream().getFieldNames());
	}


	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<BatchInfo> getCoordinator(
			String txStateId, Map conf, TopologyContext context) {
		if(log.isDebugEnabled())log.debug("getcoordinator called");	
		return new Coordinator();
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<BatchInfo> getEmitter(
			String txStateId, Map conf, TopologyContext context) {
		if(log.isDebugEnabled())log.debug("getemitter called");
		return new Emitter();
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}
	
	private XAPTupleStream getStream(){
		if(stream==null){
			if(log.isDebugEnabled())log.debug("spout creating stream");
			stream=new XAPTupleStream(cfg.getUrl(),cfg.getStreamName());
		}
		return stream;
	}

	class Emitter implements ITridentSpout.Emitter<BatchInfo>{
		//need to save several in case of pipelining
		Map<Long,BatchInfo> emitted=new HashMap<Long,BatchInfo>();

		@Override
		public void emitBatch(TransactionAttempt tx, BatchInfo coordinatorMeta,
				TridentCollector collector) {

			XAPStreamBatch<XAPTuple> batch=getStream().readBatch(cfg.getBatchSize());		
			if(log.isDebugEnabled())log.debug("read batch cnt="+batch.getInfo().qty);
			if(batch.getInfo().qty>0){
				for(int i=0;i<batch.getEntries().size();i++){
					List<Object> list=new ArrayList<Object>();
					for(String fname:getStream().getFieldNames()){
						list.add(batch.getEntries().get(i).getProperty(fname));
					}
					collector.emit(list);
				}
				emitted.put(tx.getTransactionId(),batch.getInfo());
			}
			else{
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
			}
			if(log.isDebugEnabled())log.debug(String.format("emit batch called, tx id=%d qty=%d",tx.getTransactionId(),batch.getInfo().qty));
		}

		@Override
		public void success(TransactionAttempt tx) {
			if(log.isDebugEnabled())log.debug("success called, tx attemptid="+tx.getTransactionId());
			BatchInfo info=emitted.get(tx.getTransactionId());
			if(info!=null){
				getStream().clearBatch(info);
				emitted.remove(tx.getTransactionId());
			}
			else{
				log.warn("info null");
			}
		}

		@Override
		public void close() {
			if(log.isDebugEnabled())log.debug("close called");
		}

	}

	class Coordinator implements ITridentSpout.BatchCoordinator<BatchInfo>{
		@Override
		public BatchInfo initializeTransaction(long txid, BatchInfo prevMetadata) {
			BatchInfo info=new BatchInfo();
			info.qty=cfg.getBatchSize();
			return info;
		}

		@Override
		public void success(long txid) {
			if(log.isDebugEnabled())log.debug("coord.success called");
			//noop
		}

		@Override
		public boolean isReady(long txid) {
			if(log.isDebugEnabled())log.debug("coord.isready called");
			return true;
		}

		@Override
		public void close() {
			if(log.isDebugEnabled())log.debug("coord.close called");
			//noop
		}

	}

}
