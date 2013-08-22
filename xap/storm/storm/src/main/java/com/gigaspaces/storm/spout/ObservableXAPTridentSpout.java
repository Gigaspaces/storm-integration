package com.gigaspaces.storm.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPStreamBatch;
import com.gigaspaces.streaming.model.XAPStreamBatch.BatchInfo;
import com.gigaspaces.streaming.model.XAPTuple;
import com.gigaspaces.streaming.util.FixedNumberQueue;

/**
 * A Trident batching spout implementation that reads from
 * a @see com.gigaspaces.streaming.client.XAPTupleStream and
 * records performance statistics in a space
 * 
 * @author DeWayne
 *
 */
public class ObservableXAPTridentSpout implements ITridentSpout<BatchInfo> {
	private static final long serialVersionUID = 1L;
	private static final Logger log=Logger.getLogger(ObservableXAPTridentSpout.class);
	XAPConfig cfg;
	transient XAPTupleStream stream;

	public ObservableXAPTridentSpout(XAPConfig cfg){
		if(cfg==null)throw new IllegalArgumentException("null config supplied");
		this.cfg=cfg;
		log.info("XAP spout created");
	}

	public Fields getOutputFields() {
		if(log.isDebugEnabled())log.debug("getOutputFields called, fields:"+getStream().getFieldNames());
		return new Fields(getStream().getFieldNames());
	}

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<BatchInfo> getCoordinator(
			String txStateId, Map conf, TopologyContext context) {
		if(log.isEnabledFor(Level.DEBUG))log.debug("getcoordinator called");	
		return new Coordinator();
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<BatchInfo> getEmitter(
			String txStateId, Map conf, TopologyContext context) {
		if(log.isEnabledFor(Level.DEBUG))log.debug("getemitter called");
		return new Emitter();
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	private XAPTupleStream getStream(){
		if(stream==null){
			if(log.isEnabledFor(Level.DEBUG))log.debug("spout creating stream");
			//TODO: passing in the host alone isn't good enough.  Forces the stream space 
			//      to be named "streamspace"
			XAPStreamFactory fact=new XAPStreamFactory(String.format("jini://*/*/streamspace?locators=%s",cfg.getXapHost()));			
			Set<String> streams=fact.listStreams();
			if(streams.contains(cfg.getStreamName())){
				try{
					if(log.isDebugEnabled())log.debug("using existing stream");
					stream=fact.getTupleStream(cfg.getStreamName());
				}
				catch(Exception e){
					if(e instanceof RuntimeException)throw (RuntimeException)e;
					throw new RuntimeException(e);
				}
			}
			else{
				if(log.isDebugEnabled())log.debug("creating new tuple stream");
				stream=fact.createNewTupleStream(cfg.getStreamName(), 0, Arrays.asList(cfg.getFields()));
			}
		}
		return stream;
	}

	/**
	 * For collecting stats for the UI
	 * 
	 * @author DeWayne
	 *
	 */
	@SpaceClass
	public static class XAPSpoutStats{
		private FixedNumberQueue<Integer> readLatencyMs=new FixedNumberQueue<Integer>(10);  //per tuple read latency
		private FixedNumberQueue<Integer> tps=new FixedNumberQueue<Integer>(10);            //tuples per second

		/**
		 * Singleton
		 * @return
		 */
		@SpaceId
		@SpaceRouting
		public Integer getId(){
			return 0;
		}
		public void setId(Integer id){}

		public FixedNumberQueue<Integer> getReadLatencyMs() {
			return readLatencyMs;
		}
		public void setReadLatencyMs(FixedNumberQueue<Integer> readLatencyMs) {
			this.readLatencyMs = readLatencyMs;
		}
		public FixedNumberQueue<Integer> getTps() {
			return tps;
		}
		public void setTps(FixedNumberQueue<Integer> tps) {
			this.tps = tps;
		}

	}

	class Emitter implements ITridentSpout.Emitter<BatchInfo>{
		//need to save several in case of pipelining
		Map<Long,BatchInfo> emitted=new HashMap<Long,BatchInfo>();
		private transient GigaSpace space=null;
		private XAPSpoutStats stats=new XAPSpoutStats();
		private int batchcnt=0;
		private UrlSpaceConfigurer usc=null;

		public Emitter(){
			try{
				usc=new UrlSpaceConfigurer(String.format("jini://*/*/streamspace?locators=%s",cfg.getXapHost()));
				space=new GigaSpaceConfigurer(usc.space()).gigaSpace();
				space.write(new XAPSpoutStats());
			}
			catch(Exception e){ e.printStackTrace();} 
		}

		@Override
		public void emitBatch(TransactionAttempt tx, BatchInfo coordinatorMeta,
				TridentCollector collector) {
			if(log.isEnabledFor(Level.INFO))log.info("emit batch called, tx="+tx.getTransactionId());
			long now=System.currentTimeMillis();
			XAPStreamBatch<XAPTuple> batch=getStream().readBatch(cfg.getBatchSize());		
			if(log.isEnabledFor(Level.INFO))log.info("read batch cnt="+batch.getInfo().qty);
			if(batch.getInfo().qty>0){
				for(int i=0;i<batch.getEntries().size();i++){
					List<Object> list=new ArrayList<Object>();
					for(String fname:getStream().getFieldNames()){
						list.add(batch.getEntries().get(i).getProperty(fname));
					}
					collector.emit(list);
				}
				emitted.put(tx.getTransactionId(),batch.getInfo());
				
				//Update stats for ui
				try{
					ChangeSet cs=new ChangeSet();
					cs.addToCollection("readLatencyMs", (int)(System.currentTimeMillis()-now));
					space.change(new IdQuery<XAPSpoutStats>(XAPSpoutStats.class,0,0), cs);
				}
				catch(Exception e){ e.printStackTrace();}
			}
			else{
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
			}
			if(log.isEnabledFor(Level.DEBUG))log.debug(String.format("emit batch called, tx id=%d qty=%d",tx.getTransactionId(),batch.getInfo().qty));
		}

		@Override
		public void success(TransactionAttempt tx) {
			if(log.isEnabledFor(Level.DEBUG))log.debug("success called, tx attemptid="+tx.getTransactionId());
			BatchInfo info=emitted.get(tx.getTransactionId());
			if(info!=null){
				if(log.isDebugEnabled())log.debug("info found");
				getStream().clearBatch(info);
				emitted.remove(tx.getTransactionId());
			}
			else{
				log.debug("info null.");
			}
		}

		@Override
		public void close() {
			if(log.isEnabledFor(Level.DEBUG))log.debug("close called");
			try{
				if(usc!=null)usc.destroy();
			}
			catch(Exception e){}
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
			if(log.isEnabledFor(Level.DEBUG))log.debug("coord.success called");
			//noop
		}

		@Override
		public boolean isReady(long txid) {
			if(log.isEnabledFor(Level.DEBUG))log.debug("coord.isready called");
			return true;
		}

		@Override
		public void close() {
			if(log.isEnabledFor(Level.DEBUG))log.debug("coord.close called");
			//noop
		}

	}


}
