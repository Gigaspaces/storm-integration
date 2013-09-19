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

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.storm.perf.PerfStats;
import com.gigaspaces.streaming.client.XAPStreamFactory;
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
	transient UrlSpaceConfigurer usc;
	transient GigaSpace statsSpace;
	transient XAPTupleStream stream;

	public XAPTridentSpout(XAPConfig cfg){
		if(cfg==null)throw new IllegalArgumentException("null config supplied");
		this.cfg=cfg;
		log.debug("XAP spout created");
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
			//Set up stats object if configured
			setupStatsCollector();

			if(log.isEnabledFor(Level.DEBUG))log.debug("spout creating stream");
			//TODO: passing in the host alone isn't good enough.  Forces the stream space 
			//      to be named "streamspace"
			XAPStreamFactory fact=new XAPStreamFactory(xapUrl());
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

	private void setupStatsCollector() {
		if(!cfg.isCollectStats())return;
		if(usc==null){
			usc=new UrlSpaceConfigurer(xapUrl());
			statsSpace=new GigaSpaceConfigurer(usc.space()).gigaSpace();
		}
		PerfStats ps=new PerfStats(10,cfg.getBatchSize());
		try{
			statsSpace.write(ps,WriteModifiers.WRITE_ONLY);
		}
		catch(Exception e){ /*ignore*/} 
	}

	private String xapUrl(){
		return String.format("jini://*/*/streamspace?locators=%s",cfg.getXapHost());
	}

	class Emitter implements ITridentSpout.Emitter<BatchInfo>{
		//need to save several in case of pipelining
		Map<Long,BatchInfo> emitted=new HashMap<Long,BatchInfo>();
		private transient GigaSpace space=null;
		private int batchcnt=0;
		private UrlSpaceConfigurer usc=null;

		public Emitter(){
		}

		@Override
		public void emitBatch(TransactionAttempt tx, BatchInfo coordinatorMeta,
				TridentCollector collector) {
			if(log.isEnabledFor(Level.DEBUG))log.debug("emit batch called, tx="+tx.getTransactionId());
			long now=0;
			if(cfg.isCollectStats())now=System.currentTimeMillis();
			XAPStreamBatch<XAPTuple> batch=getStream().readBatch(cfg.getBatchSize());
			if(log.isEnabledFor(Level.DEBUG))log.debug("read batch cnt="+batch.getInfo().qty);
			if(batch.getInfo().qty>0){
				if(cfg.isCollectStats())updateStats(System.currentTimeMillis()-now);
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
			if(log.isEnabledFor(Level.DEBUG))log.debug(String.format("emit batch called, tx id=%d qty=%d",tx.getTransactionId(),batch.getInfo().qty));
		}

		private void updateStats(long l) {
			ChangeSet cs=new ChangeSet();
			cs.addToCollection("readLatency", (int)l);
			statsSpace.change(new IdQuery<PerfStats>(PerfStats.class,0), cs, ChangeModifiers.ONE_WAY);
		}

		@Override
		public void success(TransactionAttempt tx) {
			if(log.isEnabledFor(Level.DEBUG))log.debug("success called, tx attemptid="+tx.getTransactionId());
			BatchInfo debug=emitted.get(tx.getTransactionId());
			if(debug!=null){
				if(log.isDebugEnabled())log.debug("debug found");
				getStream().clearBatch(debug);
				emitted.remove(tx.getTransactionId());
			}
			else{
				log.debug("debug null.");
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
			BatchInfo debug=new BatchInfo();
			debug.qty=cfg.getBatchSize();
			return debug;
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
