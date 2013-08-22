package com.gigaspaces.storm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Logger;

import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import storm.trident.topology.TridentTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.gigaspaces.storm.spout.XAPConfig;
import com.gigaspaces.storm.spout.XAPTridentSpout;
import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPStreamBatch;
import com.gigaspaces.streaming.model.XAPTuple;

public class TestTopology {
	private static final Logger log=Logger.getLogger(TestTopology.class.getName());
	
	public static StormTopology createTopology(String toponame, String xaphost, String streamName)throws Exception{
		TridentTopologyBuilder builder=new TridentTopologyBuilder();
		
		XAPConfig config=new XAPConfig();
		config.setBatchSize(10);
		config.setStreamName(streamName);
		
		builder.setSpout("xapspout",streamName,"id",new XAPTridentSpout(config),1,"group");
		Map<String,String> groups=new HashMap<String,String>();
		groups.put("xapspout","group");
		builder.setBolt("printbolt", new PrintBolt(xaphost,"streamspace",streamName),1,new HashSet<String>(),groups).globalGrouping("xapspout","xapin");
		
		return builder.buildTopology();
		
	}
	
	public static void main(String[] args)throws Exception{
		String toponame="test";
		if(args.length>0)toponame=args[0];
		String xaphost="127.0.0.1";
		if(args.length>1)xaphost=args[1];
		String streamName="xapin";
		if(args.length>2)streamName=args[2];
		StormTopology topo=createTopology(toponame,xaphost,streamName);
		Config cfg=new Config();
		cfg.setDebug(true);
		cfg.setNumWorkers(1);
		StormSubmitter.submitTopology(toponame,cfg, topo);
		log.info("exiting TestTopology.main");
	}
	
	public static class Streamer implements Runnable{
		
		private XAPTupleStream stream;

		public Streamer(XAPTupleStream stream){
			this.stream=stream;
		}
		
		@Override
		public void run() {
			for(int i=0;i<1000;i+=2){
				XAPTuple tuple=stream.createTuple();
				tuple.setProperty("f1",i);
				tuple.setProperty("f2",i+1);
				stream.writeBatch(tuple,tuple,tuple,tuple,tuple);
				System.out.println("wrote batch");
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class PrintBolt implements ITridentBatchBolt{
		private Map config;
		private TopologyContext context;
		private BatchOutputCollector collector;
		private String host,space,streamName;
		private transient XAPTupleStream stream;
		private Object batchId=null;
		private int batchCnt=0;
		
		public PrintBolt(String xapHost, String spaceName,String stream){
			host=xapHost;
			space=spaceName;
			this.streamName=stream;
		}

		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector){
			
			this.config=conf;
			this.context=context;
			this.collector=collector;
			try {
				stream=new XAPStreamFactory(String.format("jini://*/*/%s?locators=%s",space,host)).getTupleStream(streamName);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("PrintBolt:prepare called: stream="+stream.toString());
		}

		@Override
		public void execute(BatchInfo batchInfo, Tuple tuple) {
			System.out.println("PrintBolt:in execute: "+tuple);
			if(batchId==null || (!batchId.equals(batchInfo.batchId))){
				batchId=batchInfo.batchId;
				batchCnt=1;
			}
			else{
				batchCnt++;
			}
		}

		@Override
		public void finishBatch(BatchInfo batchInfo) {
			System.out.println("PrintBolt:in finishbatch");
			stream.clearBatch(new XAPStreamBatch.BatchInfo(stream.getTupleTypeName(),batchCnt));
		}

		@Override
		public Object initBatchState(String batchGroup, Object batchId) {
			System.out.println("PrintBolt:in initbatchstate");
			
			return null;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			System.out.println("PrintBolt:in declare");
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			System.out.println("PrintBolt:in get config");
			return null;
		}

		@Override
		public void cleanup() {
			System.out.println("PrintBolt:in cleanup");
		}
		
	}

}
