package com.gigaspaces.storm.misc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.trident.TridentTopology;
import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import storm.trident.topology.TridentTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.gigaspaces.storm.spout.XAPConfig;
import com.gigaspaces.storm.spout.XAPTridentSpout;
import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPTuple;


/**
 * A test class that runs a local storm topology against an already running
 * stream hosting space "streamspace".
 * 
 * @author DeWayne
 *
 */
public class XapTest {
	
	public static StormTopology createTopology()throws Exception{
		TridentTopology tt=new TridentTopology();
		XAPStreamFactory fact=new XAPStreamFactory("jini://*/*/streamspace");
		XAPTupleStream stream;
		Set<String> streams=fact.listStreams();
		
		if(streams.contains("xapin")){
			stream=fact.getTupleStream("xapin");
			if(stream==null)throw new Exception("stream xapin not found");
			//stream.clear();  //TODO - clear doesn't work
			System.out.println("got existing xapin:"+stream);
		}
		else{
			stream=new XAPStreamFactory("jini://*/*/streamspace").createNewTupleStream("xapin",0,Arrays.asList("f1","f2"));
		}
		
		TridentTopologyBuilder builder=new TridentTopologyBuilder();
		builder.
			setSpout("xapspout","xapin","id",new XAPTridentSpout(new XAPConfig("jini://*/*/streamspace","xapin",10)),1,"group");
		Map<String,String> groups=new HashMap<String,String>();
		groups.put("xapspout","group");
		builder.setBolt("printbolt", new PrintBolt(),1,new HashSet<String>(),groups).globalGrouping("xapspout","xapin");
		return builder.buildTopology();
		
	}
	
	public static void main(String[] args)throws Exception{		
		//StormTopology topo=createTopology();
		//Config cfg=new Config();
		//cfg.setDebug(true);
		//LocalCluster cluster=new LocalCluster();
		new Thread(new Streamer(new XAPStreamFactory("jini://*/*/streamspace").getTupleStream("xapin"))).start();
		//cluster.submitTopology("x",cfg,topo);
		//StormSubmitter.submitTopology("blorf",cfg,topo);
		//StormSubmitter.submitJar(cfg,"../storm-test-topology/target/storm-test-topology-1.0-SNAPSHOT.jar");
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

		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector) {
			
			System.out.println("PrintBolt:prepare called");
		}

		@Override
		public void execute(BatchInfo batchInfo, Tuple tuple) {
			System.out.println("PrintBolt:in execute: "+tuple.getIntegerByField("f1"));
			
		}

		@Override
		public void finishBatch(BatchInfo batchInfo) {
			// TODO Auto-generated method stub
			System.out.println("PrintBolt:in finishbatch");
			
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
