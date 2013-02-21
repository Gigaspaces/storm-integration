package com.gigaspaces.storm.spout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import storm.trident.TridentTopology;
import storm.trident.topology.TridentTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

import com.gigaspaces.storm.misc.XapTest.PrintBolt;
import com.gigaspaces.storm.misc.XapTest.Streamer;
import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;


public class StormTests {

	@Test
	public void mytest()throws Exception{
		StormTopology topo=createTopology();
		Config cfg=new Config();
		cfg.setDebug(true);
		LocalCluster cluster=new LocalCluster();
		new Thread(new Streamer(new XAPStreamFactory("jini://*/*/streamspace").getTupleStream("xapin"))).start();
		cluster.submitTopology("x",cfg,topo);
		StormSubmitter.submitTopology("blorf",cfg,topo);
		StormSubmitter.submitJar(cfg,"../storm-test-topology/target/storm-test-topology-1.0-SNAPSHOT.jar");
		
	}
	
	
	private StormTopology createTopology()throws Exception{
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
	
}
