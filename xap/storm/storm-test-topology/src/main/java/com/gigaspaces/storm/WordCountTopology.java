package com.gigaspaces.storm;

import java.util.logging.Logger;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;

import com.gigaspaces.storm.spout.XAPConfig;
import com.gigaspaces.storm.spout.XAPTridentSpout;

public class WordCountTopology {
	private static final Logger log=Logger.getLogger(WordCountTopology.class.getName());
	
    public static void main(String[] args) throws Exception {
    	if(args.length!=3)throw new RuntimeException("requires 3 args: toponame, host, streamName");
    	
    	String topoName=args[0];
		String xaphost=args[1];
		String streamName=args[2];
		
		log.info(String.format("executing wordcount with %s %s %s",topoName,xaphost,streamName));
		
		XAPConfig config=new XAPConfig();
		config.setBatchSize(100);
		config.setHost(xaphost);
		config.setSpaceName("streamspace");
		config.setStreamName(streamName);
		
        Config conf = new Config();
        conf.setDebug(true);
        
        XAPTridentSpout spout=new XAPTridentSpout(config);
        
        TridentTopology topology = new TridentTopology();        
        TridentState wordCounts =
              topology.newStream("spout1", spout)
                .parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
//                .persistentAggregate(new XAPTridentState.XAPTridentStateFactory(xaphost, "streamspace"),new Count(),new Fields("count"))
                .persistentAggregate(new MemoryMapState.Factory(),
                                     new Count(), new Fields("count"))         
                .parallelismHint(16);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, topology.build());
        
            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
