package com.gigaspaces.storm;

import java.util.logging.Logger;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.gigaspaces.storm.spout.XAPConfig;
import com.gigaspaces.storm.spout.XAPTridentSpout;
import com.gigaspaces.storm.state.XAPState;

public class WordCountTopology {
	private static final Logger log=Logger.getLogger(WordCountTopology.class.getName());
	
    public static void main(String[] args) throws Exception {
    	
    	if(args.length<3)throw new RuntimeException("requires 3 args: toponame, host, streamName [workerCnt]");
    	
    	String topoName=args[0];
		String xaphost=args[1];
		String streamName=args[2];
		int workerCnt=4;
		if(args.length>3)workerCnt=Integer.parseInt(args[3]);
		
		log.info(String.format("executing wordcount with %s %s %s",topoName,xaphost,streamName));
		
		XAPConfig config=new XAPConfig();
		config.setBatchSize(1000);
		config.setStreamName(streamName);
		config.setXapHost(xaphost);
		config.setFields("sentence");
		
        Config conf = new Config();
        //conf.setDebug(true);
        
        XAPTridentSpout spout=new XAPTridentSpout(config);
        
        TridentTopology topology = new TridentTopology();        
        TridentState wordCounts =
              topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new SplitLarge(3), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(XAPState.nonTransactional(String.format("jini://*/*/streamspace?locators=%s",xaphost)), new Count(), new Fields("count"))
                ;
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(workerCnt);
            StormSubmitter.submitTopology(topoName, conf, topology.build());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, topology.build());
        
            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
    
}

/**
 * Splits and filters out small words, and removes punctuation
 * 
 * @author DeWayne
 *
 */
class SplitLarge extends BaseFunction {
	
	private int size;

	public SplitLarge(){}
	
	public SplitLarge(int size){
		this.size=size;
	}

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        for(String word: tuple.getString(0).split("[ ;,\\?\\-\\\"\\!]+")) {
            if(word.length() > size) {
                collector.emit(new Values(word.toLowerCase()));
            }
        }
    }
    
}


