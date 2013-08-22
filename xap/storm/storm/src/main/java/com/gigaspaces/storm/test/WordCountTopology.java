package com.gigaspaces.storm.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.Split;
import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import storm.trident.topology.TridentTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.gigaspaces.storm.spout.XAPConfig;
import com.gigaspaces.storm.spout.XAPTridentSpout;
import com.gigaspaces.storm.state.XAPState;
import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPTuple;

public class WordCountTopology {
    public static class SplitSentence implements ITridentBatchBolt {
    	BatchOutputCollector _collector;
        
        public SplitSentence() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector) {
			_collector=collector;
		}

		@Override
		public void execute(BatchInfo batchInfo, Tuple tuple) {
			for(String word:tuple.getString(0).split("\\w")){
				((List)batchInfo).add(word);
			}
		}

		@Override
		public void finishBatch(BatchInfo batchInfo) {
			_collector.emit((List<Object>)batchInfo.state);
		}

		@Override
		public Object initBatchState(String batchGroup, Object batchId) {
			return new ArrayList<Object>();
		}

		@Override
		public void cleanup() {
			
		}
    }  
    
    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if(count==null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        TridentTopologyBuilder builder = new TridentTopologyBuilder();
        
		XAPStreamFactory fact=new XAPStreamFactory("jini://*/*/streamspace?groups=gigaspaces-9.6.0-XAPPremium-ga");
		XAPTupleStream stream;
		Set<String> streams=fact.listStreams();
		
		if(streams.contains("xapin")){
			stream=fact.getTupleStream("xapin");
			if(stream==null)throw new Exception("stream xapin not found");
			//stream.clear();  //TODO - clear doesn't work
			System.out.println("got existing xapin:"+stream);
		}
		else{
			stream=new XAPStreamFactory("jini://*/*/streamspace?groups=gigaspaces-9.6.0-XAPPremium-ga").createNewTupleStream("xapin",0,Arrays.asList("sentence"));
		}
		XAPTuple t=stream.createTuple();
		t.setProperty("sentence","this is the end beautiful friend");
		stream.writeBatch(t);
                         
        TridentTopology tt=new TridentTopology();
        
        tt.newStream("xapspout",new XAPTridentSpout(new XAPConfig("localhost","xapin",10)))
        .each(new Fields("sentence"),new Split(),new Fields("word"))
        .groupBy(new Fields("word"))
        //.persistentAggregate(new WordCountsFactory("jini://*/*/streamspace?groups=gigaspaces-9.6.0-XAPPremium-ga"), new Count(), new Fields("count"))
        .persistentAggregate(XAPState.nonTransactional("jini://*/*/streamspace?groups=gigaspaces-9.6.0-XAPPremium-ga"), new Count(), new Fields("count"))
        .parallelismHint(4);

        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
        } else {        
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.buildTopology());
        
            Thread.sleep(30000);

            cluster.shutdown();
        }
    }
}
