package com.gigaspaces.storm.state;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class WordCountsFactory implements StateFactory{

	@Override
	public State makeState(Map conf, int partitionIndex, int numPartitions) {
		return new WordCounts((String)conf.get("spaceUrl"));
	}

}
