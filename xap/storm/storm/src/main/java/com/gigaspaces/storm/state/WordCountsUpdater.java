package com.gigaspaces.storm.state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class WordCountsUpdater extends BaseStateUpdater<WordCounts> {

	@Override
	public void updateState(WordCounts state, List<TridentTuple> tuples,
			TridentCollector collector) {
		List<String> words=new ArrayList<String>();
		List<Integer> counts=new ArrayList<Integer>();

		for(TridentTuple tuple:tuples){
			words.add(tuple.getString(0));
			counts.add(tuple.getInteger(1));
		}
		state.updateCounts(words, counts);
	}

}
