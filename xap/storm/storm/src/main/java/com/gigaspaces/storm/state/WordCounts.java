package com.gigaspaces.storm.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import storm.trident.state.State;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.query.IdQuery;

public class WordCounts implements State {
	private GigaSpace space;
	
	public WordCounts(String spaceUrl){
		space=new GigaSpaceConfigurer(new UrlSpaceConfigurer(spaceUrl).space()).gigaSpace();
	}

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
	}

	public void updateCounts(List<String> words,List<Integer> counts){
		ChangeSet cs=new ChangeSet();
		for(int i=0;i<words.size();i++){
			cs.increment("counts."+words.get(i), counts.get(i));
		}
		space.change(new IdQuery<WordCountAggregator>(WordCountAggregator.class,"0"),cs);
	}
	
	@SpaceClass
	public static class WordCountAggregator{
		private Map<String,Integer> counts=new HashMap<String,Integer>();

		public Map<String, Integer> getCounts() {
			return counts;
		}
		public void setCounts(Map<String, Integer> counts) {
			this.counts = counts;
		}
		@SpaceId
		public String getId() {
			return "0";
		}
		public void setId(String id) {
		}

	}
}
