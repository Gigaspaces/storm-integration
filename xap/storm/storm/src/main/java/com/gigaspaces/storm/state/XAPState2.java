package com.gigaspaces.storm.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.SnapshottableMap;
import backtype.storm.tuple.Values;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.storm.perf.PerfStats;

/**
 * This is a change API example state provider.  It is NOT general purpose, and
 * only does simple accumulations
 * 
 * @author DeWayne
 *
 * @param <T> the type to be stored
 */
public class XAPState2 implements IBackingMap<Long> {
	private static final Logger log=Logger.getLogger(XAPState2.class);
	private UrlSpaceConfigurer spaceCfg=null;
	ThreadLocal<Long> start=new ThreadLocal<Long>();
	private GigaSpace space=null;
	SnapshottableMap<Long> _delegate;
	private boolean collectStats;	

	public static StateFactory nonTransactional(String url,boolean collectStats){
		return new Factory(url,collectStats);
	}

	public static class Factory implements StateFactory{
		String url;
		private boolean collectStats;

		public Factory(String url, boolean collectStats){
			this.url=url;
			this.collectStats=collectStats;
		}

		@Override
		public State makeState(Map conf, int partitionIndex, int numPartitions) {
			// Non-transactional for now  TODO make transactional
			return new SnapshottableMap(NonTransactionalMap.build(new XAPState2(url,collectStats)), new Values("$GLOBAL$"));
		}

	}

	public XAPState2(String url,boolean collectStats){
		this.collectStats=collectStats;
		spaceCfg=new UrlSpaceConfigurer(url);
		space=new GigaSpaceConfigurer(spaceCfg.space()).gigaSpace();
		try{
			XAPStateMap statemap=new XAPStateMap();
			statemap.items=new HashMap<Object,Long>();
			space.write(statemap,WriteModifiers.WRITE_ONLY);
		}catch(Exception e){
			//ignore
		}
	}
	@Override
	public List multiGet(List<List<Object>> keys) {
		//No need to read values since change api will be updating it
		if(collectStats)start.set(System.currentTimeMillis());
		final List<Long> items=new ArrayList<Long>(keys.size());
		for(List<Object> key:keys){
			items.add(0L);
		}
		return items;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<Long> vals) {
		final ChangeSet cs=new ChangeSet();

		for(int i=0;i<keys.size();i++){
			Serializable singleKey=toSingleKey(keys.get(i));
			cs.increment("items."+singleKey, vals.get(i));
		}
		space.change(new IdQuery<XAPStateMap>(XAPStateMap.class,0,0), cs, ChangeModifiers.ONE_WAY);
		if(collectStats)updateStats(System.currentTimeMillis()-start.get());
	}
	
	// Note: relies on spout to create PerfStats object
	private void updateStats(long l) {
		if(!collectStats)return;
		ChangeSet cs=new ChangeSet();
		cs.addToCollection("writeLatency", (int)l);
		try{
			space.change(new IdQuery<PerfStats>(PerfStats.class,0), cs, ChangeModifiers.ONE_WAY);
		}
		catch(Exception e){}
	}
	
	private String toSingleKey(List<Object> key) {
		if(key.size()!=1) {
			throw new RuntimeException("XAP state does not support compound keys");
		}
		return key.get(0).toString();
	}
	
	@SpaceClass
	public static class XAPStateMap {
		private Integer id=null;
		private Map<Object, Long> items=null;
		
		public XAPStateMap(){}
		
		@SpaceId(autoGenerate=false)
		@SpaceRouting
		public Integer getId() {
			return 0;
		}
		public void setId(Integer id) {
			//noop
		}

		public Map<Object, Long> getItems() {
			return items;
		}

		public void setItems(Map<Object, Long> items) {
			this.items = items;
		}
	}

}
