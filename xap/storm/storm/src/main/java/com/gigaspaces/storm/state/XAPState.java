package com.gigaspaces.storm.state;

import java.util.ArrayList;
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
import com.gigaspaces.client.ReadByIdsResult;

/**
 * A Non transaction XAP state implementation.
 * 
 * TODO - make it support transactions
 * TODO - make it utilize the change API for updates
 * 
 * @author DeWayne
 *
 * @param <T> the type to be stored
 */
public class XAPState<T> implements IBackingMap<T> {
	private static final Logger log=Logger.getLogger(XAPState.class);
	private UrlSpaceConfigurer spaceCfg=null;
	private GigaSpace space=null;
	SnapshottableMap<T> _delegate;	

	public static StateFactory nonTransactional(String url){
		return new Factory(url);
	}

	public static class Factory implements StateFactory{
		String url;

		public Factory(String url){
			this.url=url;
		}

		@Override
		public State makeState(Map conf, int partitionIndex, int numPartitions) {
			// Non-transactional for now  TODO make transactional
			return new SnapshottableMap(NonTransactionalMap.build(new XAPState<Long>(url)), new Values("$GLOBAL$"));
		}

	}

	public XAPState(String url){
		spaceCfg=new UrlSpaceConfigurer(url);
		space=new GigaSpaceConfigurer(spaceCfg.space()).gigaSpace();
	}
	@Override
	public List multiGet(List<List<Object>> keys) {
		List<Object> mkeys=new ArrayList<Object>();
		for(List<Object> list:keys){
			mkeys.add(toSingleKey(list));
		}
		ReadByIdsResult<XAPStateItem> result=space.readByIds(XAPStateItem.class,mkeys.toArray());
		List<T> items=new ArrayList<T>();
		if(result==null)return items;
		for(XAPStateItem<T> item:result.getResultsArray()){
			if(item!=null){
				items.add(item.getValue());
			}
			else{
				items.add(null);
			}
		}
		return items;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		List<XAPStateItem<T>> items=new ArrayList<XAPStateItem<T>>();

		for(int i=0;i<keys.size();i++){
			Object singleKey=toSingleKey(keys.get(i));
			items.add(new XAPStateItem(singleKey,vals.get(i)));
		}
		space.writeMultiple(items.toArray());
	}

	private String toSingleKey(List<Object> key) {
		if(key.size()!=1) {
			throw new RuntimeException("XAP state does not support compound keys");
		}
		return key.get(0).toString();
	}
	
	@SpaceClass
	public static class XAPStateItem<T> {
		private Object key;
		private T value;
		
		public XAPStateItem(){}
		
		public XAPStateItem(Object key,T value){
			this.key=key;
			this.value=value;
		}

		@SpaceId(autoGenerate=false)
		@SpaceRouting
		public Object getKey() {
			return key;
		}
		public void setKey(Object key) {
			this.key = key;
		}
		public T getValue() {
			return value;
		}
		public void setValue(T value) {
			this.value = value;
		}
	}

}
