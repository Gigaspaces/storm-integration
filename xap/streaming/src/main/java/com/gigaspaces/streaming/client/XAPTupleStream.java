package com.gigaspaces.streaming.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.gigaspaces.streaming.model.XAPStream;
import com.gigaspaces.streaming.model.XAPStreamBatch;
import com.gigaspaces.streaming.model.XAPStreamBatch.BatchInfo;
import com.gigaspaces.streaming.model.XAPStreamConfig;
import com.gigaspaces.streaming.model.XAPTuple;
import com.j_spaces.core.client.TakeModifiers;

/**
 * Represents a stream connection.  tuple type names are constructed
 * from a name + "__" + instance id.  This to allow multiple streams
 * with the same StreamDef in the same space.  A XAP Stream is essentially
 * a wrapper around the GigaSpace interface to provide a stream oriented
 * API.  Currently haven't implements stream instances:hardcoded to 0
 * 
 * @author DeWayne
 *
 */

public class XAPTupleStream implements XAPStream<XAPTuple>{
	private static final long serialVersionUID = 1L;
	private static final Logger log=Logger.getLogger(XAPTupleStream.class);
	private transient GigaSpace space;
	private String spaceUrl;
	private String tupleTypeName;
	private String name;
	private Integer instance=0;
	private transient XAPStreamConfig streamConfig;

	public XAPTupleStream(){}

	public XAPTupleStream(String spaceUrl,String name){
		if(spaceUrl==null)throw new IllegalArgumentException("null space url supplied");
		if(name==null || name.length()==0)throw new IllegalArgumentException("null or 0 length name supplied");
		if(name.contains("_"))throw new IllegalArgumentException("names can't have underscores");
		this.spaceUrl=spaceUrl;
		this.name=name;
		this.tupleTypeName=name+"_0_tuple";
	}

	/**
	 * Convenience to create empty tuples for this stream
	 * @return
	 */
	public XAPTuple createTuple(){
		return new XAPTuple(tupleTypeName,getStreamConfig().getRoutingValue());
	}

	private XAPStreamConfig getStreamConfig(){
		if(streamConfig==null){
			GigaSpace space=getSpace();
			XAPStreamConfig template=new XAPStreamConfig();
			template.setName(name);
			template=space.read(template);
			if(template==null)throw new RuntimeException("stream config not found for: "+name);
			streamConfig=template;
		}
		return streamConfig;
	}

	/** 
	 * Write tuples to this stream.  Assumes tuples are of the same type
	 * 
	 * @param tuple
	 */
	@Override
	public void writeBatch(XAPTuple...tuple){
		getSpace().writeMultiple(tuple);
	}

	@Override
	public XAPStreamBatch<XAPTuple> readBatch(int max){
		XAPStreamBatch<XAPTuple> batch=new XAPStreamBatch<XAPTuple>();
		XAPTuple template=new XAPTuple();
		template.setTypeName(name+"_"+instance+"_tuple");
		
		XAPTuple[] tuples=(XAPTuple[])getSpace().readMultiple(template,max);
		if(tuples!=null){
			batch.setEntries(Arrays.asList(tuples));
			batch.setInfo(new BatchInfo(template.getTypeName(),tuples.length));
		}
		return batch;
	}

	@Override
	public XAPStreamBatch<XAPTuple> rereadBatch(BatchInfo info){
		XAPStreamBatch<XAPTuple> batch=new XAPStreamBatch<XAPTuple>();

		XAPTuple template=new XAPTuple();
		template.setTypeName(info.entryTypeName);
		XAPTuple[] tuples=getSpace().readMultiple(template,info.qty);
		if(tuples!=null){
			if(tuples.length!=info.qty)throw new RuntimeException(String.format("unable to reread batch: wanted %d, got %d entries ",info.qty,tuples.length));
			batch.setInfo(info);
			batch.setEntries(Arrays.asList(tuples));
		}
		return batch;
	}

	@Override
	public void clearBatch(BatchInfo info){
		if(info==null)throw new IllegalArgumentException("null info supplied");
		XAPTuple template=new XAPTuple();
		template.setTypeName(info.entryTypeName);
		XAPTuple[] tuples=getSpace().takeMultiple(template,info.qty,TakeModifiers.FIFO);
		if(tuples!=null){
			if(tuples.length!=info.qty)throw new RuntimeException(String.format("unable to clear batch: wanted %d, got %d entries ",info.qty,tuples.length));
			return;
		}
		throw new RuntimeException("batch clear failed: null returned from take");
	}

	@Override
	public int count() {
		XAPTuple template=new XAPTuple();
		template.setTypeName(this.tupleTypeName);
		return getSpace().count(template);
	}

	@Override
	public void clear() {
		XAPTuple template=new XAPTuple();
		template.setTypeName(tupleTypeName);
		getSpace().clear(template);
	}

	/// GETTERS/SETTERS

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}
	public String getTupleTypeName() {
		return tupleTypeName;
	}

	public GigaSpace getSpace() {
		// do lazy connect here
		if(space==null){
			log.debug("connecting to: "+spaceUrl);
			space=new GigaSpaceConfigurer(new UrlSpaceConfigurer(spaceUrl).space()).gigaSpace();
		}
		return space;
	}

	public void setSpace(GigaSpace space) {
		this.space = space;
	}

	public void setInstance(Integer instance) {
		this.instance = instance;
	}

	public Integer getInstance() {
		return instance;
	}

	public List<String> getFieldNames(){
		List<String> list=new ArrayList<String>();

		GigaSpace space=getSpace();
		XAPStreamConfig template=new XAPStreamConfig();
		template.setName(name);
		template=space.read(template);
		if(template==null)throw new RuntimeException("stream config not found for :"+name);
		return template.getFields();
	}

	public static void main(String[] args)throws Exception{
	}



}
