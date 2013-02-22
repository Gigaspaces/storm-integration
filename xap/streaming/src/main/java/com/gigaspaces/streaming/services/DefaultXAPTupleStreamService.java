package com.gigaspaces.streaming.services;

import java.util.ArrayList;
import java.util.List;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceTypeManager;
import org.openspaces.core.context.GigaSpaceContext;
import org.openspaces.remoting.RemotingService;
import org.openspaces.remoting.Routing;
import org.springframework.transaction.annotation.Transactional;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPStreamConfig;
import com.gigaspaces.streaming.model.XAPTuple;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.client.UpdateModifiers;

@RemotingService
public class DefaultXAPTupleStreamService implements XAPTupleStreamService{
	@GigaSpaceContext
	private GigaSpace space;

	@Override
	@Transactional
	public XAPTupleStream createStream(@Routing String streamName, int routingValue,List<String> fields){
		XAPTupleStream newstream=null;

		if(streamExists(streamName))throw new IllegalArgumentException("stream already exists");

		newstream=new XAPTupleStream(space.getSpace().getURL().toString(),streamName);
		newstream.setInstance(0);
		
		//register tuple types
		registerTupleTypeFromFields(streamName,0,fields);

		//create stream config
		XAPStreamConfig cfg=new XAPStreamConfig();
		cfg.setName(streamName);
		cfg.setRoutingValue(routingValue);
		cfg.setFields(fields);
		space.write(cfg,LeaseContext.FOREVER,1000L,UpdateModifiers.WRITE_ONLY);

		return newstream;
	}

	@Override
	public XAPTupleStream openStream(@Routing String streamName) {
		XAPStreamConfig cfg=space.readById(XAPStreamConfig.class,streamName);
		if(cfg==null)return null;
		return new XAPTupleStream(space.getSpace().getURL().toString(),streamName);
	}

	@Override
	public List<String> listStreams() {
		List<String> list=new ArrayList<String>();
		
		XAPStreamConfig template=new XAPStreamConfig();
		XAPStreamConfig[] cfgs=space.readMultiple(template);
		for(XAPStreamConfig cfg:cfgs)list.add(cfg.getName());
		return list;
	}

	@Override
	@Transactional
	public void destroyTupleStream(@Routing("getName") XAPTupleStream stream) {
		
		XAPStreamConfig cfgtemplate=new XAPStreamConfig();
		cfgtemplate.setName(stream.getName());
		space.clear(cfgtemplate); //clear config
		
		XAPTuple template=new XAPTuple();
		template.setTypeName(stream.getTupleTypeName());
		space.clear(template); //clear tuples
		
	}
	
	private boolean streamExists(String streamName){
		XAPStreamConfig template=new XAPStreamConfig();
		template.setName(streamName);
		return space.read(template,100L)!=null;
	}
	
	/**
	 * Registers a new tuple type.  By default, the document id is used as the routing id, 
	 * @param streamName 
	 * @param instanceId - not implemented
	 * @param fields - fields on the 
	 * @param routing
	 * @return
	 */
	private String registerTupleTypeFromFields(String streamName,Integer instanceId,List<String> fields){
		GigaSpaceTypeManager man=space.getClustered().getTypeManager();
		
		String tupleName=streamName+"_"+instanceId+"_tuple";
		SpaceTypeDescriptorBuilder descbuilder=new SpaceTypeDescriptorBuilder(tupleName).
		documentWrapperClass(XAPTuple.class).fifoSupport(FifoSupport.ALL);
		descbuilder.addFixedProperty(XAPTuple.ROUTING_FIELD, Integer.class);
		descbuilder.routingProperty(XAPTuple.ROUTING_FIELD);
		for(int i=0;i<fields.size();i++){
			if(fields.get(i).equals(XAPTuple.ROUTING_FIELD))throw new IllegalArgumentException("invalid field name:"+XAPTuple.ROUTING_FIELD);
			descbuilder.addFixedProperty(fields.get(i),Object.class);
		}
		man.registerTypeDescriptor(descbuilder.create());
		return tupleName;
	}
}
