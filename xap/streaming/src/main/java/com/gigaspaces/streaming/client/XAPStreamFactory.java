package com.gigaspaces.streaming.client;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.remoting.ExecutorRemotingProxyConfigurer;

import com.gigaspaces.streaming.model.XAPStreamConfig;
import com.gigaspaces.streaming.model.XAPTuple;
import com.gigaspaces.streaming.services.XAPTupleStreamService;

/**
 * Construct streams.  Encapsulates RPC calls to XAP
 * service(s) in spaces hosting streams.
 * 
 * @author DeWayne
 *
 */
public class XAPStreamFactory {
	private static final Logger log=Logger.getLogger(XAPStreamFactory.class.getName());
	private GigaSpace space;
	private XAPTupleStreamService svc;
	private String spaceUrl;
	
	public XAPStreamFactory(String spaceUrl){
		this.space=new GigaSpaceConfigurer(new UrlSpaceConfigurer(spaceUrl).space()).gigaSpace();
		this.spaceUrl=spaceUrl;
		svc=new ExecutorRemotingProxyConfigurer<XAPTupleStreamService>(space,XAPTupleStreamService.class).proxy();
	}
	
	/**
	 * Creates a new Stream with the supplied name.  Fails if a stream
	 * with the same name exists
	 * 
	 * @param streamName the name of the stream to create
	 * @param routingValue determines partition location of stream
	 * @param fields the fields in a tuple.  Tuple fields ordered by this list.
	 * @return the stream reference
	 */
	public XAPTupleStream createNewTupleStream(String streamName,int routingValue,List<String> fields){
		XAPTupleStream newstream=svc.createStream(streamName, routingValue, fields);
		newstream.setSpace(space);
		return newstream;
	}
	
	/**
	 * Lists streams in the space
	 * 
	 * @return
	 */
	public Set<String> listStreams(){
		Set<String> streams=svc.listStreams();
		if(streams==null){
			log.severe("listStreams returned null");
			return null;
		}
		if(log.isLoggable(Level.FINE)){
			log.fine("listStreams called. Found streams:");
			int i=0;
			for(Iterator<String> it=streams.iterator();it.hasNext();){
				log.fine(String.format("  %3d - %s",i++,it.next()));
			}
		}
		return svc.listStreams();
	}

	/**
	 * Gets a reference to an existing stream. This method ignores
	 * the stream instance concept (default to 0).  TBD, implement
	 * stream instances.
	 * 
	 * @param streamName
	 * @return
	 */
	public XAPTupleStream getTupleStream(String streamName)throws Exception{
		XAPTupleStream stream=svc.openStream(streamName);
		if(stream==null){
			log.warning(String.format("stream '%s' not found",streamName));
			return null;
		}
		log.info("opened stream "+streamName);
		stream.setSpace(space);
		return stream;
	}

	/**
	 * Destroys the stream configuration and deletes related tuples.
	 * 
	 * @param stream
	 */
	public void destroyTupleStream(XAPTupleStream stream) {
		XAPStreamConfig cfgtemplate=new XAPStreamConfig();
		cfgtemplate.setName(stream.getName());
		space.clear(cfgtemplate); //clear config
		
		XAPTuple template=new XAPTuple();
		template.setTypeName(stream.getTupleTypeName());
		space.clear(template); //clear tuples
		
	}

	public static void main(String[] args){
	}
	
}
