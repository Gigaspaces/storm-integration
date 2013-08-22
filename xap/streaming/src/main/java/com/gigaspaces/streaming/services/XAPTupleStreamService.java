package com.gigaspaces.streaming.services;

import java.util.List;
import java.util.Set;

import com.gigaspaces.streaming.client.XAPTupleStream;

/**
 * A service definition that provides server side
 * support to the streaming implementation.
 * 
 * @author DeWayne
 *
 */
public interface XAPTupleStreamService {
	/**
	 * Creates a stream
	 *  
	 * @param streamName the name of the stream
	 * @param routingValue the routing value for the stream entries
	 * @param fields the ordered list of fields in the tuples
	 * @return the stream reference
	 */
	XAPTupleStream createStream(String streamName, int routingValue,List<String> fields);
	/**
	 * Opens an existing stream
	 * 
	 * @param streamName the name of the stream to open
	 * @return the stream reference
	 */
	XAPTupleStream openStream(String streamName);
	/**
	 * Destroys a stream
	 * 
	 * @param stream the stream to destroy
	 */
	void destroyTupleStream(XAPTupleStream stream);
	/**
	 * Lists all streams by name
	 * 
	 * @return a list of stream names
	 */
	Set<String> listStreams();
}
