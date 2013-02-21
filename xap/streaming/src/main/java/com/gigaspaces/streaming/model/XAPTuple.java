package com.gigaspaces.streaming.model;

import com.gigaspaces.document.SpaceDocument;

/**
 * Client side construct mainly to differentiate tuples from arbitrary
 * space documents.  On the space side, everything is space documents.
 * XAPTuples should not be read or written directly to a space. 
 * The only requirement of XAPTuples is that they have 
 * a type name that ends in "_tuple".
 * 
 * @author DeWayne
 *
 */
public class XAPTuple extends SpaceDocument{
	public static final String ROUTING_FIELD="_routing";
	/**
	 * Don't use this unless you know what you're doing. Use the tuple
	 * creation methods in the XAPTupleStream class.
	 */
	public XAPTuple(){}
	
	public XAPTuple(String name,int routing){
		if(!name.endsWith("_tuple")){
			throw new IllegalArgumentException("typename must end in _tuple");
		}
		this.setTypeName(name);
		this.setProperty("_routing",new Integer(routing));
	}
		
}
