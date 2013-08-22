package com.gigaspaces.streaming.model;

import java.util.List;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.streaming.util.FixedNumberQueue;

/**
 * Simple space class that holds stream metadata.  Instances of this class
 * define a stream in a space.  
 * 
 * @author DeWayne
 *
 */

@SpaceClass
public class XAPStreamConfig{
	private String name;
	private Integer routingValue;
	private List<String> fields;
	private FixedNumberQueue<Integer> backlog=new FixedNumberQueue<Integer>(10);

	@SpaceId
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@SpaceRouting
	public Integer getRoutingValue(){
		return routingValue;
	}
	public void setRoutingValue(Integer val){
		if(this.routingValue!=null)throw new RuntimeException("routing value can't be changed");
		this.routingValue=val;
	}
	/*
	 * The fields field provides tuple ordering.  Tuples are by nature
	 * ordered, space document fields are not.
	 */
	public void setFields(List<String> fields) {
		this.fields = fields;
	}
	public List<String> getFields() {
		return fields;
	}
	public FixedNumberQueue<Integer> getBacklog() {
		return backlog;
	}
	public void setBacklog(FixedNumberQueue<Integer> backlog) {
		this.backlog = backlog;
	}
}
