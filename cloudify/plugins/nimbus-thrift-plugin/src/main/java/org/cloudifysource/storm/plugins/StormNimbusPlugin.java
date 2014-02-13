package org.cloudifysource.storm.plugins;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.cloudifysource.domain.context.ServiceContext;
import org.cloudifysource.usm.Plugin;
import org.cloudifysource.usm.UniversalServiceManagerBean;
import org.cloudifysource.usm.dsl.ServiceConfiguration;
import org.cloudifysource.usm.monitors.Monitor;
import org.cloudifysource.usm.monitors.MonitorException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;

public class StormNimbusPlugin implements Plugin,Monitor{
	Logger log=Logger.getLogger(StormNimbusPlugin.class.getName());
	private Map<String,Object> config=null;
	private ServiceContext ctx=null;
	private TSocket thrift;
	private Client nimbus;
	
	@Override
	public void setConfig(Map<String, Object> config) {
		this.config=config;
	}

	@Override
	public void setServiceContext(ServiceContext ctx) {
		this.ctx=ctx;
	}

	@Override
	public Map<String, Number> getMonitorValues(
			UniversalServiceManagerBean ubean, ServiceConfiguration sconf)
			throws MonitorException {
		Map<String,Number> vals=new HashMap<String,Number>();
		
		//get vals
		Client nimbus=getNimbus();
		ClusterSummary sum=null;
		if(nimbus==null)return null;
		try {
			sum=nimbus.getClusterInfo();
		} catch (TException e) {
			log.log(Level.SEVERE,e.getMessage(),e);
			return vals;
		}
		if(sum==null){
			log.warning("null summary returned");
			return vals;
		}
		List<TopologySummary> tops=sum.get_topologies();
		int executors=0,tasks=0,workers=0;
		for(TopologySummary tsum:tops){
			executors+=tsum.get_num_executors();
			tasks+=tsum.get_num_tasks();
			workers+=tsum.get_num_workers();
		}
		
		for(Map.Entry<String,Object> entry:config.entrySet()){
			if(entry.getValue().equals("uptime_secs"))vals.put(entry.getKey(),sum.get_nimbus_uptime_secs());
			if(entry.getValue().equals("topology_count"))vals.put(entry.getKey(),tops.size());
			if(entry.getValue().equals("executor_count"))vals.put(entry.getKey(),executors);
			if(entry.getValue().equals("task_count"))vals.put(entry.getKey(),tasks);
			if(entry.getValue().equals("worker_count"))vals.put(entry.getKey(),workers);
		}
		
		return vals;
	}

	private Client getNimbus(){
		if(nimbus!=null)return nimbus;
		thrift=new TSocket("127.0.0.1",6627);
		try{
			thrift.open();
		}
		catch(Exception e){
			log.log(Level.SEVERE,e.getMessage(),e);
			return null;
		}
		nimbus=new Nimbus.Client.Factory().getClient(new TBinaryProtocol(new TFramedTransport(thrift)));
		return nimbus;
	}

/*	@Override
	public Map<String, Object> getDetails(UniversalServiceManagerBean usmbean,
			ServiceConfiguration sconf) throws DetailsException {
		Client nimbus=getNimbus();
		if(nimbus==null)return null;
		
		Map<String,Object> details=new HashMap<String,Object>();
		
		return null;
	}
*/
}
