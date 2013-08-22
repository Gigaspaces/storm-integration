package com.gigaspaces.streaming.util;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.context.GigaSpaceContext;
import org.springframework.stereotype.Component;

import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.streaming.model.XAPStreamConfig;
import com.gigaspaces.streaming.model.XAPTuple;
import com.j_spaces.core.client.SQLQuery;

/**
 * Gathers stream stats and writes the in relevant stream config.
 * TODO -- rethink this.  maybe(certainly) not best site for stats
 * 
 * @author DeWayne
 *
 */
//@Component
public class StreamStatGatherer {
	@GigaSpaceContext
	GigaSpace space;
	Worker worker=null;

	public void postConstruct(){
		//Start worker
		worker=new Worker(space);
		worker.start();
	}
	
	public void preDestroy(){
		//Stop worker
		worker.kill();
		while(worker.isAlive()){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

class Worker extends Thread{
	private boolean alive=true;
	private GigaSpace space=null;
	private SQLQuery<XAPTuple> tupleQuery=new SQLQuery<XAPTuple>(XAPTuple.class,"name=?");
	
	public Worker(GigaSpace space){
		this.space=space;
	}

	@Override
	public void run() {
		while(alive){
			//get stream configs
			XAPStreamConfig[] configs=space.readMultiple(new XAPStreamConfig());
			if(configs!=null){
				for(XAPStreamConfig config:configs){
					//TODO -below is not good.  Tuple name construction not public
					String tname=config.getName()+"_0_tuple";
					XAPTuple tuple=new XAPTuple(tname,config.getRoutingValue());
					int cnt=space.count(tuple);
					ChangeSet cs=new ChangeSet();
					cs.addToCollection("backlog",cnt);
				}
			}
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
			}
		}
	}
	
	public void kill(){
		this.alive=false;
	}
	
}
