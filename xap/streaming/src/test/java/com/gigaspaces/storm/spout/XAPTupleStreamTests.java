package com.gigaspaces.storm.spout;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider;

import com.gigaspaces.streaming.client.XAPStreamFactory;
import com.gigaspaces.streaming.client.XAPTupleStream;
import com.gigaspaces.streaming.model.XAPStreamBatch;
import com.gigaspaces.streaming.model.XAPStreamConfig;
import com.gigaspaces.streaming.model.XAPTuple;


public class XAPTupleStreamTests{
	static UrlSpaceConfigurer sc;
	static GigaSpace space;
	static ProcessingUnitContainer container;
	static ProcessingUnitContainer container2;
	static XAPStreamFactory fact;

	@BeforeClass
	public static void beforeClass()throws Exception{
		IntegratedProcessingUnitContainerProvider provider = new IntegratedProcessingUnitContainerProvider();
		// provide cluster information for the specific PU instance
		ClusterInfo clusterInfo = new ClusterInfo();
		clusterInfo.setSchema("partitioned-sync2backup");
		clusterInfo.setNumberOfInstances(2);
		clusterInfo.setNumberOfBackups(0);
		clusterInfo.setInstanceId(1);
		provider.setClusterInfo(clusterInfo);

		// set the config location (override the default one - classpath:/META-INF/spring/pu.xml)
		provider.addConfigLocation("classpath:test-pu.xml");

		// Build the Spring application context and "start" it
		container = provider.createContainer();
		clusterInfo.setInstanceId(2);
		container2=provider.createContainer();

		fact=new XAPStreamFactory("jini://localhost/*/space");

		sc=new UrlSpaceConfigurer("jini://localhost/*/space");
		space=new GigaSpaceConfigurer(sc.space()).gigaSpace();
	}

	@AfterClass
	public static void afterClass(){
		try{
			Thread.sleep(500L);
			container.close();
			sc.destroy();
		}
		catch(Exception e){

		}
	}

	@Before
	public void before(){
		space.clear(null);
	}

	@Test
	public void createStream()throws Exception{
		XAPTupleStream stream=fact.createNewTupleStream("mystream",0,Arrays.asList("field1","field2"));
		assertEquals(1,space.count(null));

		Object obj=space.read(new Object());
		assertEquals(XAPStreamConfig.class.getName(), obj.getClass().getName());

		XAPStreamConfig cfg=(XAPStreamConfig)obj;
		assertEquals("mystream",cfg.getName());
	}

	@Test
	public void connectStream()throws Exception{
		XAPTupleStream stream=fact.createNewTupleStream("mystream",0,Arrays.asList("field1","field2"));

		XAPTupleStream stream2=fact.getTupleStream("mystream");
		assertNotNull(stream2);
		assertEquals(1,space.count(null));

		Object obj=space.read(new Object());
		assertEquals(XAPStreamConfig.class.getName(), obj.getClass().getName());

		XAPStreamConfig cfg=(XAPStreamConfig)obj;
		assertEquals("mystream",cfg.getName());

		assertEquals(stream.count(),stream2.count());
	}

	@Test
	public void readWrite()throws Exception{
		XAPTupleStream stream=fact.createNewTupleStream("mystream",0,Arrays.asList("field1","field2"));
		stream.writeBatch(stream.createTuple(),stream.createTuple());
		assertEquals(2,stream.count());

		XAPStreamBatch<XAPTuple> batch=stream.readBatch(1);
		assertNotNull(batch);
		assertNotNull(batch.getEntries());
		assertEquals(1,batch.getEntries().size());

	}

	@Test
	public void batchOps()throws Exception{
		XAPTupleStream stream=fact.createNewTupleStream("mystream",0,Arrays.asList("field1","field2"));

		//write 3
		stream.writeBatch(stream.createTuple(),stream.createTuple(),stream.createTuple());
		//read batch of 2
		XAPStreamBatch<XAPTuple> batch=stream.readBatch(2);
		//clear batch
		stream.clearBatch(batch.getInfo());
		//should be one left
		assertEquals(1,space.count(stream.createTuple()));
	}

	@Test
	public void rereadBatch()throws Exception{
		XAPTupleStream stream=fact.createNewTupleStream("mystream",0,Arrays.asList("field1","field2"));

		//write 3
		stream.writeBatch(stream.createTuple(),stream.createTuple(),stream.createTuple());

		//read batch of 2
		XAPStreamBatch<XAPTuple> batch=stream.readBatch(2);

		List<XAPTuple> list1=new ArrayList<XAPTuple>();
		list1.addAll(batch.getEntries());

		XAPStreamBatch<XAPTuple> batch2=stream.rereadBatch(batch.getInfo());
		List<XAPTuple> list2=new ArrayList<XAPTuple>();
		list2.addAll(batch2.getEntries());

		//compare
		assertEquals(list1.size(),list2.size());
		for(int i=0;i<list1.size();i++){
			assertEquals(list1.get(i).getTypeName(),list2.get(i).getTypeName());
		}
	}


}
