package org.openspaces.rest.space;

import static org.junit.Assert.*;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider;
import org.openspaces.rest.space.SpaceTaskAPIController.SpaceTaskRequest;
import org.springframework.web.servlet.ModelAndView;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class SpaceTaskAPIControllerTest {
	static UrlSpaceConfigurer sc;
	static GigaSpace space;  //clustered space proxy
	static ClusterInfo info;
	static ProcessingUnitContainer container;
	static ProcessingUnitContainer container2;

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
		info=clusterInfo;

		// set the config location (override the default one - classpath:/META-INF/spring/pu.xml)
		provider.addConfigLocation("classpath:test-pu.xml");

		// Build the Spring application context and "start" it
		container = provider.createContainer();
		clusterInfo.setInstanceId(2);
		container2=provider.createContainer();

		sc=new UrlSpaceConfigurer("jini://localhost/*/space");
		space=new GigaSpaceConfigurer(sc.space()).gigaSpace().getClustered();
		
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
	
	/*
	 * 
	 * ------------------------    TESTS -----------------------
	 */
	
	@Test
	public void sanityTest() {
		SpaceTaskAPIController controller=new SpaceTaskAPIController();
		SpaceTaskRequest body=new SpaceTaskRequest();
		
		body.language="groovy";
		body.target="all";
		body.code="println \"here\";return \"success\";";
		ModelAndView result=controller.executeTask("space","localhost",body);
		assertEquals(1,result.getModel().size());
		
	}
	
	@Test
	public void writeToSpaceTest(){
		space.clear(new Object());
		
		SpaceTaskAPIController controller=new SpaceTaskAPIController();
		SpaceTaskRequest body=new SpaceTaskRequest();
		
		body.language="groovy";
		body.target="all";
		body.code="gigaSpace.write(new String(\"test\"));return \"success\";";
		ModelAndView result=controller.executeTask("space","localhost",body);
		assertEquals(2,space.count(new Object()));
	}

	@Test
	public void readSpaceBroadcastTest(){
		space.getTypeManager().registerTypeDescriptor(new SpaceTypeDescriptorBuilder("test-type")
				.idProperty("id",false).addFixedProperty("val",String.class).create());
		space.clear(new Object());
		SpaceDocument d=new SpaceDocument("test-type");
		d.setProperty("id",0);
		d.setProperty("val","val0");
		space.write(d);
		d.setProperty("id",1);
		d.setProperty("val","val1");
		space.write(d);
		
		SpaceTaskAPIController controller=new SpaceTaskAPIController();
		SpaceTaskRequest body=new SpaceTaskRequest();
		
		body.language="groovy";
		body.target="all";
		body.code="import com.gigaspaces.document.*;"+
				"def newdoc=gigaSpace.read(new SpaceDocument(\"test-type\"));"+
				"return newdoc.getProperty(\"val\");";
		ModelAndView result=controller.executeTask("space","localhost",body);
		assertEquals(1,result.getModel().size());
		assertEquals(2,((List)result.getModel().get("results")).size());
	}
	
	@Test
	public void readSpaceSinglePartitionTest(){
		space.getTypeManager().registerTypeDescriptor(new SpaceTypeDescriptorBuilder("test-type")
				.idProperty("id",false).addFixedProperty("val",String.class).create());
		space.clear(new Object());
		SpaceDocument d=new SpaceDocument("test-type");
		d.setProperty("id",0);
		d.setProperty("val","val0");
		space.write(d);
		d.setProperty("id",1);
		d.setProperty("val","val1");
		space.write(d);
		
		SpaceTaskAPIController controller=new SpaceTaskAPIController();
		SpaceTaskRequest body=new SpaceTaskRequest();
		
		body.language="groovy";
		body.target=0;
		body.code="import com.gigaspaces.document.*;"+
				"def newdoc=gigaSpace.read(new SpaceDocument(\"test-type\"));"+
				"return newdoc.getProperty(\"val\");";
		ModelAndView result=controller.executeTask("space","localhost",body);
		assertEquals(1,result.getModel().size());
		assertEquals(1,((List)result.getModel().get("results")).size());
		
		ObjectMapper om=new ObjectMapper();
		try{
		 System.out.println(om.writeValueAsString(result.getModel().get("results")));
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
}

