/*
 * Copyright 2011 GigaSpaces Technologies Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at *
 *     
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License."
 */
package org.openspaces.rest.space;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.openspaces.core.GigaSpace;
import org.openspaces.remoting.scripting.ExecutorScriptingProxyConfigurer;
import org.openspaces.remoting.scripting.ScriptingExecutor;
import org.openspaces.remoting.scripting.StaticScript;
import org.openspaces.rest.utils.ControllerUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 * Spring MVC controller for the RESTful Space API (Task Execution)
 * 
 */
@Controller
@RequestMapping(value = "/rest/task/*")
public class SpaceTaskAPIController {
	private static final Logger log=Logger.getLogger(SpaceTaskAPIController.class.getName());

	public SpaceTaskAPIController(){
		super();
	}

	/**
	 * Execute task on supplied space.  Space must be running the remoting
	 * service exporter
	 * 
	 * @return
	 */
	@RequestMapping(value = "/execute",method=RequestMethod.POST)
	public ModelAndView executeTask(@RequestParam String spaceName,@RequestParam String locators,@RequestBody SpaceTaskRequest body)
	{
		return execute(spaceName,locators,body);
	}

	private ModelAndView execute(String spaceName, String locators,final SpaceTaskRequest request){
		GigaSpace space=ControllerUtils.xapCache.get(spaceName,locators);
		final int instanceCount=ControllerUtils.xapCache.getInstances(spaceName);
		ExecutorService svc=Executors.newFixedThreadPool(instanceCount);
		int instances=0;

		log.fine("request.target="+request.target);
		if(request.target!=null && !request.target.equals("all")){
			instances=1;
		}
		else{
			instances=instanceCount;
		}
		
		System.out.println("instances="+instances);
		
		List<Callable<Object>> tasks=new ArrayList<Callable<Object>>(instances);
		for(int i=0;i<instances;i++){
			Object routing=0;
			if(request.target!=null && request.target.equals("all")){
				routing=i;
			}
			else{
				routing=request.target;
			}
			tasks.add(new ScriptCallable(space,request,routing));
		}

		ModelAndView mv=new ModelAndView("jsonView");
		List<Object> model=new ArrayList<Object>(instances);
		try{
			List<Future<Object>> results=svc.invokeAll(tasks);

			for(Future<Object> fut:results){
				if(fut.get()!=null)model.add(fut.get());
			}
			mv.addObject("results",model);
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
		finally{
			svc.shutdownNow();
		}
		return mv;
	}


	protected static class SpaceTaskRequest{
		public Object target;
		public String language;
		public String code;
		public Object getTarget() {
			return target;
		}
		public void setTarget(Object target) {
			this.target = target;
		}
		public String getLanguage() {
			return language;
		}
		public void setLanguage(String language) {
			this.language = language;
		}
		public String getCode() {
			return code;
		}
		public void setCode(String code) {
			this.code = code;
		}
		
		
	}

	private static class ScriptCallable implements Callable<Object>{
		private final GigaSpace space;
		private final StaticScript script;
		
		public ScriptCallable(final GigaSpace space,final SpaceTaskRequest req, Object routing){
			this.space=space;
			script=new StaticScript("resttask"+UUID.randomUUID().toString(),req.language,req.code);
			script.routing(routing);
		}
		
		@Override
		public Object call() throws Exception {
			ScriptingExecutor<Object>  executor = new ExecutorScriptingProxyConfigurer<Object>(space)
					.scriptingExecutor();

			return executor.execute(script);
		}
	}
	

}
