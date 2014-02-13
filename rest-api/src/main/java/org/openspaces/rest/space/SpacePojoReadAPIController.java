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


import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import org.openspaces.core.GigaSpace;
import org.openspaces.rest.utils.ControllerUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import com.j_spaces.core.client.SQLQuery;

/**
 * Spring MVC controller for the RESTful Space API (POJO READS)
 * 
 */
@Controller
@RequestMapping(value = "/rest/pojo/*")
public class SpacePojoReadAPIController {
	private static final Logger log=Logger.getLogger(SpacePojoReadAPIController.class.getName());
	private static final Logger logger = Logger.getLogger(SpacePojoReadAPIController.class.getName());
	
	/**
	 * REST ReadMultiple by query request handler - readMultiple
	 * 
	 */
	@RequestMapping(value = "/readMultiple", method = RequestMethod.GET)
	public ModelAndView readMultiple(
			@RequestParam String spaceName, @RequestParam String locators,
	        @RequestParam String classname, @RequestParam Integer max, @RequestParam String query, HttpServletResponse response){
	    if(logger.isLoggable(Level.FINE))
	        logger.fine("creating read query with type: " +  classname + " and query="+query );
	    
	    Object template;
		try {
			template = Class.forName(classname).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		Object[] objs=null;
		if(max==null)max=Integer.MAX_VALUE;
		
		GigaSpace gigaSpace=ControllerUtils.xapCache.get(spaceName,locators);
		
		if(query==null ||query.length()==0){
			objs=gigaSpace.readMultiple(template,max);
		}
		else{
			objs=gigaSpace.readMultiple(new SQLQuery(template.getClass(),query),max);
		}
	    
	    ModelAndView mv=new ModelAndView("jsonView");
	    if(objs!=null){
	    	int i=0;
	    	for(Object obj:objs){
	    		i++;
	    		mv.addObject(String.valueOf(i),obj);
	    	}
	    }
	    response.setHeader("Access-Control-Allow-Origin","*");
        return mv;
	}

	/**
	 * REST ReadById by query request handler - readMultiple
	 * 
	 */
	@RequestMapping(value = "/readById", method = RequestMethod.GET)
	public ModelAndView readById(
			@RequestParam String spaceName, @RequestParam String locators,
	        @RequestParam String classname, @RequestParam String id, @RequestParam String idClass,
	        @RequestParam String routing, @RequestParam String routingClass, HttpServletResponse response)throws Exception{

		log.fine(String.format("readById called params: classname=%s id=%s idClass=%s routing=%s routingClass=%s",
				classname,id,idClass,routing,routingClass));
		
		Class<?> _valueClass=Class.forName(classname);
	    Class<?> _idClass=Class.forName(idClass);
	    Class<?> _routingClass=Class.forName(routingClass);

		Object idobj=_idClass.getConstructor(String.class).newInstance(id);
		Object routingobj=_routingClass.getConstructor(String.class).newInstance(routing);
		GigaSpace gigaSpace=ControllerUtils.xapCache.get(spaceName,locators);
		log.fine(String.format("reading: gigaSpace=%s _valueClass=%s idobj=%s routingobj=%s",
				gigaSpace,_valueClass,idobj,routingobj));
		Object obj=gigaSpace.readById(_valueClass,idobj,routingobj);
	    
	    ModelAndView mv=new ModelAndView("jsonView");
	    if(obj!=null){
    		mv.addObject(id,obj);
	    }
	    response.setHeader("Access-Control-Allow-Origin","*");
        return mv;
	}
	
	/**
	 * REST COUNT -  Unrestricted for now
	 * 
	 * TODO: add query body
	 * 
	 */
	@RequestMapping(value = "/count", method = RequestMethod.GET)
	public ModelAndView count(
			@RequestParam String spaceName, @RequestParam String locators,
	        @RequestParam String classname, HttpServletResponse response){
	    if(logger.isLoggable(Level.FINE))
	        logger.fine("creating read query with type: " +  classname );
	    
	    Object template;
		try {
			template = Class.forName(classname).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		GigaSpace gigaSpace=ControllerUtils.xapCache.get(spaceName,locators);

	    Integer cnt=gigaSpace.count(template);
	    
	    ModelAndView mv=new ModelAndView("jsonView");
	    if(cnt!=null){
	    	mv.addObject("count",cnt);
	    }
	    response.setHeader("Access-Control-Allow-Origin","*");
        return mv;
	}
	

}
