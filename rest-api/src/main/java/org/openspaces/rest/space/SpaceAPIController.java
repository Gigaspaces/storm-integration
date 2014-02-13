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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import net.jini.core.lease.Lease;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.rest.exceptions.ObjectNotFoundException;
import org.openspaces.rest.exceptions.TypeNotFoundException;
import org.openspaces.rest.utils.ControllerUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.QueryResultType;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.UpdateModifiers;

/**
 * Spring MVC controller for the RESTful Space API
 * 
 * usage examples:
 * 	GET:
 * 		http://localhost:8080/rest/data/Item/1
 * 		http://192.168.9.47:8080/rest/data/Item/_criteria?q=data2='common'
 * 
 *      Limit result size: 
 *      http://192.168.9.47:8080/rest/data/Item/_criteria?q=data2='common'&s=10
 * 
 *  DELETE:
 *  	curl -XDELETE http://localhost:8080/rest/data/Item/1 
 *  	curl -XDELETE http://localhost:8080/rest/data/Item/_criteria?q=id=1
 *  
 *      Limit result size:
 *      curl -XDELETE http://localhost:8080/rest/data/Item/_criteria?q=data2='common'&s=5
 * 
 *  POST:
 *  	curl -XPOST -d '[{"id":"1", "data":"testdata", "data2":"common", "nestedData" : {"nestedKey1":"nestedValue1"}}, {"id":"2", "data":"testdata2", "data2":"common", "nestedData" : {"nestedKey2":"nestedValue2"}}, {"id":"3", "data":"testdata3", "data2":"common", "nestedData" : {"nestedKey3":"nestedValue3"}}]' http://localhost:8080/rest/data/Item
 * 
 *  PUT:
 *  	curl -XPUT -d '{"id":"1", "data":"testdata", "data2":"commonUpdated", "nestedData" : {"nestedKey1":"nestedValue1Updated"}}' http://192.168.9.47:8080/rest/data/Item
 *  
 * @author rafi
 * @since 8.0
 */
@Controller
@RequestMapping(value = "/rest/data/*")
public class SpaceAPIController {

	private static final String CRITERIA_KEYWORD = "_criteria";
	private static final String QUERY_PARAM = "q";
	private static final String SIZE_PARAM = "s";
	private static final String SPACE_PARAM="space";
	private static final String LOCATORS_PARAM="locators";

	private int maxReturnValues = Integer.MAX_VALUE;
	private final Map<String,GigaSpace> connectionCache=new HashMap<String,GigaSpace>();
	private UrlSpaceConfigurer scfg=null;
	private static final Logger logger = Logger.getLogger(SpaceAPIController.class.getName());

	/**
	 * redirects to index view
	 * @return
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public ModelAndView redirectToIndex(){
		return new ModelAndView("index");
	}

	/**
	 * REST GET by query request handler
	 * 
	 * @param type
	 * @param query
	 * @return
	 * @throws ObjectNotFoundException
	 */
	@RequestMapping(value = "/{type}/" + CRITERIA_KEYWORD, method = RequestMethod.GET, params=QUERY_PARAM)
	public @ResponseBody Map<String, Object>[] getByQuery(
			@PathVariable String type, 
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@RequestParam(value=QUERY_PARAM) String query,
			@RequestParam(value=SIZE_PARAM, required=false) Integer size) throws ObjectNotFoundException{
		if(logger.isLoggable(Level.FINE))
			logger.fine("creating read query with type: " + type + " and query: " + query);

		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		SQLQuery<SpaceDocument> sqlQuery = new SQLQuery<SpaceDocument>(type, query, QueryResultType.DOCUMENT);
		int maxSize = (size==null ? maxReturnValues : size.intValue());
		SpaceDocument[] docs;
		try {
			docs = gigaSpace.readMultiple(sqlQuery, maxSize);
		} catch (DataAccessException e) {
			throw translateDataAccessException(gigaSpace,e, type);
		}

		Map<String, Object>[] result;
		if (docs == null || docs.length == 0){
			throw new ObjectNotFoundException("no objects matched the criteria");
		}
		result = ControllerUtils.createPropertiesResult(docs);
		return result;
	}


	/**
	 * REST GET by ID request handler
	 * 
	 * @param type
	 * @param id
	 * @return
	 * @throws ObjectNotFoundException
	 * @throws UnknownTypeException 
	 */
	@RequestMapping(value = "/{type}/{id}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object> getById(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			@PathVariable String id) throws ObjectNotFoundException{
		
		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		//read by id request
		Object typedBasedId = getTypeBasedIdObject(gigaSpace,type, id);
		if(logger.isLoggable(Level.FINE))
			logger.fine("creating readbyid query with type: " + type + " and id: " + id);
		IdQuery<SpaceDocument> idQuery = new IdQuery<SpaceDocument>(type, typedBasedId, QueryResultType.DOCUMENT);
		SpaceDocument doc = gigaSpace.readById(idQuery);
		if (doc == null){
			throw new ObjectNotFoundException("no object matched the criteria");
		}
		return doc.getProperties();
	}

	/**
	 * REST COUNT request handler
	 * 
	 */
	@RequestMapping(value = "/{type}/count", method = RequestMethod.GET)
	public ModelAndView count(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			HttpServletResponse response) throws ObjectNotFoundException{
		
		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		//read by id request
		Integer cnt = gigaSpace.count(new SpaceDocument(type));
		if (cnt == null){
			throw new ObjectNotFoundException("no object matched the criteria");
		}
		ModelAndView mv=new ModelAndView("jsonView");
		mv.addObject("count",cnt);
		response.setHeader("Access-Control-Allow-Origin","*");
		return mv;
	}


	/**
	 * REST GET by type request handler
	 * 
	 * @param type
	 * @return
	 * @throws ObjectNotFoundException
	 * @throws UnknownTypeException 
	 */
	@RequestMapping(value = "/{type}", method = RequestMethod.GET)
	public @ResponseBody Map<String, Object>[]  getByType(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			@RequestParam(value=SIZE_PARAM, required=false) Integer size)throws ObjectNotFoundException{
		return getByQuery(type,space,locators, "", size);
	}

	private Object getTypeBasedIdObject(GigaSpace gigaSpace,String type, String id) {
		SpaceTypeDescriptor typeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(type);
		if (typeDescriptor == null){
			throw new TypeNotFoundException(type);
		}

		//Investigate id type
		String idPropertyName = typeDescriptor.getIdPropertyName();
		SpacePropertyDescriptor idProperty = typeDescriptor.getFixedProperty(idPropertyName);
		try {
			return ControllerUtils.convertPropertyToPrimitiveType(id, idProperty.getType(), idPropertyName);
		} catch (UnknownTypeException e) {
			throw new DataAccessException("Only primitive SpaceId is currently supported by RestData") {};
		}
	}


	/**
	 * REST DELETE by id request handler
	 * 
	 * @param type
	 * @param id
	 * @return
	 * @throws ObjectNotFoundException
	 */
	@RequestMapping(value = "/{type}/{id}", method = RequestMethod.DELETE)
	public @ResponseBody Map<String, Object> deleteById(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			@PathVariable String id) throws ObjectNotFoundException {
		
		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		//take by id
		Object typedBasedId = getTypeBasedIdObject(gigaSpace,type, id);
		if(logger.isLoggable(Level.FINE))
			logger.fine("creating takebyid query with type: " + type + " and id: " + id);
		SpaceDocument doc;
		doc = gigaSpace.takeById(new IdQuery<SpaceDocument>(type, typedBasedId, QueryResultType.DOCUMENT));
		if (doc == null){
			throw new ObjectNotFoundException("no object matched the criteria");
		}
		return doc.getProperties();
	}

	/**
	 * REST DELETE by query request handler 
	 * @param type
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/{type}/" + CRITERIA_KEYWORD, method = RequestMethod.DELETE, params=QUERY_PARAM)
	public @ResponseBody Map<String, Object>[] deleteByQuery(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type, 
			@RequestParam(value=QUERY_PARAM) String query,
			@RequestParam(value=SIZE_PARAM, required=false) Integer size){
		if(logger.isLoggable(Level.FINE))
			logger.fine("creating take query with type: " + type + " and query: " + query);

		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		SQLQuery<SpaceDocument> sqlQuery = new SQLQuery<SpaceDocument>(type, query, QueryResultType.DOCUMENT);
		int maxSize = (size==null ? maxReturnValues : size.intValue());
		SpaceDocument[] docs;
		try {
			docs = gigaSpace.takeMultiple(sqlQuery, maxSize);
		} catch (DataAccessException e) {
			throw translateDataAccessException(gigaSpace, e, type);
		}
		return ControllerUtils.createPropertiesResult(docs);
	}

	/**
	 * REST DELETE by type request handler 
	 * @param type
	 * @param query
	 * @return
	 */
	@RequestMapping(value = "/{space}/{locators}/{type}", method = RequestMethod.DELETE)
	public @ResponseBody Map<String, Object>[] deleteByType(
			@PathVariable String space, 
			@PathVariable String locators, 
			@PathVariable String type, 
			@RequestParam(value=SIZE_PARAM, required=false) Integer size){
		return deleteByQuery(space,locators,type, "", size);
	}

	/**
	 * REST POST request handler
	 * 
	 * @param type
	 * @param reader
	 * @return
	 * @throws TypeNotFoundException 
	 */
	@RequestMapping(value = "/{type}", method = RequestMethod.POST)
	public @ResponseBody String post(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			BufferedReader reader) 
					throws TypeNotFoundException{
		if(logger.isLoggable(Level.FINE))
			logger.fine("performing post, type: " + type);

		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		createAndWriteDocuments(gigaSpace, type, reader, UpdateModifiers.WRITE_ONLY);
		return "success";
	}

	/**
	 * REST PUT request handler
	 * 
	 * @param type
	 * @param reader
	 * @return
	 * @throws TypeNotFoundException 
	 */
	@RequestMapping(value = "/{type}", method = RequestMethod.PUT)
	public @ResponseBody String put(
			@RequestParam(value=SPACE_PARAM) String space,
			@RequestParam(value=LOCATORS_PARAM,defaultValue="localhost") String locators,
			@PathVariable String type,
			BufferedReader reader) 
					throws TypeNotFoundException{
		if(logger.isLoggable(Level.FINE))
			logger.fine("performing put, type: " + type);

		GigaSpace gigaSpace=ControllerUtils.xapCache.get(space,locators);
		createAndWriteDocuments(gigaSpace, type, reader, UpdateModifiers.UPDATE_OR_WRITE);
		return "success";
	}

	private RuntimeException translateDataAccessException(GigaSpace gigaSpace,DataAccessException e, String type) {
		if (gigaSpace.getTypeManager().getTypeDescriptor(type) == null) {
			return new TypeNotFoundException(type);
		} else {
			return e;
		}
	}

	/**
	 * TypeNotFoundException Handler, returns an error response to the client
	 * 
	 * @param writer
	 * @throws IOException
	 */
	@ExceptionHandler(TypeNotFoundException.class)
	@ResponseStatus(value=HttpStatus.NOT_FOUND)
	public void resolveTypeDescriptorNotFoundException(TypeNotFoundException e, Writer writer) throws IOException {
		if(logger.isLoggable(Level.FINE))
			logger.fine("type descriptor for typeName: " + e.getTypeName() + " not found, returning error response");

		writer.write("{\"error\":\"Type: " + e.getTypeName() + " is not registered in space.\"}");
	}

	/**
	 * ObjectNotFoundException Handler, returns an error response to the client
	 * 
	 * @param writer
	 * @throws IOException
	 */
	@ExceptionHandler(ObjectNotFoundException.class)
	@ResponseStatus(value=HttpStatus.NOT_FOUND)
	public void resolveDocumentNotFoundException(Writer writer) throws IOException {
		if(logger.isLoggable(Level.FINE))
			logger.fine("space id query has no results, returning error response");

		writer.write("{\"error\":\"Object not found\"}");
	}

	/**
	 * DataAcessException Handler, returns an error response to the client
	 * @param e
	 * @param writer
	 * @throws IOException
	 */
	@ExceptionHandler(DataAccessException.class)
	@ResponseStatus(value=HttpStatus.INTERNAL_SERVER_ERROR)
	public void resolveDataAccessException(Exception e, Writer writer) throws IOException {
		if(logger.isLoggable(Level.WARNING))
			logger.log(Level.WARNING, "received DataAccessException exception", e);

		writer.write(String.format( "{\"error\":{\"java.class\":\"%s\", \"message\":\"%s\"}}",
				e.getClass(), e.getMessage()));
	}


	/**
	 * helper method that creates space documents from the httpRequest payload and writes them to space.
	 * 
	 * @param type
	 * @param reader
	 * @param updateModifiers
	 * @throws TypeNotFoundException 
	 */
	private void createAndWriteDocuments(GigaSpace gigaSpace, String type, BufferedReader reader, int updateModifiers) 
			throws TypeNotFoundException{
		logger.info("creating space Documents from payload");
		SpaceDocument[] spaceDocuments = ControllerUtils.createSpaceDocuments(type, reader, gigaSpace);
		if (spaceDocuments != null && spaceDocuments.length > 0){
			gigaSpace.writeMultiple(spaceDocuments, Lease.FOREVER, updateModifiers);
			if(logger.isLoggable(Level.FINE))
				logger.fine("wrote space documents to space");
		}else{
			if(logger.isLoggable(Level.FINE))
				logger.fine("did not write anything to space");
		}
	}

	public void setMaxReturnValues(int maxReturnValues) {
		this.maxReturnValues = maxReturnValues;
	}


}
