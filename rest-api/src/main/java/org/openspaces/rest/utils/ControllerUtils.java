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
package org.openspaces.rest.utils;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.admin.space.Space;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.rest.exceptions.TypeNotFoundException;
import org.springframework.http.converter.HttpMessageNotReadableException;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.UnknownTypeException;

/**
 * some helper methods to the SpaceApiController class
 * @author rafi
 * @since 8.0
 */
public class ControllerUtils {
	private static final Logger logger = Logger.getLogger(ControllerUtils.class.getName());
	private static final TypeReference<HashMap<String, Object>[]> typeRef = new TypeReference<HashMap<String, Object>[]>() {};
	private static final ObjectMapper mapper = new ObjectMapper();
	public static final XapConnectionCache xapCache=new XapConnectionCache();

	public static SpaceDocument[] createSpaceDocuments(String type, BufferedReader reader, GigaSpace gigaSpace) 
			throws TypeNotFoundException {
		HashMap<String, Object>[] propertyMapArr;
		try{
			//get payload
			StringBuilder sb = new StringBuilder();
			String line = reader.readLine();
			while (line != null) {
				sb.append(line + "\n");
				line = reader.readLine();
			}
			reader.close();
			//if single json object convert it to array
			String data = sb.toString();
			if (!data.startsWith("[")){
				sb.insert(0, "[");
				sb.append("]");
			}
			//convert to json
			propertyMapArr = mapper.readValue(sb.toString(), typeRef);
		} catch(Exception e){
			throw new HttpMessageNotReadableException(e.getMessage(), e.getCause());
		}
		SpaceDocument[] documents = new SpaceDocument[propertyMapArr.length];
		for (int i = 0; i < propertyMapArr.length; i++) {
			try {
				Map<String, Object> typeBasedProperties = getTypeBasedProperties(type, propertyMapArr[i], gigaSpace);
				documents[i] = new SpaceDocument(type, typeBasedProperties);
			} catch (UnknownTypeException e) {
				logger.log(Level.SEVERE,"could not convert properties based on type", e);
				//cancel previous documents and return null ( do not write anything to space)
				return null;
			}
		}
		return documents;
	}

	public static Map<String, Object>[] createPropertiesResult(SpaceDocument[] docs) {
		Map<String, Object>[] result = new HashMap[docs.length];
		for (int i = 0; i < docs.length; i++) {
			result[i] = new HashMap(docs[i].getProperties());
		}
		return result;
	}


	/**
	 * @param documentType
	 * @param propertyMap
	 * @param gigaSpace
	 * @return
	 * @throws UnknownTypeException
	 * @throws TypeNotFoundException 
	 */
	private static Map<String, Object> getTypeBasedProperties(String documentType, Map<String, Object> propertyMap, GigaSpace gigaSpace) throws UnknownTypeException, TypeNotFoundException {
		SpaceTypeDescriptor spaceTypeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(documentType);
		if (spaceTypeDescriptor == null){
			throw new TypeNotFoundException(documentType);
		}else{
			Map<String, Object> buildTypeBasedProperties = buildTypeBasedProperties(propertyMap, spaceTypeDescriptor, gigaSpace);
			return buildTypeBasedProperties;
		}

	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> buildTypeBasedProperties(
			Map<String, Object> propertyMap,
			SpaceTypeDescriptor spaceTypeDescriptor, GigaSpace gigaSpace) throws UnknownTypeException, TypeNotFoundException {
		HashMap<String, Object> newPropertyMap = new HashMap<String, Object>();
		for(Entry<String, Object> entry : propertyMap.entrySet()){
			String propKey = entry.getKey();
			Object oldPropValue = entry.getValue();
			SpacePropertyDescriptor propDesc = spaceTypeDescriptor.getFixedProperty(propKey);
			if (propDesc == null){
				if(logger.isLoggable(Level.WARNING))
					logger.warning("could not find SpacePropertyDescriptor for " + propKey + ", using String as property type");
				newPropertyMap.put(propKey, oldPropValue);
			}/*else if(propDesc.getType().equals(Object.class)){
                logger.warning("Existing Type of " + propKey + " is Object, using String as property type");
                newPropertyMap.put(propKey, oldPropValue);
            }*/
			else{
				Object convertedObj;
				if (oldPropValue instanceof Map){
					String typeName = propDesc.getType().getName();
					Map<String, Object> nestedObjProps = getTypeBasedProperties(typeName, (Map<String, Object>) oldPropValue, gigaSpace) ;
					convertedObj = new SpaceDocument(typeName, nestedObjProps);
				}else{
					convertedObj = convertPropertyToPrimitiveType((String)oldPropValue, propDesc.getType(), propKey);
				}
				newPropertyMap.put(propKey, convertedObj);
			}
		}
		return newPropertyMap;
	}

	public static Object convertPropertyToPrimitiveType(String object, Class type, String propKey) throws UnknownTypeException {
		if (type.equals(Long.class))
			return Long.valueOf(object);

		if (type.equals(Boolean.class))
			return Boolean.valueOf(object);

		if (type.equals(Integer.class))
			return Integer.valueOf(object);

		if (type.equals(Byte.class))
			return Byte.valueOf(object);

		if (type.equals(Short.class))
			return Short.valueOf(object);

		if (type.equals(Float.class))
			return Float.valueOf(object);

		if (type.equals(Double.class))
			return Double.valueOf(object);

		if (type.isEnum())
			return Enum.valueOf(type, object);

		if (type.equals(String.class) || type.equals(Object.class))
			return String.valueOf(object);

		//unknown type
		throw new UnknownTypeException("non primitive type when converting property", type.getName());
	}

	/**
	 * Open ended thread safe cache for XAP connections
	 * 
	 * @author DeWayne
	 *
	 */
	public static class XapConnectionCache{
		private final Logger log=Logger.getLogger("XapConnectionCache");
		private static Map<String,XapEndpoint> cache=new ConcurrentHashMap<String,XapEndpoint>();

		public XapConnectionCache(){
		}

		public GigaSpace get(String spaceName,String locators){
			if(spaceName==null || spaceName.length()==0)throw new IllegalArgumentException("invalid (null or 0 length) spacename");
			if(locators==null || locators.length()==0)throw new IllegalArgumentException("invalid (null or 0 length) spacename");

			synchronized(cache){

				log.finest("getting space");
				GigaSpace gs=get(spaceName);
				if(gs!=null)return gs;

				String url="jini://*/*/"+spaceName+"?locators="+locators;
				log.finest("  connecting to "+url);
				UrlSpaceConfigurer usc=new UrlSpaceConfigurer(url);
				gs=new GigaSpaceConfigurer(usc.space()).gigaSpace();
				log.finest("  got space.  connecting admin");
				Admin admin=new AdminFactory().addLocators(locators).discoverUnmanagedSpaces().useDaemonThreads(true).create();
				for(Map.Entry<String,Space> entry:admin.getSpaces().getNames().entrySet()){
					log.finest("    found space:"+entry.getKey());
				}
				log.finest("  admin created, waiting for space:"+spaceName);
				Space space=admin.getSpaces().waitFor(spaceName);
				cache.put(spaceName,new XapEndpoint(gs,usc,space.getNumberOfInstances()));
				log.finest("  returning space");
				return gs;
			}
		}

		/**
		 * Gets a space in the cache.  Doesn't open new connections.
		 * @param spaceName the name of the space to get
		 * @return GigaSpace if successful.  Null otherwise.
		 */
		public GigaSpace get(String spaceName){
			XapEndpoint ep=cache.get(spaceName);
			if(ep==null)return null;
			return ep.space;
		}

		public int getInstances(String spaceName){
			if(spaceName==null || spaceName.length()==0)throw new IllegalArgumentException("null or zero length space name");

			XapEndpoint ep=cache.get(spaceName);
			if(ep==null)throw new IllegalArgumentException("space name '"+spaceName+"' unknown");

			return ep.instanceCount;
		}

	}

	private static class XapEndpoint{
		public GigaSpace space=null;
		public UrlSpaceConfigurer usc=null;
		public int instanceCount=0;

		public XapEndpoint(GigaSpace space,UrlSpaceConfigurer usc,int instances){
			this.space=space;
			this.usc=usc;
			this.instanceCount=instances;
		}

	}

}
