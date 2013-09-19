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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.openspaces.core.GigaSpace;
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

}
