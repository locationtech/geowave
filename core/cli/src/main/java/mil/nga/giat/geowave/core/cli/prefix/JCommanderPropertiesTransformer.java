/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.cli.prefix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

/**
 * This class will translate a given set of ParameterDescription entries into a
 * properties file or back given a JCommander translation map.
 */
public class JCommanderPropertiesTransformer
{

	private static Logger LOGGER = LoggerFactory.getLogger(JCommanderPropertiesTransformer.class);

	// The namespace is prepended to entries translated via
	// this translator in the Properties object, or it is used
	// to only retrieve properties that start with this
	// namespace.
	private final String propertyFormat;
	private List<Object> objects = new ArrayList<Object>();

	public JCommanderPropertiesTransformer(
			String namespace ) {
		if (namespace == null) {
			propertyFormat = "%s";
		}
		else {
			propertyFormat = String.format(
					"%s.%s",
					namespace,
					"%s");
		}
	}

	public JCommanderPropertiesTransformer() {
		this(
				null);
	}

	/**
	 * Add an object to be translated
	 * 
	 * @param object
	 */
	public void addObject(
			Object object ) {
		objects.add(object);
	}

	/**
	 * Entries are needed to translate to/from the objects using the JCommander
	 * prefixes.
	 * 
	 * @return
	 */
	private Collection<TranslationEntry> generateEntries() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		for (Object obj : objects) {
			translator.addObject(obj);
		}
		JCommanderTranslationMap map = translator.translate();
		return map.getEntries().values();
	}

	/**
	 * Take the options and translate them to a map.
	 * 
	 * @param properties
	 */
	public void transformToMap(
			Map<String, String> properties ) {
		Properties props = new Properties();
		transformToProperties(props);
		for (String prop : props.stringPropertyNames()) {
			properties.put(
					prop,
					props.getProperty(prop));
		}
	}

	/**
	 * Take the options and translate them from a map.
	 * 
	 * @param properties
	 */
	public void transformFromMap(
			Map<String, String> properties ) {
		Properties props = new Properties();
		for (Entry<String, String> prop : properties.entrySet()) {
			if (prop.getValue() != null) {
				props.setProperty(
						prop.getKey(),
						prop.getValue());
			}
		}
		transformFromProperties(props);
	}

	/**
	 * Take the given values in the translation map, and convert them to a
	 * properties list.
	 * 
	 * @param toProperties
	 */
	public void transformToProperties(
			Properties toProperties ) {
		// Translate all fields.
		for (TranslationEntry entry : generateEntries()) {

			// Get the Properties name.
			String propertyName = entry.getAsPropertyName();
			propertyName = String.format(
					propertyFormat,
					propertyName);

			// Get the value.
			Object value = null;
			try {
				value = entry.getParam().get(
						entry.getObject());
			}
			catch (Exception e) {
				LOGGER.warn(
						"Unable to set value",
						e);
				continue;
			}
			if (value == null) {
				continue;
			}
			// Dyn parameter, serialize map.
			if (entry.getParam().isDynamicParameter()) {
				@SuppressWarnings("unchecked")
				Map<String, String> props = (Map<String, String>) value;
				for (Map.Entry<String, String> prop : props.entrySet()) {
					if (prop.getValue() != null) {
						toProperties.put(
								String.format(
										"%s.%s",
										propertyName,
										prop.getKey()),
								prop.getValue());
					}
				}
			}
			else {
				toProperties.put(
						propertyName,
						value.toString());
			}
		}
	}

	/**
	 * Take the given properties list, and convert it to the given objects.
	 * 
	 * @param fromProperties
	 */
	public void transformFromProperties(
			Properties fromProperties ) {

		// This JCommander object is used strictly to use the 'convertValue'
		// function which happens to be public.
		JCommander jc = new JCommander();

		// Translate all fields.
		for (TranslationEntry entry : generateEntries()) {

			// Get the Properties name.
			String propertyName = entry.getAsPropertyName();
			propertyName = String.format(
					propertyFormat,
					propertyName);

			// Set the value.
			if (entry.getParam().isDynamicParameter()) {
				Map<String, String> fromMap = new HashMap<String, String>();
				Set<String> propNames = fromProperties.stringPropertyNames();
				for (String propName : propNames) {
					if (propName.startsWith(propertyName)) {
						// Parse
						String parsedName = propName.substring(propertyName.length() + 1);
						fromMap.put(
								parsedName,
								fromProperties.getProperty(propName));
					}
				}
				// Set the map.
				entry.getParam().set(
						entry.getObject(),
						fromMap);
			}
			else {
				String value = fromProperties.getProperty(propertyName);
				if (value != null) {
					// Convert the value to the expected format, and
					// set it on the original object.
					entry.getParam().set(
							entry.getObject(),
							jc.convertValue(
									entry.getParam(),
									entry.getParam().getType(),
									value));
				}
			}
		}
	}
}
