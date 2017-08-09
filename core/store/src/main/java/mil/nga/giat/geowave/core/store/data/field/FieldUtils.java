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
package mil.nga.giat.geowave.core.store.data.field;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.SPIServiceRegistry;
import mil.nga.giat.geowave.core.store.util.GenericTypeResolver;

/**
 * This class has a set of convenience methods to determine the appropriate
 * field reader and writer for a given field type (Class)
 * 
 */
public class FieldUtils
{
	private static final Logger LOGGER = LoggerFactory.getLogger(FieldUtils.class);
	private static Map<Class<?>, FieldReader<?>> fieldReaderRegistry = null;
	private static Map<Class<?>, FieldWriter<?, ?>> fieldWriterRegistry = null;

	private static synchronized Map<Class<?>, FieldReader<?>> getRegisteredFieldReaders() {
		if (fieldReaderRegistry == null) {
			initRegistry();
		}
		return fieldReaderRegistry;
	}

	private static synchronized Map<Class<?>, FieldWriter<?, ?>> getRegisteredFieldWriters() {
		if (fieldWriterRegistry == null) {
			initRegistry();
		}
		return fieldWriterRegistry;
	}

	private static synchronized void initRegistry() {
		fieldReaderRegistry = new HashMap<Class<?>, FieldReader<?>>();
		fieldWriterRegistry = new HashMap<Class<?>, FieldWriter<?, ?>>();
		final Iterator<FieldSerializationProviderSpi> serializationProviders = new SPIServiceRegistry(
				FieldSerializationProviderSpi.class).load(FieldSerializationProviderSpi.class);
		while (serializationProviders.hasNext()) {
			final FieldSerializationProviderSpi<?> serializationProvider = serializationProviders.next();
			if (serializationProvider != null) {
				final Class<?> type = GenericTypeResolver.resolveTypeArgument(
						serializationProvider.getClass(),
						FieldSerializationProviderSpi.class);
				final FieldReader<?> reader = serializationProvider.getFieldReader();
				if (reader != null) {
					if (fieldReaderRegistry.containsKey(type)) {
						LOGGER.warn("Field reader already registered for " + type + "; not able to add " + reader);
					}
					else {
						fieldReaderRegistry.put(
								type,
								reader);
					}
				}
				final FieldWriter<?, ?> writer = serializationProvider.getFieldWriter();
				if (writer != null) {
					if (fieldWriterRegistry.containsKey(type)) {
						LOGGER.warn("Field writer already registered for " + type + "; not able to add " + writer);
					}
					else {
						fieldWriterRegistry.put(
								type,
								writer);
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> FieldReader<T> getDefaultReaderForClass(
			final Class<T> myClass ) {
		final Map<Class<?>, FieldReader<?>> internalFieldReaders = getRegisteredFieldReaders();
		// try concrete class
		final FieldReader<T> reader = (FieldReader<T>) internalFieldReaders.get(myClass);
		if (reader != null) {
			return reader;
		}
		// if the concrete class lookup failed, try inheritance
		return (FieldReader<T>) getAssignableValueFromClassMap(
				myClass,
				internalFieldReaders);
	}

	@SuppressWarnings("unchecked")
	public static <T> FieldWriter<?, T> getDefaultWriterForClass(
			final Class<T> myClass ) {
		final Map<Class<?>, FieldWriter<?, ?>> internalFieldWriters = getRegisteredFieldWriters();
		// try concrete class
		final FieldWriter<?, T> writer = (FieldWriter<?, T>) internalFieldWriters.get(myClass);
		if (writer != null) {
			return writer;
		} // if the concrete class lookup failed, try inheritance
		return (FieldWriter<?, T>) getAssignableValueFromClassMap(
				myClass,
				internalFieldWriters);
	}

	public static <T> T getAssignableValueFromClassMap(
			final Class<?> myClass,
			final Map<Class<?>, T> classToAssignableValueMap ) {
		// loop through the map to discover the first class that is assignable
		// from myClass
		for (final Entry<Class<?>, T> candidate : classToAssignableValueMap.entrySet()) {
			if (candidate.getKey().isAssignableFrom(
					myClass)) {
				return candidate.getValue();
			}
		}
		return null;
	}

	public static <RowType, FieldType> FieldWriter<RowType, FieldType> getDefaultWriterForClass(
			final Class<FieldType> myClass,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		return new BasicWriter<RowType, FieldType>(
				getDefaultWriterForClass(myClass),
				visibilityHandler);
	}
}
