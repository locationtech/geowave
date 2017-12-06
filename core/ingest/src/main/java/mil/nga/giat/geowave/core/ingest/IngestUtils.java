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
package mil.nga.giat.geowave.core.ingest;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.List;

import mil.nga.giat.geowave.core.ingest.hdfs.HdfsUrlStreamHandlerFactory;
import mil.nga.giat.geowave.core.ingest.s3.S3URLStreamHandlerFactory;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

public class IngestUtils
{
	public static enum URLTYPE {
		S3,
		HDFS
	}

	public static void setURLStreamHandlerFactory(
			URLTYPE urlType )
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException {

		Field factoryField = URL.class.getDeclaredField("factory");
		// HP Fortify "Access Control" false positive
		// The need to change the accessibility here is
		// necessary, has been review and judged to be safe
		factoryField.setAccessible(true);

		URLStreamHandlerFactory urlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField.get(null);

		if (urlStreamHandlerFactory == null) {
			if (urlType == URLTYPE.S3) {
				URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory());
			}
			else { // HDFS
				URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory());
			}

		}
		else {
			if (urlType == URLTYPE.S3) {
				if (urlStreamHandlerFactory instanceof S3URLStreamHandlerFactory) {
					return;
				}
			}
			else { // HDFS
				if (urlStreamHandlerFactory instanceof HdfsUrlStreamHandlerFactory) {
					return;
				}
			}

			Field lockField = URL.class.getDeclaredField("streamHandlerLock");
			// HP Fortify "Access Control" false positive
			// The need to change the accessibility here is
			// necessary, has been review and judged to be safe
			lockField.setAccessible(true);
			synchronized (lockField.get(null)) {

				factoryField.set(
						null,
						null);

				if (urlType == URLTYPE.S3) {
					URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory(
							urlStreamHandlerFactory));
				}
				else { // HDFS
					URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory(
							urlStreamHandlerFactory));
				}
			}
		}
	}

	/**
	 * Determine whether an index is compatible with the visitor
	 * 
	 * @param index
	 *            an index that an ingest type supports
	 * @return whether the adapter is compatible with the common index model
	 */
	public static boolean isCompatible(
			final DataAdapterProvider<?> adapterProvider,
			final IndexPluginOptions dimensionalityProvider ) {
		final Class<? extends CommonIndexValue>[] supportedTypes = adapterProvider.getSupportedIndexableTypes();
		if ((supportedTypes == null) || (supportedTypes.length == 0)) {
			return false;
		}
		final Class<? extends CommonIndexValue>[] requiredTypes = dimensionalityProvider
				.getIndexPlugin()
				.getRequiredIndexTypes();
		for (final Class<? extends CommonIndexValue> requiredType : requiredTypes) {
			boolean fieldFound = false;
			for (final Class<? extends CommonIndexValue> supportedType : supportedTypes) {
				if (requiredType.isAssignableFrom(supportedType)) {
					fieldFound = true;
					break;
				}
			}
			if (!fieldFound) {
				return false;
			}
		}
		return true;

	}

	public static boolean isSupported(
			final DataAdapterProvider<?> adapterProvider,
			final List<IndexPluginOptions> dimensionalityTypes ) {
		for (final IndexPluginOptions option : dimensionalityTypes) {
			if (isCompatible(
					adapterProvider,
					option)) {
				return true;
			}
		}
		return false;

	}

}
