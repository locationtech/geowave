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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

public class IngestUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);

	public static boolean checkIndexesAgainstProvider(
			final String providerName,
			final DataAdapterProvider<?> adapterProvider,
			final List<IndexPluginOptions> indexOptions ) {
		boolean valid = true;
		for (final IndexPluginOptions option : indexOptions) {
			if (!IngestUtils.isCompatible(
					adapterProvider,
					option)) {
				// HP Fortify "Log Forging" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.warn("Local file ingest plugin for ingest type '" + providerName
						+ "' does not support dimensionality '" + option.getType() + "'");
				valid = false;
			}
		}
		return valid;
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
