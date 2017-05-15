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
package mil.nga.giat.geowave.analytic;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

public class ScopedJobConfiguration
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(ScopedJobConfiguration.class);

	private final Configuration jobConfiguration;

	private final Class<?> scope;
	private Logger logger = LOGGER;

	public ScopedJobConfiguration(
			final Configuration jobConfiguration,
			final Class<?> scope ) {
		super();
		this.jobConfiguration = jobConfiguration;
		this.scope = scope;
	}

	public ScopedJobConfiguration(
			final Configuration jobConfiguration,
			final Class<?> scope,
			final Logger logger ) {
		super();
		this.jobConfiguration = jobConfiguration;
		this.scope = scope;
		this.logger = logger;
	}

	public int getInt(
			final Enum<?> property,
			final int defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		final int v = jobConfiguration.getInt(
				propName,
				defaultValue);
		return v;
	}

	public String getString(
			final Enum<?> property,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return jobConfiguration.get(
				propName,
				defaultValue);
	}

	public <T> T getInstance(
			final Enum<?> property,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		try {
			final String propName = GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property);
			if (jobConfiguration.getRaw(propName) == null) {
				if (defaultValue == null) {
					return null;
				}
				logger.warn("Using default for property " + propName);
			}
			return jobConfiguration.getClass(
					GeoWaveConfiguratorBase.enumToConfKey(
							scope,
							property),
					defaultValue,
					iface).newInstance();
		}
		catch (final Exception ex) {
			logger.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property));
			throw ex;
		}
	}

	public double getDouble(
			final Enum<?> property,
			final double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (jobConfiguration.getRaw(propName) == null) {
			logger.warn("Using default for property " + propName);
		}
		return jobConfiguration.getDouble(
				propName,
				defaultValue);
	}

	public byte[] getBytes(
			final Enum<?> property ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		final String data = jobConfiguration.getRaw(propName);
		if (data == null) {
			logger.error(propName + " not found ");
		}
		return ByteArrayUtils.byteArrayFromString(data);
	}

}
