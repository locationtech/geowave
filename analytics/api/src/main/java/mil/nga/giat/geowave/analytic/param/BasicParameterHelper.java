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
package mil.nga.giat.geowave.analytic.param;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;

public class BasicParameterHelper implements
		ParameterHelper<Object>
{
	final static Logger LOGGER = LoggerFactory.getLogger(BasicParameterHelper.class);
	private final ParameterEnum<?> parent;
	private final Class<Object> baseClass;
	private final boolean isClass;

	public BasicParameterHelper(
			final ParameterEnum<?> parent,
			final Class<Object> baseClass,
			final String name,
			final String description,
			final boolean isClass,
			final boolean hasArg ) {
		this.baseClass = baseClass;
		this.parent = parent;
		this.isClass = isClass;
	}

	@Override
	public Class<Object> getBaseClass() {
		return baseClass;
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final Object value ) {
		setParameter(
				config,
				scope,
				value,
				parent);
	}

	private static final void setParameter(
			final Configuration config,
			final Class<?> scope,
			final Object val,
			final ParameterEnum configItem ) {
		if (val != null) {
			if (val instanceof Long) {
				config.setLong(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						((Long) val));
			}
			else if (val instanceof Double) {
				config.setDouble(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						((Double) val));
			}
			else if (val instanceof Boolean) {
				config.setBoolean(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						((Boolean) val));
			}
			else if (val instanceof Integer) {
				config.setInt(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						((Integer) val));
			}
			else if (val instanceof Class) {
				config.setClass(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						((Class) val),
						((Class) val));
			}
			else if (val instanceof byte[]) {
				config.set(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						ByteArrayUtils.byteArrayToString((byte[]) val));
			}
			else {
				config.set(
						GeoWaveConfiguratorBase.enumToConfKey(
								scope,
								configItem.self()),
						val.toString());
			}

		}
	}

	@Override
	public Object getValue(
			final JobContext context,
			final Class<?> scope,
			final Object defaultValue ) {
		final ScopedJobConfiguration scopedConfig = new ScopedJobConfiguration(
				context.getConfiguration(),
				scope);
		if (baseClass.isAssignableFrom(Integer.class)) {
			return Integer.valueOf(scopedConfig.getInt(
					parent.self(),
					((Integer) defaultValue).intValue()));
		}
		else if (baseClass.isAssignableFrom(String.class)) {
			return scopedConfig.getString(
					parent.self(),
					defaultValue.toString());
		}
		else if (baseClass.isAssignableFrom(Double.class)) {
			return scopedConfig.getDouble(
					parent.self(),
					(Double) defaultValue);
		}
		else if (baseClass.isAssignableFrom(byte[].class)) {
			return scopedConfig.getBytes(parent.self());
		}
		else if ((defaultValue == null) || (defaultValue instanceof Class)) {
			try {
				return scopedConfig.getInstance(
						parent.self(),
						baseClass,
						(Class) defaultValue);
			}
			catch (InstantiationException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to get instance from job context",
						e);
			}
		}
		return null;
	}

	@Override
	public Object getValue(
			final PropertyManagement propertyManagement ) {
		try {
			return propertyManagement.getProperty(parent);
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to deserialize property '" + parent.toString() + "'",
					e);
			return null;
		}
	}

	@Override
	public void setValue(
			final PropertyManagement propertyManagement,
			final Object value ) {
		Object storeValue = value;
		if (this.isClass && value instanceof String) {
			try {
				storeValue = Class.forName(value.toString());
			}
			catch (ClassNotFoundException e) {
				LOGGER.error(
						"Class " + value.toString() + " for property " + parent + " is not found",
						e);
			}
		}
		propertyManagement.store(
				parent,
				storeValue);
	}
}
