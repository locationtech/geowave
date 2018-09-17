/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
/**
 * 
 */
package org.locationtech.geowave.core.cli.utils;

import java.lang.reflect.Constructor;

import org.locationtech.geowave.core.cli.converters.GeoWaveBaseConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 *
 */
public class JCommanderParameterUtils
{
	private static Logger LOGGER = LoggerFactory.getLogger(JCommanderParameterUtils.class);

	public static boolean isPassword(
			Parameter parameter ) {
		boolean isPassword = false;
		if (parameter != null) {
			Class<?> superClass = null;
			Class<? extends IStringConverter<?>> converterClass = parameter.converter();
			if (converterClass != null) {
				superClass = converterClass.getSuperclass();
				while (superClass != null && superClass != GeoWaveBaseConverter.class) {
					superClass = superClass.getSuperclass();
				}
			}

			if (superClass != null && superClass.equals(GeoWaveBaseConverter.class)) {
				GeoWaveBaseConverter<?> converter = getParameterBaseConverter(parameter);
				if (converter != null) {
					isPassword = isPassword || converter.isPassword();
				}
			}
			isPassword = isPassword || parameter.password();
		}
		return isPassword;
	}

	public static boolean isRequired(
			Parameter parameter ) {
		boolean isRequired = false;
		if (parameter != null) {
			if (parameter.converter() != null && parameter.converter().getSuperclass().equals(
					GeoWaveBaseConverter.class)) {
				GeoWaveBaseConverter<?> converter = getParameterBaseConverter(parameter);
				if (converter != null) {
					isRequired = isRequired || converter.isRequired();
				}
			}
			isRequired = isRequired || parameter.required();
		}
		return isRequired;
	}

	private static GeoWaveBaseConverter<?> getParameterBaseConverter(
			Parameter parameter ) {
		GeoWaveBaseConverter<?> converter = null;
		try {
			Constructor<?> ctor = parameter.converter().getConstructor(
					String.class);
			if (ctor != null) {
				converter = (GeoWaveBaseConverter<?>) ctor.newInstance(new Object[] {
					""
				});
			}
		}
		catch (Exception e) {
			LOGGER.error(
					"An error occurred getting converter from parameter: " + e.getLocalizedMessage(),
					e);
		}
		return converter;
	}
}
