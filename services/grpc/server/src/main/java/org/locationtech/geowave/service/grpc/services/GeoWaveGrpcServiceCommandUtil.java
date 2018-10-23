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
package org.locationtech.geowave.service.grpc.services;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.GenericTypeResolver;

import com.beust.jcommander.ParametersDelegate;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GeoWaveGrpcServiceCommandUtil
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServiceCommandUtil.class.getName());

	static void setGrpcToCommandFields(
			Map<FieldDescriptor, Object> m,
			ServiceEnabledCommand cmd ) {
		for (Map.Entry<FieldDescriptor, Object> entry : m.entrySet()) {
			try {
				mapFieldValue(
						cmd,
						cmd.getClass(),
						entry);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
			catch (IllegalArgumentException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
			catch (IllegalAccessException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
		}
	}

	private static void mapFieldValue(
			Object cmd,
			final Class<?> cmdClass,
			Map.Entry<FieldDescriptor, Object> entry )
			throws IOException,
			IllegalArgumentException,
			IllegalAccessException {

		try {
			Field currField = cmdClass.getDeclaredField(entry.getKey().getName());
			currField.setAccessible(true);
			Object value;
			if (entry.getValue() == null) {
				value = null;
			}
			else if (currField.getType().isArray() && entry.getValue() instanceof List) {
				// lets assume String as other arrays are not used with
				// JCommander at least currently
				// something like this line would have to be used to get the
				// class from the generic and instantiate an array of that class
				// GenericTypeResolver.resolveTypeArguments(entry.getValue().getClass(),
				// List.class)[0]
				value = ((List) entry.getValue()).toArray(new String[0]);
			}
			else {
				value = entry.getValue();
			}
			currField.set(
					cmd,
					value);
		}
		catch (final NoSuchFieldException e) {
			// scan the parameters delegates for the field if it could not be
			// found
			// as a stand-alone member
			Field[] fields = cmdClass.getDeclaredFields();
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].isAnnotationPresent(ParametersDelegate.class)) {
					fields[i].setAccessible(true);
					mapFieldValue(
							(fields[i].get(cmd)),
							fields[i].getType(),
							entry);
				}
			}

			// bubble up through the class hierarchy
			if (cmdClass.getSuperclass() != null) mapFieldValue(
					cmd,
					cmdClass.getSuperclass(),
					entry);

		}
		catch (final IllegalAccessException e) {
			LOGGER.error(
					"Exception encountered setting fields on command",
					e);
		}

	}
}
