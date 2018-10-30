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
package org.locationtech.geowave.service.rest.field;

import java.lang.reflect.Field;

import com.beust.jcommander.Parameter;

public class ParameterRestField implements
		RestField
{
	protected final Field field;
	protected final Parameter parameter;

	public ParameterRestField(
			final Field field,
			final Parameter parameter ) {
		this.field = field;
		this.parameter = parameter;
	}

	@Override
	public String getName() {
		return field.getName();
	}

	@Override
	public Class<?> getType() {
		return field.getType();
	}

	@Override
	public String getDescription() {
		return parameter.description();
	}

	@Override
	public boolean isRequired() {
		return parameter.required();
	}

	public Field getField() {
		return this.field;
	}
}
