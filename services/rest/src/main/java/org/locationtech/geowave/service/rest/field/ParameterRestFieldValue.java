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

public class ParameterRestFieldValue extends
		ParameterRestField implements
		RestFieldValue
{
	private final Object instance;

	public ParameterRestFieldValue(
			final Field field,
			final Parameter parameter,
			final Object instance ) {
		super(
				field,
				parameter);
		this.instance = instance;
	}

	@Override
	public void setValue(
			final Object value )
			throws IllegalArgumentException,
			IllegalAccessException {
		// HP Fortify "Access Control" false positive
		// The need to change the accessibility here is
		// necessary, has been review and judged to be safe
		field.setAccessible(true);
		field.set(
				instance,
				value);
	}

	public Field getField() {
		return super.getField();
	}

}
