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

public class BasicRestField<T> implements
		RestField<T>
{

	private final String name;
	private final Class<T> type;
	private final String description;
	private final boolean required;

	public BasicRestField(
			final String name,
			final Class<T> type,
			final String description,
			final boolean required ) {
		this.name = name;
		this.type = type;
		this.description = description;
		this.required = required;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Class<T> getType() {
		return type;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public boolean isRequired() {
		return required;
	}
}
