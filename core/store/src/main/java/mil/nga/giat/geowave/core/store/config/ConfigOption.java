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
package mil.nga.giat.geowave.core.store.config;

public class ConfigOption
{
	private final String name;
	private final String description;
	private final boolean optional;
	private boolean password;
	private Class type;

	public ConfigOption(
			final String name,
			final String description,
			final boolean optional,
			final Class type ) {
		this.name = name;
		this.description = description;
		this.optional = optional;
		this.type = type;
	}

	public Class getType() {
		return type;
	}

	public void setType(
			Class type ) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public boolean isOptional() {
		return optional;
	}

	public boolean isPassword() {
		return password;
	}

	public void setPassword(
			boolean password ) {
		this.password = password;
	}
}
