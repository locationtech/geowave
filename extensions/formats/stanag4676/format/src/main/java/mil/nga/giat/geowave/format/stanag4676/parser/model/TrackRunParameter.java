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
package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class TrackRunParameter
{
	private Long id;

	private String name;
	private String value;

	public TrackRunParameter() {}

	public TrackRunParameter(
			String name,
			String value ) {
		this.name = name;
		this.value = value;
	}

	public Long getId() {
		return id;
	}

	public void setId(
			Long id ) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(
			String value ) {
		this.value = value;
	}
}
