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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import java.io.Serializable;
import java.util.UUID;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class TestObject implements
		Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final GeometryFactory factory = new GeometryFactory();

	public Geometry geo;
	public String id;
	public String groupID = "1";
	public String name;
	public int level = 1;

	public TestObject() {
		id = UUID.randomUUID().toString();
	}

	public TestObject(
			Geometry geo,
			String id,
			String groupID ) {
		super();
		this.geo = geo;
		this.id = id;
		this.groupID = groupID;
		this.name = id;
	}

	public TestObject(
			final Coordinate coor,
			final String id ) {
		geo = factory.createPoint(coor);
		geo.setSRID(2029);
		this.id = id;
		this.name = id;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(
			int level ) {
		this.level = level;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getGroupID() {
		return groupID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((geo == null) ? 0 : geo.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TestObject other = (TestObject) obj;
		if (geo == null) {
			if (other.geo != null) return false;
		}
		else if (!geo.equals(other.geo)) return false;
		if (id == null) {
			if (other.id != null) return false;
		}
		else if (!id.equals(other.id)) return false;
		return true;
	}
}
