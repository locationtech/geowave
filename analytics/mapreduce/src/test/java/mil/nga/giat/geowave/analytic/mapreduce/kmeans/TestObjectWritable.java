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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.vividsolutions.jts.geom.Coordinate;

public class TestObjectWritable implements
		Writable
{

	private TestObject obj;

	public TestObjectWritable() {

	}

	public TestObjectWritable(
			final TestObject obj ) {
		super();
		this.obj = obj;
	}

	public TestObject getObj() {
		return obj;
	}

	public void setObj(
			final TestObject obj ) {
		this.obj = obj;
	}

	@Override
	public void readFields(
			final DataInput arg0 )
			throws IOException {
		final String id = arg0.readUTF();
		final String name = arg0.readUTF();
		final String gid = arg0.readUTF();
		final double x = arg0.readDouble();
		final double y = arg0.readDouble();
		obj = new TestObject(
				new Coordinate(
						x,
						y),
				id);
		obj.setName(name);
		obj.groupID = gid;

	}

	@Override
	public void write(
			final DataOutput arg0 )
			throws IOException {
		arg0.writeUTF(obj.id);
		arg0.writeUTF(obj.name);
		arg0.writeUTF(obj.groupID);
		arg0.writeDouble(obj.geo.getCoordinate().x);
		arg0.writeDouble(obj.geo.getCoordinate().y);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((obj == null) ? 0 : obj.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TestObjectWritable other = (TestObjectWritable) obj;
		if (this.obj == null) {
			if (other.obj != null) return false;
		}
		else if (!this.obj.equals(other.obj)) return false;
		return true;
	}

}
