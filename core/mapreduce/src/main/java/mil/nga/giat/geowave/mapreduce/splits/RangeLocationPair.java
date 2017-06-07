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
package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class RangeLocationPair
{
	// Type of 'range' is the only difference between this and the Accumulo
	// version

	// Should change to a generic type and reuse

	private GeoWaveRowRange range;
	private String location;
	private double cardinality;

	public RangeLocationPair() {}

	public RangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		this.location = location;
		this.range = range;
		this.cardinality = cardinality;
	}

	public double getCardinality() {
		return cardinality;
	}

	public GeoWaveRowRange getRange() {
		return range;
	}

	public String getLocation() {
		return location;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((location == null) ? 0 : location.hashCode());
		result = (prime * result) + ((range == null) ? 0 : range.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final RangeLocationPair other = (RangeLocationPair) obj;
		if (location == null) {
			if (other.location != null) {
				return false;
			}
		}
		else if (!location.equals(other.location)) {
			return false;
		}
		if (range == null) {
			if (other.range != null) {
				return false;
			}
		}
		else if (!range.equals(other.range)) {
			return false;
		}
		return true;
	}

	public void readFields(
			final DataInput in )
			throws IOException,
			InstantiationException,
			IllegalAccessException {
		range = buildRowRangeInstance();
		range.readFields(in);
		location = in.readUTF();
		cardinality = in.readDouble();
	}

	protected abstract GeoWaveRowRange buildRowRangeInstance();

	public void write(
			final DataOutput out )
			throws IOException {
		range.write(out);
		out.writeUTF(location);
		out.writeDouble(cardinality);
	}
}
