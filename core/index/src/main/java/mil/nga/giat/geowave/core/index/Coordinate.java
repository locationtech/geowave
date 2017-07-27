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
package mil.nga.giat.geowave.core.index;

import java.util.Arrays;

public class Coordinate
{
	private long coordinate;
	private byte[] binId;

	protected Coordinate() {}

	public Coordinate(
			final long coordinate,
			final byte[] binId ) {
		this.coordinate = coordinate;
		this.binId = binId;
	}

	public long getCoordinate() {
		return coordinate;
	}

	public void setCoordinate(
			final long coordinate ) {
		this.coordinate = coordinate;
	}

	public byte[] getBinId() {
		return binId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(binId);
		result = (prime * result) + (int) (coordinate ^ (coordinate >>> 32));
		result = (prime * result) + Arrays.hashCode(binId);
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
		final Coordinate other = (Coordinate) obj;
		if (!Arrays.equals(
				binId,
				other.binId)) {
			return false;
		}
		if (coordinate != other.coordinate) {
			return false;
		}
		return true;
	}

}
