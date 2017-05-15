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

public class MultiDimensionalCoordinates
{
	// this is a generic placeholder for tiers
	private final byte[] multiDimensionalId;
	private final Coordinate[] coordinatePerDimension;

	public MultiDimensionalCoordinates() {
		multiDimensionalId = new byte[] {};
		coordinatePerDimension = new Coordinate[] {};
	}

	public MultiDimensionalCoordinates(
			final byte[] multiDimensionalId,
			final Coordinate[] coordinatePerDimension ) {
		super();
		this.multiDimensionalId = multiDimensionalId;
		this.coordinatePerDimension = coordinatePerDimension;
	}

	public byte[] getMultiDimensionalId() {
		return multiDimensionalId;
	}

	public Coordinate getCoordinate(
			final int dimension ) {
		return coordinatePerDimension[dimension];
	}

	public int getNumDimensions() {
		return coordinatePerDimension.length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(coordinatePerDimension);
		result = (prime * result) + Arrays.hashCode(multiDimensionalId);
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
		final MultiDimensionalCoordinates other = (MultiDimensionalCoordinates) obj;
		if (!Arrays.equals(
				coordinatePerDimension,
				other.coordinatePerDimension)) {
			return false;
		}
		if (!Arrays.equals(
				multiDimensionalId,
				other.multiDimensionalId)) {
			return false;
		}
		return true;
	}
}
