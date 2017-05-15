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
package mil.nga.giat.geowave.core.index.sfc;

import mil.nga.giat.geowave.core.index.ByteArrayRange;

/***
 * This class encapsulates a set of ranges returned from a space filling curve
 * decomposition.
 * 
 */
public class RangeDecomposition
{
	private final ByteArrayRange[] ranges;

	/**
	 * Constructor used to create a new Range Decomposition object.
	 * 
	 * @param ranges
	 *            ranges for the space filling curve
	 */
	public RangeDecomposition(
			final ByteArrayRange[] ranges ) {
		this.ranges = ranges;
	}

	/**
	 * 
	 * @return the ranges associated with this Range Decomposition
	 */
	public ByteArrayRange[] getRanges() {
		return ranges;
	}
}
