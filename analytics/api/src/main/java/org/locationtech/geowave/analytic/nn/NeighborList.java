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
package org.locationtech.geowave.analytic.nn;

import java.util.Map.Entry;

import org.locationtech.geowave.core.index.ByteArray;

public interface NeighborList<NNTYPE> extends
		Iterable<Entry<ByteArray, NNTYPE>>
{
	public enum InferType {
		NONE,
		SKIP, // distance measure is skipped
		REMOVE // skipped and removed from future selection
	};

	/**
	 * May be called prior to init() when discovered by entry itself.
	 * 
	 * @param entry
	 * @return
	 */
	public boolean add(
			DistanceProfile<?> distanceProfile,
			ByteArray id,
			NNTYPE value );

	/**
	 * See if the entries relationships have already been inferred
	 * 
	 * @param entry
	 * @return
	 */
	public InferType infer(
			final ByteArray id,
			final NNTYPE value );

	/**
	 * Clear the contents.
	 */
	public void clear();

	public int size();

	public boolean isEmpty();

}
