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
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;

public interface Aggregation<P extends Persistable, R, T> extends
		Persistable
{
	/**
	 * Returns a persistable object for any parameters that must be persisted to
	 * properly compute the aggregation
	 *
	 * @return A persistable object for any parameters that must be persisted to
	 *         properly compute the aggregation
	 */
	P getParameters();

	/**
	 * Sets the parameters based on what has been persisted
	 *
	 * @param parameters
	 *            the persisted parameters for this aggregation function
	 */
	void setParameters(
			P parameters );

	/**
	 * Get the current result of the aggregation. This must be mergeable and it
	 * is the responsibility of the caller to merge separate results if desired.
	 * It is the responsibility of the aggregation to start with a new instance
	 * of the result at the beginning of any aggregation.
	 *
	 * @return the current result of the aggregation
	 */
	R getResult();

	R merge(
			R result1,
			R result2 );

	byte[] resultToBinary(
			R result );

	R resultFromBinary(
			byte[] binary );

	/**
	 * this will be called if the result should be reset to its default value
	 *
	 */
	void clearResult();

	/**
	 * Update the aggregation result using the new entry provided
	 *
	 * @param entry
	 *            the new entry to compute an updated aggregation result on
	 */
	void aggregate(
			T entry );
}
