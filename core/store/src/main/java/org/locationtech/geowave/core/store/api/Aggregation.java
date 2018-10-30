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

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * An Aggregation function that mathematically represents any commutative monoid
 * (ie. a function that is both commutative and associative). For some data
 * stores Aggregations will be run distributed on the server within the scope of
 * iterating through the results for maximum efficiency. A third party
 * Aggregation can be used, but if serverside processing is enabled, the third
 * party Aggregation implementation must also be on the server classpath.
 *
 * @param <P>
 *            input type for the aggregation
 * @param <R>
 *            result type for the aggregation
 * @param <T>
 *            data type of the entries for the aggregation
 */
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

	/**
	 * this is responsible for reducing 2 results to a single result by
	 *
	 * @param result1
	 * @param result2
	 * @return
	 */
	default R merge(
			final R result1,
			final R result2 ) {
		if (result1 == null) {
			return result2;
		}
		else if (result2 == null) {
			return result1;
		}
		else if ((result1 instanceof Mergeable) && (result2 instanceof Mergeable)) {
			((Mergeable) result1)
					.merge(
							(Mergeable) result2);
			return result1;
		}

		return null;
	}

	/**
	 * This is responsible for writing the result to binary
	 *
	 * @param result
	 *            the result value
	 * @return the binary representing this value
	 */
	byte[] resultToBinary(
			R result );

	/**
	 * This is responsible for reading the result from binary
	 *
	 * @param binary
	 *            the binary representing this result
	 * @return the result value
	 */
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
