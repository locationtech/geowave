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
package org.locationtech.geowave.core.store.entities;

import java.util.Iterator;

import com.google.common.base.Function;

/**
 * Interface for a function that transforms an iterator of {@link GeoWaveRow}s
 * to another type. The interface transforms an iterator rather than an
 * individual row to allow iterators to merge rows before transforming them if
 * needed.
 *
 * @param <T>
 *            the type to transform each {@link GeoWaveRow} into
 */
public interface GeoWaveRowIteratorTransformer<T> extends
		Function<Iterator<GeoWaveRow>, Iterator<T>>
{
	public static GeoWaveRowIteratorTransformer<GeoWaveRow> NO_OP_TRANSFORMER = new GeoWaveRowIteratorTransformer<GeoWaveRow>() {

		@Override
		public Iterator<GeoWaveRow> apply(
				Iterator<GeoWaveRow> input ) {
			return input;
		}

	};
}
