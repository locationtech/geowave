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
package org.locationtech.geowave.core.store.adapter.statistics;

import java.util.List;

import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class FieldNameStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	private final int bitPosition;

	public FieldNameStatisticVisibility(
			final String fieldName,
			final CommonIndexModel model,
			final DataTypeAdapter<T> adapter ) {
		this.bitPosition = adapter.getPositionOfOrderedField(
				model,
				fieldName);
	}

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow r : kvs) {
			for (final GeoWaveValue v : r.getFieldValues()) {
				final List<Integer> positions = BitmaskUtils.getFieldPositions(v.getFieldMask());
				if (positions.contains(bitPosition)) {
					return v.getVisibility();
				}
			}
		}
		return null;
	}
}
