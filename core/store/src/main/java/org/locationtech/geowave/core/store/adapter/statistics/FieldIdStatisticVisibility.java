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

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

public class FieldIdStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	private final int bitPosition;

	public FieldIdStatisticVisibility(
			final ByteArrayId fieldId,
			final CommonIndexModel model,
			final DataAdapter adapter ) {
		this.bitPosition = adapter.getPositionOfOrderedField(
				model,
				fieldId);
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
