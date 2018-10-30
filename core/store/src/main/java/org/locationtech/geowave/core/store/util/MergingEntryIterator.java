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
package org.locationtech.geowave.core.store.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class MergingEntryIterator<T> extends
		NativeEntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(NativeEntryIteratorWrapper.class);

	private final Map<Short, RowMergingDataAdapter> mergingAdapters;
	private final Map<Short, RowTransform> transforms;

	public MergingEntryIterator(
			final PersistentAdapterStore adapterStore,
			final Index index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, GeoWaveRow> scanCallback,
			final Map<Short, RowMergingDataAdapter> mergingAdapters,
			final double[] maxResolutionSubsamplingPerDimension ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback,
				null,
				maxResolutionSubsamplingPerDimension,
				true);
		this.mergingAdapters = mergingAdapters;
		transforms = new HashMap<Short, RowTransform>();
	}

	protected GeoWaveRow getNextEncodedResult() {
		GeoWaveRow nextResult = scannerIt.next();

		final short internalAdapterId = nextResult.getAdapterId();

		final RowMergingDataAdapter mergingAdapter = mergingAdapters.get(internalAdapterId);

		if ((mergingAdapter != null) && (mergingAdapter.getTransform() != null)) {
			final RowTransform rowTransform = getRowTransform(
					internalAdapterId,
					mergingAdapter);

			// This iterator expects a single GeoWaveRow w/ multiple fieldValues
			// (HBase)
			nextResult = mergeSingleRowValues(
					nextResult,
					rowTransform);
		}

		return nextResult;
	}

	private RowTransform getRowTransform(
			short internalAdapterId,
			RowMergingDataAdapter mergingAdapter ) {
		RowTransform transform = transforms.get(internalAdapterId);
		if (transform == null) {
			transform = mergingAdapter.getTransform();
			// set strategy
			try {
				transform.initOptions(mergingAdapter.getOptions(
						internalAdapterId,
						null));
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to initialize merge strategy for adapter: " + mergingAdapter.getTypeName(),
						e);
			}
			transforms.put(
					internalAdapterId,
					transform);
		}

		return transform;
	}

	protected GeoWaveRow mergeSingleRowValues(
			final GeoWaveRow singleRow,
			final RowTransform rowTransform ) {
		if (singleRow.getFieldValues().length < 2) {
			return singleRow;
		}

		// merge all values into a single value
		Mergeable merged = null;

		for (GeoWaveValue fieldValue : singleRow.getFieldValues()) {
			final Mergeable mergeable = rowTransform.getRowAsMergeableObject(
					singleRow.getAdapterId(),
					new ByteArray(
							fieldValue.getFieldMask()),
					fieldValue.getValue());

			if (merged == null) {
				merged = mergeable;
			}
			else {
				merged.merge(mergeable);
			}
		}

		GeoWaveValue[] mergedFieldValues = new GeoWaveValue[] {
			new GeoWaveValueImpl(
					singleRow.getFieldValues()[0].getFieldMask(),
					singleRow.getFieldValues()[0].getVisibility(),
					rowTransform.getBinaryFromMergedObject(merged))
		};

		return new GeoWaveRowImpl(
				new GeoWaveKeyImpl(
						singleRow.getDataId(),
						singleRow.getAdapterId(),
						singleRow.getPartitionKey(),
						singleRow.getSortKey(),
						singleRow.getNumberOfDuplicates()),
				mergedFieldValues);
	}

	@Override
	protected boolean hasNextScannedResult() {
		return scannerIt.hasNext();
	}

	@Override
	public void remove() {
		throw new NotImplementedException(
				"Transforming iterator cannot use remove()");
	}

}
