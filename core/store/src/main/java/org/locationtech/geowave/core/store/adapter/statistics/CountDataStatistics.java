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

import java.nio.ByteBuffer;
import java.util.HashSet;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class CountDataStatistics<T> extends
		AbstractDataStatistics<T, Long, BaseStatisticsQueryBuilder<Long>> implements
		DeleteCallback<T, GeoWaveRow>
{
	public final static BaseStatisticsType<Long> STATS_TYPE = new BaseStatisticsType<>(
			"COUNT_DATA");

	private long count = Long.MIN_VALUE;

	public CountDataStatistics() {
		this(
				null);
	}

	public CountDataStatistics(
			final Short internalDataAdapterId ) {
		super(
				internalDataAdapterId,
				STATS_TYPE);
	}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	public long getCount() {
		return count;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(8);
		buffer.putLong(count);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		count = buffer.getLong();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (!isSet()) {
			count = 0;
		}
		count += 1;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (!isSet()) {
			count = 0;
		}
		if ((statistics != null) && (statistics instanceof CountDataStatistics)) {
			@SuppressWarnings("unchecked")
			final CountDataStatistics<T> cStats = (CountDataStatistics<T>) statistics;
			if (cStats.isSet()) {
				count = count + cStats.count;
			}
		}
	}

	/**
	 * This is expensive, but necessary since there may be duplicates
	 */
	// TODO entryDeleted should only be called once with all duplicates
	private transient HashSet<ByteArray> ids = new HashSet<>();

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kv ) {
		if (kv.length > 0) {
			if (ids.add(new ByteArray(
					kv[0].getDataId()))) {
				if (!isSet()) {
					count = 0;
				}
				count -= 1;
			}
		}
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[adapterId=").append(
				super.getAdapterId());
		buffer.append(
				", count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public Long getResult() {
		return count;
	}

	@Override
	protected String resultsName() {
		return "count";
	}

	@Override
	protected Object resultsValue() {
		return Long.toString(count);
	}
}
