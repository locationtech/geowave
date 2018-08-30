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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class CountDataStatistics<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	public final static ByteArrayId STATS_TYPE = new ByteArrayId(
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
	private transient HashSet<ByteArrayId> ids = new HashSet<ByteArrayId>();

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kv ) {
		if (kv.length > 0) {
			if (ids.add(new ByteArrayId(
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
				"count[internalDataAdapterId=").append(
				super.getInternalDataAdapterId());
		buffer.append(
				", count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Count statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"statisticsID",
				statisticsId.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"count",
				count);
		return jo;
	}
}
