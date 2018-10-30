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

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

abstract public class AbstractDataStatistics<T, R, B extends StatisticsQueryBuilder<R, B>> implements
		InternalDataStatistics<T, R, B>
{
	/**
	 * ID of source data adapter
	 */
	protected Short adapterId;
	protected byte[] visibility;
	/**
	 * ID of statistic to be tracked
	 */
	protected StatisticsType<R, B> statisticsType;

	protected String extendedId;

	protected AbstractDataStatistics() {}

	public AbstractDataStatistics(
			final Short internalDataAdapterId,
			final StatisticsType<R, B> statisticsType ) {
		this(
				internalDataAdapterId,
				statisticsType,
				"");
	}

	public AbstractDataStatistics(
			final Short adapterId,
			final StatisticsType<R, B> statisticsType,
			final String extendedId ) {
		this.adapterId = adapterId;
		this.statisticsType = statisticsType;
		this.extendedId = extendedId;
	}

	@Override
	public void setType(
			final StatisticsType<R, B> statisticsType ) {
		this.statisticsType = statisticsType;
	}

	@Override
	public void setExtendedId(
			final String extendedId ) {
		this.extendedId = extendedId;
	}

	@Override
	public String getExtendedId() {
		return extendedId;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public Short getAdapterId() {
		return adapterId;
	}

	@Override
	public void setAdapterId(
			final short adapterId ) {
		this.adapterId = adapterId;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}

	@Override
	public StatisticsType<R, B> getType() {
		return statisticsType;
	}

	protected ByteBuffer binaryBuffer(
			final int size ) {
		final byte stypeBytes[] = statisticsType.toBinary();
		final byte sidBytes[] = StringUtils.stringToBinary(extendedId);
		final ByteBuffer buffer = ByteBuffer.allocate(size + 6 + stypeBytes.length + sidBytes.length);
		buffer.putShort(adapterId);
		buffer.putShort((short) stypeBytes.length);
		buffer.putShort((short) sidBytes.length);
		buffer.put(stypeBytes);
		buffer.put(sidBytes);
		return buffer;
	}

	protected ByteBuffer binaryBuffer(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		adapterId = buffer.getShort();
		final int typeLength = Short.toUnsignedInt(buffer.getShort());
		final int extenedIdLength = Short.toUnsignedInt(buffer.getShort());
		final byte typeBytes[] = new byte[typeLength];
		buffer.get(typeBytes);
		statisticsType = new BaseStatisticsType();
		statisticsType.fromBinary(typeBytes);
		final byte[] extendedIdBytes = new byte[extenedIdLength];
		buffer.get(extendedIdBytes);
		extendedId = StringUtils.stringFromBinary(extendedIdBytes);
		return buffer;
	}

	@SuppressWarnings("unchecked")
	public InternalDataStatistics<T, R, B> duplicate() {
		return (InternalDataStatistics<T, R, B>) PersistenceUtils.fromBinary(PersistenceUtils.toBinary(this));
	}

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"dataType",
				store.getTypeName(adapterId));
		jo.put(
				"statsType",
				statisticsType.getString());
		if ((extendedId != null) && !extendedId.isEmpty()) {
			jo.put(
					"extendedId",
					extendedId);
		}
		jo.put(
				resultsName(),
				resultsValue());
		return jo;
	}

	protected abstract String resultsName();

	protected abstract Object resultsValue();

	@Override
	public String toString() {
		return "AbstractDataStatistics [adapterId=" + adapterId + ", statisticsType=" + statisticsType.getString()
				+ "]";
	}
}
