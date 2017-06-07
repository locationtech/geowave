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
package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

import com.google.protobuf.InvalidProtocolBufferException;

import mil.nga.giat.geowave.datastore.hbase.query.protobuf.FilterProtos;

/**
 * This is a Filter which will run on Tablet Server during Scan. HBase uses
 * these filters instead of Iterators. It makes use of Protocol Buffer library
 * See {@link https ://developers.google.com/protocol-buffers/docs/javatutorial}
 * for more info.
 */
public class SingleEntryFilter extends
		FilterBase
{

	public static final String ADAPTER_ID = "adapterid";
	public static final String DATA_ID = "dataid";
	private final byte[] adapterId;
	private final byte[] dataId;

	public SingleEntryFilter(
			final byte[] dataId,
			final byte[] adapterId ) {

		if (adapterId == null) {
			throw new IllegalArgumentException(
					"'adapterid' must be set for " + SingleEntryFilter.class.getName());
		}
		if (dataId == null) {
			throw new IllegalArgumentException(
					"'dataid' must be set for " + SingleEntryFilter.class.getName());
		}

		this.adapterId = adapterId;
		this.dataId = dataId;
	}

	@Override
	public ReturnCode filterKeyValue(
			final Cell v )
			throws IOException {

		boolean accept = true;

		final byte[] localAdapterId = CellUtil.cloneFamily(v);

		if (Arrays.equals(
				localAdapterId,
				adapterId)) {
			final byte[] rowId = CellUtil.cloneRow(v);

			final byte[] metadata = Arrays.copyOfRange(
					rowId,
					rowId.length - 12,
					rowId.length);

			final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
			final int adapterIdLength = metadataBuf.getInt();
			final int dataIdLength = metadataBuf.getInt();

			final ByteBuffer buf = ByteBuffer.wrap(
					rowId,
					0,
					rowId.length - 12);
			final byte[] indexId = new byte[rowId.length - 12 - adapterIdLength - dataIdLength];
			final byte[] rawAdapterId = new byte[adapterIdLength];
			final byte[] rawDataId = new byte[dataIdLength];
			buf.get(indexId);
			buf.get(rawAdapterId);
			buf.get(rawDataId);

			if (!Arrays.equals(
					rawDataId,
					dataId) && Arrays.equals(
					rawAdapterId,
					adapterId)) {
				accept = false;
			}
		}
		else {
			accept = false;
		}

		return accept ? ReturnCode.INCLUDE : ReturnCode.SKIP;
	}

	public static Filter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		FilterProtos.SingleEntryFilter proto;
		try {
			proto = FilterProtos.SingleEntryFilter.parseFrom(pbBytes);
		}
		catch (final InvalidProtocolBufferException e) {
			throw new DeserializationException(
					e);
		}
		return new SingleEntryFilter(
				proto.getDataId().toByteArray(),
				proto.getAdapterId().toByteArray());
	}

	@Override
	public byte[] toByteArray() {
		final FilterProtos.SingleEntryFilter.Builder builder = FilterProtos.SingleEntryFilter.newBuilder();
		if (adapterId != null) {
			builder.setAdapterId(ByteStringer.wrap(adapterId));
		}
		if (dataId != null) {
			builder.setDataId(ByteStringer.wrap(dataId));
		}

		return builder.build().toByteArray();
	}
}
