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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import mil.nga.giat.geowave.core.index.IndexUtils;

public class FixedCardinalitySkippingFilter extends
		FilterBase
{
	private Integer bitPosition;
	private byte[] nextRow = null;
	private ReturnCode returnCode;
	private boolean init = false;

	public FixedCardinalitySkippingFilter() {}

	public FixedCardinalitySkippingFilter(
			final Integer bitPosition ) {
		this.bitPosition = bitPosition;
	}

	@Override
	public ReturnCode filterKeyValue(
			final Cell cell )
			throws IOException {
		// Make sure we have the next row to include
		if (!init) {
			init = true;
			getNextRowKey(cell);
		}

		// Compare current row w/ next row
		returnCode = checkNextRow(cell);

		// If we're at or past the next row, advance it
		if (returnCode != ReturnCode.SKIP) {
			getNextRowKey(cell);
		}

		return returnCode;
	}

	private ReturnCode checkNextRow(
			final Cell cell ) {
		final byte[] row = CellUtil.cloneRow(cell);

		final byte[] rowCopy = new byte[nextRow.length];

		System.arraycopy(
				row,
				0,
				rowCopy,
				0,
				rowCopy.length);

		final int cmp = Bytes.compareTo(
				rowCopy,
				nextRow);

		if (cmp < 0) {
			return ReturnCode.SKIP;
		}
		else {
			return ReturnCode.INCLUDE;
		}
	}

	private void getNextRowKey(
			final Cell currentCell ) {
		final byte[] row = CellUtil.cloneRow(currentCell);

		nextRow = IndexUtils.getNextRowForSkip(
				row,
				bitPosition);
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
		buf.putInt(bitPosition);

		return buf.array();
	}

	public static FixedCardinalitySkippingFilter parseFrom(
			final byte[] bytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int bitpos = buf.getInt();

		return new FixedCardinalitySkippingFilter(
				bitpos);
	}

}
