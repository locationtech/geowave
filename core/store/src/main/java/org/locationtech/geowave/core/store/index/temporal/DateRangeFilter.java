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
package org.locationtech.geowave.core.store.index.temporal;

import java.nio.ByteBuffer;
import java.util.Date;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class DateRangeFilter implements
		QueryFilter
{
	protected String fieldName;
	protected Date start;
	protected Date end;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	public DateRangeFilter() {
		super();
	}

	public DateRangeFilter(
			final String fieldName,
			final Date start,
			final Date end,
			final boolean inclusiveLow,
			final boolean inclusiveHigh ) {
		super();
		this.fieldName = fieldName;
		this.start = start;
		this.end = end;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArray dateLongBytes = (ByteArray) persistenceEncoding.getCommonData().getValue(
				fieldName);
		if (dateLongBytes != null) {
			final long dateLong = Lexicoders.LONG.fromByteArray(dateLongBytes.getBytes());
			final Date value = new Date(
					dateLong);
			if (start != null) {
				if (inclusiveLow) {
					if (value.compareTo(start) < 0) {
						return false;
					}
					else if (value.compareTo(start) <= 0) {
						return false;
					}
				}
			}
			if (end != null) {
				if (inclusiveHigh) {
					if (value.compareTo(end) > 0) {
						return false;
					}
					else if (value.compareTo(end) >= 0) {
						return false;
					}
				}
			}
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public byte[] toBinary() {
		final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
		final ByteBuffer bb = ByteBuffer.allocate(VarintUtils.unsignedIntByteLength(fieldNameBytes.length)
				+ fieldNameBytes.length + VarintUtils.timeByteLength(start.getTime())
				+ VarintUtils.timeByteLength(end.getTime()) + 2);
		VarintUtils.writeUnsignedInt(
				fieldNameBytes.length,
				bb);
		bb.put(fieldNameBytes);
		VarintUtils.writeTime(
				start.getTime(),
				bb);
		VarintUtils.writeTime(
				end.getTime(),
				bb);
		bb.put((byte) (inclusiveLow ? 0x1 : 0x0));
		bb.put((byte) (inclusiveHigh ? 0x1 : 0x0));
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldNameBytes = new byte[VarintUtils.readUnsignedInt(bb)];
		bb.get(fieldNameBytes);
		fieldName = StringUtils.stringFromBinary(fieldNameBytes);
		start = new Date(
				VarintUtils.readTime(bb));
		end = new Date(
				VarintUtils.readTime(bb));
		inclusiveLow = (bb.get() > 0) ? true : false;
		inclusiveHigh = (bb.get() > 1) ? true : false;
	}
}
