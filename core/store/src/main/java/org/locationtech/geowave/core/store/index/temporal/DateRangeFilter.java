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
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldNameBytes.length + 8 + 8 + 8);
		bb.putInt(fieldNameBytes.length);
		bb.put(fieldNameBytes);
		bb.putLong(start.getTime());
		bb.putLong(end.getTime());
		final int rangeInclusiveHighInt = (inclusiveHigh) ? 1 : 0;
		final int rangeInclusiveLowInt = (inclusiveLow) ? 1 : 0;
		bb.putInt(rangeInclusiveLowInt);
		bb.putInt(rangeInclusiveHighInt);
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldNameBytes = new byte[bb.getInt()];
		bb.get(fieldNameBytes);
		fieldName = StringUtils.stringFromBinary(fieldNameBytes);
		start = new Date(
				bb.getLong());
		end = new Date(
				bb.getLong());
		inclusiveLow = (bb.getInt() == 1) ? true : false;
		inclusiveHigh = (bb.getInt() == 1) ? true : false;
	}
}
