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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.index.temporal.DateRangeFilter;
import org.locationtech.geowave.core.store.index.temporal.TemporalIndexStrategy;
import org.junit.Assert;

public class DateRangeFilterTest
{
	private static final SimpleDateFormat format = new SimpleDateFormat(
			"MM-dd-yyyy HH:mm:ss");

	@Test
	public void testSerialization() {
		final Date start = new Date();
		final Date end = new Date();
		final DateRangeFilter filter = new DateRangeFilter(
				"myAttribute",
				start,
				end,
				false,
				false);
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final DateRangeFilter deserializedFilter = (DateRangeFilter) PersistenceUtils.fromBinary(filterBytes);
		Assert.assertTrue(filter.fieldName.equals(deserializedFilter.fieldName));
		Assert.assertTrue(filter.start.equals(deserializedFilter.start));
		Assert.assertTrue(filter.end.equals(deserializedFilter.end));
		Assert.assertTrue(filter.inclusiveLow == deserializedFilter.inclusiveLow);
		Assert.assertTrue(filter.inclusiveHigh == deserializedFilter.inclusiveHigh);
	}

	@Test
	public void testAccept()
			throws ParseException {
		final DateRangeFilter filter = new DateRangeFilter(
				"myAttribute",
				format.parse("01-01-2014 11:01:01"),
				format.parse("12-31-2014 11:01:01"),
				true,
				true);

		// should match because date is in range
		final IndexedPersistenceEncoding<ByteArray> persistenceEncoding = new IndexedPersistenceEncoding<ByteArray>(
				null,
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArray>(
						"myAttribute",
						new ByteArray(
								TemporalIndexStrategy.toIndexByte(format.parse("06-01-2014 11:01:01")))),
				null);

		Assert.assertTrue(filter.accept(
				null,
				persistenceEncoding));

		// should not match because date is out of range
		final IndexedPersistenceEncoding<ByteArray> persistenceEncoding2 = new IndexedPersistenceEncoding<ByteArray>(
				null,
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArray>(
						"myAttribute",
						new ByteArray(
								Lexicoders.LONG.toByteArray(format.parse(
										"01-01-2015 11:01:01").getTime()))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding2));

		// should not match because of attribute mismatch
		final IndexedPersistenceEncoding<ByteArray> persistenceEncoding3 = new IndexedPersistenceEncoding<ByteArray>(
				null,
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArray>(
						"mismatch",
						new ByteArray(
								Lexicoders.LONG.toByteArray(format.parse(
										"06-01-2014 11:01:01").getTime()))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding3));
	}
}
