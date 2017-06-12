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
package mil.nga.giat.geowave.core.store.index.temporal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;

import org.junit.Test;

public class DateRangeFilterTest
{
	private static final SimpleDateFormat format = new SimpleDateFormat(
			"MM-dd-yyyy HH:mm:ss");

	@Test
	public void testSerialization() {
		final Date start = new Date();
		final Date end = new Date();
		final DateRangeFilter filter = new DateRangeFilter(
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				start,
				end,
				false,
				false);
		final byte[] filterBytes = PersistenceUtils.toBinary(filter);
		final DateRangeFilter deserializedFilter = (DateRangeFilter) PersistenceUtils.fromBinary(filterBytes);
		Assert.assertTrue(filter.fieldId.equals(deserializedFilter.fieldId));
		Assert.assertTrue(filter.start.equals(deserializedFilter.start));
		Assert.assertTrue(filter.end.equals(deserializedFilter.end));
		Assert.assertTrue(filter.inclusiveLow == deserializedFilter.inclusiveLow);
		Assert.assertTrue(filter.inclusiveHigh == deserializedFilter.inclusiveHigh);
	}

	@Test
	public void testAccept()
			throws ParseException {
		final DateRangeFilter filter = new DateRangeFilter(
				new ByteArrayId(
						StringUtils.stringToBinary("myAttribute")),
				format.parse("01-01-2014 11:01:01"),
				format.parse("12-31-2014 11:01:01"),
				true,
				true);

		// should match because date is in range
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										TemporalIndexStrategy.toIndexByte(format.parse("06-01-2014 11:01:01"))))),
				null);

		Assert.assertTrue(filter.accept(
				null,
				persistenceEncoding));

		// should not match because date is out of range
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding2 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"myAttribute"),
								new ByteArrayId(
										Lexicoders.LONG.toByteArray(format.parse(
												"01-01-2015 11:01:01").getTime())))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding2));

		// should not match because of attribute mismatch
		final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding3 = new IndexedPersistenceEncoding<ByteArrayId>(
				null,
				null,
				null,
				0,
				new PersistentDataset<ByteArrayId>(
						new PersistentValue<ByteArrayId>(
								new ByteArrayId(
										"mismatch"),
								new ByteArrayId(
										Lexicoders.LONG.toByteArray(format.parse(
												"06-01-2014 11:01:01").getTime())))),
				null);

		Assert.assertFalse(filter.accept(
				null,
				persistenceEncoding3));
	}
}
