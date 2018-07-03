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
package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time.TimeRange;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class SpatialTemporalQueryTest
{
	SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssz");

	@Test
	public void test()
			throws ParseException {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialTemporalQuery query = new SpatialTemporalQuery(
				df.parse("2005-05-17T19:32:56GMT-00:00"),
				df.parse("2005-05-17T22:32:56GMT-00:00"),
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							24,
							33),
					new Coordinate(
							28,
							33),
					new Coordinate(
							28,
							31),
					new Coordinate(
							24,
							31),
					new Coordinate(
							24,
							33)
				}));
		final SpatialTemporalQuery queryCopy = new SpatialTemporalQuery();
		queryCopy.fromBinary(query.toBinary());
		assertEquals(
				queryCopy.getQueryGeometry(),
				query.getQueryGeometry());
	}

	private CommonIndexedPersistenceEncoding createData(
			final Date start,
			final Date end,
			final Coordinate[] coordinates ) {
		final GeometryFactory factory = new GeometryFactory();
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		commonData.addValue(
				GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID,
				new GeometryWrapper(
						factory.createLineString(coordinates)));
		commonData.addValue(
				new TimeField(
						Unit.YEAR).getFieldId(),
				new TimeRange(
						start.getTime(),
						end.getTime(),
						new byte[0]));

		return new CommonIndexedPersistenceEncoding(
				(short) 1,
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				new ByteArrayId(
						"1"),
				1,
				commonData,
				new PersistentDataset<byte[]>());
	}

	public void performOp(
			final CompareOperation op,
			final boolean[] expectedResults )
			throws ParseException {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialTemporalQuery query = new SpatialTemporalQuery(
				df.parse("2005-05-17T19:32:56GMT-00:00"),
				df.parse("2005-05-17T22:32:56GMT-00:00"),
				factory.createPolygon(new Coordinate[] {
					new Coordinate(
							24,
							33),
					new Coordinate(
							28,
							33),
					new Coordinate(
							28,
							37),
					new Coordinate(
							24,
							37),
					new Coordinate(
							24,
							33)
				}),
				op);
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());

		final CommonIndexedPersistenceEncoding[] data = new CommonIndexedPersistenceEncoding[] {
			createData(
					df.parse("2005-05-17T19:32:56GMT-00:00"),
					df.parse("2005-05-17T22:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T17:32:56GMT-00:00"),
					df.parse("2005-05-17T21:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T19:33:56GMT-00:00"),
					df.parse("2005-05-17T20:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T16:32:56GMT-00:00"),
					df.parse("2005-05-17T21:32:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					}),
			createData(
					df.parse("2005-05-17T22:33:56GMT-00:00"),
					df.parse("2005-05-17T22:34:56GMT-00:00"),
					new Coordinate[] {
						new Coordinate(
								25,
								33.5),
						new Coordinate(
								26,
								34)
					})
		};
		final PrimaryIndex index = new SpatialTemporalDimensionalityTypeProvider()
				.createPrimaryIndex(new SpatialTemporalOptions());
		int pos = 0;
		for (final CommonIndexedPersistenceEncoding dataItem : data) {
			for (final QueryFilter filter : queryCopy.createFilters(index)) {
				assertEquals(
						"result: " + pos,
						expectedResults[pos++],
						filter.accept(
								index.getIndexModel(),
								dataItem));
			}
		}
	}

	@Test
	public void testContains()
			throws ParseException {
		performOp(
				CompareOperation.CONTAINS,
				new boolean[] {
					true,
					false,
					true,
					false,
					false
				});
	}

	@Test
	public void testOverlaps()
			throws ParseException {
		performOp(
				CompareOperation.OVERLAPS,
				new boolean[] {
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testIntersects()
			throws ParseException {
		performOp(
				CompareOperation.INTERSECTS,
				new boolean[] {
					true,
					true,
					true,
					true,
					false
				});
	}
}
