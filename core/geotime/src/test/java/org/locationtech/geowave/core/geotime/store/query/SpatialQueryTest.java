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
package org.locationtech.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class SpatialQueryTest
{
	@Test
	public void test() {
		final GeometryFactory factory = new GeometryFactory();
		final SpatialQuery query = new SpatialQuery(
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
		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());
		assertEquals(
				queryCopy.getQueryGeometry(),
				query.getQueryGeometry());
	}

	private IndexedPersistenceEncoding createData(
			final Geometry geomData ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<>();

		commonData.addValue(
				GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME,
				new GeometryWrapper(
						geomData));

		return new IndexedPersistenceEncoding(
				(short) 1,
				new ByteArray(
						"1"),
				new ByteArray(
						"1"),
				new ByteArray(
						"1"),
				1,
				commonData,
				new PersistentDataset<byte[]>());
	}

	public void performOp(
			final CompareOperation op,
			final boolean[] expectedResults ) {
		final GeometryFactory factory = new GeometryFactory();
		// query geometry for testing
		final Coordinate[] queryCoord = new Coordinate[] {
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
		};
		// create spatial query object with geometric relationship operator
		final SpatialQuery query = new SpatialQuery(
				factory.createPolygon(queryCoord),
				op);

		final SpatialQuery queryCopy = new SpatialQuery();
		queryCopy.fromBinary(query.toBinary());

		// This line is crossing query polygon
		final Coordinate[] line1 = new Coordinate[] {
			new Coordinate(
					22,
					32),
			new Coordinate(
					25,
					36)
		};
		// This line is completely within the query polygon
		final Coordinate[] line2 = new Coordinate[] {
			new Coordinate(
					25,
					33.5),
			new Coordinate(
					26,
					34)
		};
		// This line is completely outside of the query polygon
		final Coordinate[] line3 = new Coordinate[] {
			new Coordinate(
					21,
					33.5),
			new Coordinate(
					23,
					34)
		};
		// This line is touching one of the corner of the query polygon
		final Coordinate[] line4 = new Coordinate[] {
			new Coordinate(
					28,
					33),
			new Coordinate(
					30,
					34)
		};
		// this polygon is completely contained within the query polygon
		final Coordinate[] smallPolygon = new Coordinate[] {
			new Coordinate(
					25,
					34),
			new Coordinate(
					27,
					34),
			new Coordinate(
					27,
					36),
			new Coordinate(
					25,
					36),
			new Coordinate(
					25,
					34)
		};

		// this polygon is same as query polygon
		final Coordinate[] dataPolygon = queryCoord.clone();

		final IndexedPersistenceEncoding[] data = new IndexedPersistenceEncoding[] {
			createData(factory.createLineString(line1)),
			createData(factory.createLineString(line2)),
			createData(factory.createLineString(line3)),
			createData(factory.createLineString(line4)),
			createData(factory.createPolygon(smallPolygon)),
			createData(factory.createPolygon(dataPolygon))
		};

		int pos = 0;
		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		for (final IndexedPersistenceEncoding dataItem : data) {
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
	public void testContains() {
		performOp(
				CompareOperation.CONTAINS,
				new boolean[] {
					false,
					true,
					false,
					false,
					true,
					true
				});
	}

	@Test
	public void testOverlaps() {
		performOp(
				CompareOperation.OVERLAPS,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testIntersects() {
		performOp(
				CompareOperation.INTERSECTS,
				new boolean[] {
					true,
					true,
					false,
					true,
					true,
					true
				});
	}

	@Test
	public void testDisjoint() {
		performOp(
				CompareOperation.DISJOINT,
				new boolean[] {
					false,
					false,
					true,
					false,
					false,
					false
				});
	}

	@Test
	public void testTouches() {
		performOp(
				CompareOperation.TOUCHES,
				new boolean[] {
					false,
					false,
					false,
					true,
					false,
					false
				});
	}

	@Test
	public void testCrosses() {
		performOp(
				CompareOperation.CROSSES,
				new boolean[] {
					true,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testWithin() {
		performOp(
				CompareOperation.WITHIN,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					true
				});
	}

	@Test
	public void testEquals() {
		performOp(
				CompareOperation.EQUALS,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					true
				});
	}
}
