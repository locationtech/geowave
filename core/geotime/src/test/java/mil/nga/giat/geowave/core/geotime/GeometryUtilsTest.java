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
package mil.nga.giat.geowave.core.geotime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class GeometryUtilsTest
{
	private float DELTA = 0;
	private Point point3D;
	private Point point2D;

	@Before
	public void createGeometry() {

		final GeometryFactory gf = new GeometryFactory();

		point2D = gf.createPoint(new Coordinate(
				1,
				2));

		point3D = gf.createPoint(new Coordinate(
				1,
				2,
				3));
	}

	@Test
	public void test2DGeometryBinaryConversion() {

		// convert 2D point to binary representation
		final byte[] bytes = GeometryUtils.geometryToBinary(point2D);

		// load the converted 2D geometry
		final Geometry convGeo = GeometryUtils.geometryFromBinary(bytes);

		// get the coordinates for each version
		final Coordinate origCoords = point2D.getCoordinates()[0];
		final Coordinate convCoords = convGeo.getCoordinates()[0];

		Assert.assertEquals(
				origCoords.x,
				convCoords.x,
				DELTA);

		Assert.assertEquals(
				origCoords.y,
				convCoords.y,
				DELTA);

		Assert.assertTrue(Double.isNaN(convCoords.getOrdinate(Coordinate.Z)));
	}

	@Test
	public void test3DGeometryBinaryConversion() {

		// convert 3D point to binary representation
		final byte[] bytes = GeometryUtils.geometryToBinary(point3D);

		// load the converted 3D geometry
		final Geometry convGeo = GeometryUtils.geometryFromBinary(bytes);

		// get the coordinates for each version
		final Coordinate origCoords = point3D.getCoordinates()[0];
		final Coordinate convCoords = convGeo.getCoordinates()[0];

		Assert.assertEquals(
				origCoords.x,
				convCoords.x,
				DELTA);

		Assert.assertEquals(
				origCoords.y,
				convCoords.y,
				DELTA);

		Assert.assertEquals(
				origCoords.z,
				convCoords.z,
				DELTA);
	}

	@Test
	public void testConstraintGeneration() {

		final GeometryFactory gf = new GeometryFactory();
		final Geometry multiPolygon = gf.createMultiPolygon(new Polygon[] {
			gf.createPolygon(new Coordinate[] {
				new Coordinate(
						20.0,
						30),
				new Coordinate(
						20,
						40),
				new Coordinate(
						10,
						40),
				new Coordinate(
						10,
						30),
				new Coordinate(
						20,
						30)
			}),
			gf.createPolygon(new Coordinate[] {
				new Coordinate(
						-9,
						-2),
				new Coordinate(
						-9,
						-1),
				new Coordinate(
						-8,
						-1),
				new Coordinate(
						-8,
						-2),
				new Coordinate(
						-9,
						-2)
			})
		});
		final Constraints constraints = GeometryUtils.basicConstraintsFromGeometry(multiPolygon);
		final List<MultiDimensionalNumericData> results = constraints
				.getIndexConstraints(new ExampleNumericIndexStrategy());
		assertEquals(
				2,
				results.size());
		assertTrue(Arrays.equals(
				new double[] {
					10,
					30
				},
				results.get(
						0).getMinValuesPerDimension()));
		assertTrue(Arrays.equals(
				new double[] {
					20,
					40
				},
				results.get(
						0).getMaxValuesPerDimension()));
		assertTrue(Arrays.equals(
				new double[] {
					-9,
					-2
				},
				results.get(
						1).getMinValuesPerDimension()));
		assertTrue(Arrays.equals(
				new double[] {
					-8,
					-1
				},
				results.get(
						1).getMaxValuesPerDimension()));

	}

	public static class ExampleNumericIndexStrategy implements
			NumericIndexStrategy
	{

		@Override
		public byte[] toBinary() {
			return null;
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

		@Override
		public List<ByteArrayRange> getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				IndexMetaData... hints ) {
			return null;
		}

		@Override
		public List<ByteArrayRange> getQueryRanges(
				final MultiDimensionalNumericData indexedRange,
				final int maxEstimatedRangeDecomposition,
				IndexMetaData... hints ) {
			return null;
		}

		@Override
		public List<ByteArrayId> getInsertionIds(
				final MultiDimensionalNumericData indexedData ) {
			return null;
		}

		@Override
		public List<ByteArrayId> getInsertionIds(
				final MultiDimensionalNumericData indexedData,
				final int maxEstimatedDuplicateIds ) {
			return null;
		}

		@Override
		public MultiDimensionalNumericData getRangeForId(
				final ByteArrayId insertionId ) {
			return null;
		}

		@Override
		public MultiDimensionalCoordinates getCoordinatesPerDimension(
				final ByteArrayId insertionId ) {
			return null;
		}

		@Override
		public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
			return new NumericDimensionDefinition[] {
				new LongitudeDefinition(),
				new LatitudeDefinition()
			};
		}

		@Override
		public String getId() {
			return "test-gt";
		}

		@Override
		public double[] getHighestPrecisionIdRangePerDimension() {
			return null;
		}

		@Override
		public Set<ByteArrayId> getNaturalSplits() {
			return null;
		}

		@Override
		public int getByteOffsetFromDimensionalIndex() {
			return 0;
		}

		@Override
		public List<IndexMetaData> createMetaData() {
			return Collections.emptyList();
		}

		@Override
		public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
				MultiDimensionalNumericData dataRange,
				IndexMetaData... hints ) {
			return null;
		}
	}
}
