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
package mil.nga.giat.geowave.core.store.query;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Coordinate;
import mil.nga.giat.geowave.core.index.CoordinateRange;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;

public class CoordinateRangeUtils
{
	public static interface RangeCache
	{
		public boolean inBounds(
				final MultiDimensionalCoordinates coordinates );
	}

	private static interface RangeByBinIdCache
	{
		public boolean inBounds(
				final Coordinate coordinate );
	}

	public static class RangeLookupFactory
	{
		public static RangeCache createMultiRangeLookup(
				final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
			if ((coordinateRanges == null) || (coordinateRanges.length == 0)) {
				return new NullRangeLookup();
			}
			else if (coordinateRanges.length == 1) {
				return createRangeLookup(coordinateRanges[0].getRangesArray());
			}
			else {
				return new MultiRangeCacheLookup(
						coordinateRanges);
			}
		}

		public static RangeCache createRangeLookup(
				final MultiDimensionalCoordinateRanges[] coordinateRanges ) {
			if (coordinateRanges == null) {
				return new NullRangeLookup();
			}
			else if ((coordinateRanges.length == 1) && (coordinateRanges[0].getMultiDimensionalId() == null)) {
				return new SingleRangeLookup(
						coordinateRanges[0]);
			}
			else {
				return new MultiRangeLookup(
						coordinateRanges);
			}
		}
	}

	private static class MultiRangeCacheLookup implements
			RangeCache
	{
		private final RangeCache[] rangeCaches;

		public MultiRangeCacheLookup(
				final MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
			rangeCaches = new RangeCache[coordinateRanges.length];
			for (int i = 0; i < coordinateRanges.length; i++) {
				rangeCaches[i] = RangeLookupFactory.createRangeLookup(coordinateRanges[i].getRangesArray());
			}
		}

		@Override
		public boolean inBounds(
				final MultiDimensionalCoordinates coordinates ) {
			// this should act as an OR clause
			for (final RangeCache r : rangeCaches) {
				if (r.inBounds(coordinates)) {
					return true;
				}
			}
			return false;
		}

	}

	private static class NullRangeLookup implements
			RangeCache
	{
		@Override
		public boolean inBounds(
				final MultiDimensionalCoordinates coordinates ) {
			return false;
		}
	}

	private static class SingleRangeLookup implements
			RangeCache
	{
		private final MultiDimensionalBinLookup singleton;

		public SingleRangeLookup(
				final MultiDimensionalCoordinateRanges coordinateRanges ) {
			singleton = new MultiDimensionalBinLookup(
					coordinateRanges);
		}

		@Override
		public boolean inBounds(
				final MultiDimensionalCoordinates coordinates ) {
			return inBounds(
					coordinates,
					singleton);
		}

		private static boolean inBounds(
				final MultiDimensionalCoordinates coordinates,
				final MultiDimensionalBinLookup binLookup ) {
			final CoordinateRange[] retVal = new CoordinateRange[coordinates.getNumDimensions()];
			for (int d = 0; d < retVal.length; d++) {
				final Coordinate c = coordinates.getCoordinate(d);
				if (!binLookup.inBounds(
						d,
						c)) {
					return false;
				}
			}
			return true;
		}
	}

	private static class MultiRangeLookup implements
			RangeCache
	{
		private final Map<ByteArrayId, MultiDimensionalBinLookup> multiDimensionalIdToRangeMap;

		public MultiRangeLookup(
				final MultiDimensionalCoordinateRanges[] coordinateRanges ) {
			multiDimensionalIdToRangeMap = new HashMap<>();
			for (final MultiDimensionalCoordinateRanges r : coordinateRanges) {
				multiDimensionalIdToRangeMap.put(
						new ByteArrayId(
								r.getMultiDimensionalId()),
						new MultiDimensionalBinLookup(
								r));
			}
		}

		@Override
		public boolean inBounds(
				final MultiDimensionalCoordinates coordinates ) {
			final MultiDimensionalBinLookup binLookup = multiDimensionalIdToRangeMap.get(new ByteArrayId(
					coordinates.getMultiDimensionalId()));
			if (binLookup == null) {
				return false;
			}

			return SingleRangeLookup.inBounds(
					coordinates,
					binLookup);
		}
	}

	private static class BinLookupFactory
	{
		public static RangeByBinIdCache createBinLookup(
				final CoordinateRange[] coordinateRanges ) {
			if (coordinateRanges == null) {
				return new NullBinLookup();
			}
			else if ((coordinateRanges.length == 1) && (coordinateRanges[0].getBinId() == null)) {
				return new SingleBinLookup(
						coordinateRanges[0]);
			}
			else {
				return new MultiBinLookup(
						coordinateRanges);
			}
		}
	}

	private static class MultiDimensionalBinLookup
	{
		private final RangeByBinIdCache[] rangePerDimensionCache;

		private MultiDimensionalBinLookup(
				final MultiDimensionalCoordinateRanges ranges ) {
			rangePerDimensionCache = new RangeByBinIdCache[ranges.getNumDimensions()];
			for (int d = 0; d < rangePerDimensionCache.length; d++) {
				rangePerDimensionCache[d] = BinLookupFactory.createBinLookup(ranges.getRangeForDimension(d));
			}
		}

		public boolean inBounds(
				final int dimension,
				final Coordinate coordinate ) {
			return rangePerDimensionCache[dimension].inBounds(coordinate);

		}

	}

	private static class NullBinLookup implements
			RangeByBinIdCache
	{

		@Override
		public boolean inBounds(
				final Coordinate coordinate ) {
			return false;
		}
	}

	private static class SingleBinLookup implements
			RangeByBinIdCache
	{
		private final CoordinateRange singleton;

		public SingleBinLookup(
				final CoordinateRange singleton ) {
			this.singleton = singleton;
		}

		@Override
		public boolean inBounds(
				final Coordinate coordinate ) {
			return inBounds(
					singleton,
					coordinate);
		}

		private static boolean inBounds(
				final CoordinateRange range,
				final Coordinate coordinate ) {
			final long coord = coordinate.getCoordinate();
			return (range.getMinCoordinate() <= coord) && (range.getMaxCoordinate() >= coord);
		}
	}

	private static class MultiBinLookup implements
			RangeByBinIdCache
	{
		private final Map<ByteArrayId, CoordinateRange> binIdToRangeMap;

		public MultiBinLookup(
				final CoordinateRange[] coordinateRanges ) {
			binIdToRangeMap = new HashMap<>();
			for (final CoordinateRange r : coordinateRanges) {
				binIdToRangeMap.put(
						new ByteArrayId(
								r.getBinId()),
						r);
			}
		}

		@Override
		public boolean inBounds(
				final Coordinate coordinate ) {
			final CoordinateRange range = binIdToRangeMap.get(new ByteArrayId(
					coordinate.getBinId()));
			if (range == null) {
				return false;
			}

			return SingleBinLookup.inBounds(
					range,
					coordinate);
		}

	}
}
