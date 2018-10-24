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
package org.locationtech.geowave.core.geotime.util;

import java.awt.geom.Point2D;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.measure.unit.BaseUnit;
import javax.measure.unit.DerivedUnit;
import javax.measure.unit.NonSI;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.factory.GeoTools;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.util.ClasspathUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.uzaygezen.core.BitSetMath;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * This class contains a set of Geometry utility methods that are generally
 * useful throughout the GeoWave core codebase
 */
public class GeometryUtils
{
	public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
	private final static Logger LOGGER = LoggerFactory.getLogger(GeometryUtils.class);
	private static final Object MUTEX = new Object();
	private static final Object MUTEX_DEFAULT_CRS = new Object();
	private static final int DEFAULT_DIMENSIONALITY = 2;
	public static final String DEFAULT_CRS_STR = "EPSG:4326";
	private static CoordinateReferenceSystem defaultCrsSingleton;
	private static Set<ClassLoader> initializedClassLoaders = new HashSet<>();

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings()
	public static CoordinateReferenceSystem getDefaultCRS() {
		if (defaultCrsSingleton == null) { // avoid sync penalty if we can
			synchronized (MUTEX_DEFAULT_CRS) {
				// have to do this inside the sync to avoid double init
				if (defaultCrsSingleton == null) {
					try {
						initClassLoader();
						defaultCrsSingleton = CRS.decode(
								DEFAULT_CRS_STR,
								true);
					}
					catch (final Exception e) {
						LOGGER.error(
								"Unable to decode " + DEFAULT_CRS_STR + " CRS",
								e);
						defaultCrsSingleton = DefaultGeographicCRS.WGS84;
					}
				}
			}
		}
		return defaultCrsSingleton;
	}

	public static void initClassLoader()
			throws MalformedURLException {
		synchronized (MUTEX) {
			final ClassLoader myCl = GeometryUtils.class.getClassLoader();
			if (initializedClassLoaders.contains(myCl)) {
				return;
			}
			final ClassLoader classLoader = ClasspathUtils.transformClassLoader(myCl);
			if (classLoader != null) {
				GeoTools.addClassLoader(classLoader);
			}
			initializedClassLoaders.add(myCl);
		}
	}

	public static Constraints basicConstraintsFromGeometry(
			final Geometry geometry ) {

		final List<ConstraintSet> set = new LinkedList<>();
		constructListOfConstraintSetsFromGeometry(
				geometry,
				set,
				false);

		return new Constraints(
				set);
	}

	/**
	 * This utility method will convert a JTS geometry to contraints that can be
	 * used in a GeoWave query.
	 *
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static GeoConstraintsWrapper basicGeoConstraintsWrapperFromGeometry(
			final Geometry geometry ) {

		final List<ConstraintSet> set = new LinkedList<>();
		final boolean geometryConstraintsExactMatch = constructListOfConstraintSetsFromGeometry(
				geometry,
				set,
				true);

		return new GeoConstraintsWrapper(
				new Constraints(
						set),
				geometryConstraintsExactMatch,
				geometry);
	}

	/**
	 * Recursively decompose geometry into a set of envelopes to create a single
	 * set.
	 *
	 * @param geometry
	 * @param destinationListOfSets
	 * @param checkTopoEquality
	 */
	private static boolean constructListOfConstraintSetsFromGeometry(
			final Geometry geometry,
			final List<ConstraintSet> destinationListOfSets,
			final boolean checkTopoEquality ) {

		// Get the envelope of the geometry being held
		final int n = geometry.getNumGeometries();
		boolean retVal = true;
		if (n > 1) {
			retVal = false;
			for (int gi = 0; gi < n; gi++) {
				constructListOfConstraintSetsFromGeometry(
						geometry.getGeometryN(gi),
						destinationListOfSets,
						checkTopoEquality);
			}
		}
		else {
			final Envelope env = geometry.getEnvelopeInternal();
			destinationListOfSets.add(basicConstraintSetFromEnvelope(env));
			if (checkTopoEquality) {
				retVal = new GeometryFactory().toGeometry(
						env).equalsTopo(
						geometry);
			}
		}
		return retVal;
	}

	/**
	 * This utility method will convert a JTS envelope to contraints that can be
	 * used in a GeoWave query.
	 *
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static ConstraintSet basicConstraintSetFromEnvelope(
			final Envelope env ) {
		// Create a NumericRange object using the x axis
		final NumericRange rangeLongitude = new NumericRange(
				env.getMinX(),
				env.getMaxX());

		// Create a NumericRange object using the y axis
		final NumericRange rangeLatitude = new NumericRange(
				env.getMinY(),
				env.getMaxY());

		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<>();
		// Create and return a new IndexRange array with an x and y axis
		// range

		final ConstraintData xRange = new ConstraintData(
				rangeLongitude,
				false);
		final ConstraintData yRange = new ConstraintData(
				rangeLatitude,
				false);
		constraintsPerDimension.put(
				CustomCRSUnboundedSpatialDimensionX.class,
				xRange);
		constraintsPerDimension.put(
				CustomCRSUnboundedSpatialDimensionY.class,
				yRange);
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				xRange);
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				yRange);

		return new ConstraintSet(
				constraintsPerDimension);
	}

	/**
	 * This utility method will convert a JTS envelope to contraints that can be
	 * used in a GeoWave query.
	 *
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static Constraints basicConstraintsFromEnvelope(
			final Envelope env ) {

		return new Constraints(
				basicConstraintSetFromEnvelope(env));
	}

	/**
	 * This utility method will convert a JTS envelope to that can be used in a
	 * GeoWave query.
	 *
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static ConstraintSet basicConstraintsFromPoint(
			final double latitudeDegrees,
			final double longitudeDegrees ) {
		// Create a NumericData object using the x axis
		final NumericData latitude = new NumericValue(
				latitudeDegrees);

		// Create a NumericData object using the y axis
		final NumericData longitude = new NumericValue(
				longitudeDegrees);

		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				new ConstraintData(
						longitude,
						false));
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				new ConstraintData(
						latitude,
						false));
		return new ConstraintSet(
				constraintsPerDimension);
	}

	public static MultiDimensionalNumericData getBoundsFromEnvelope(
			final Envelope envelope ) {
		final NumericRange[] boundsPerDimension = new NumericRange[2];
		boundsPerDimension[0] = new NumericRange(
				envelope.getMinX(),
				envelope.getMaxX());
		boundsPerDimension[1] = new NumericRange(
				envelope.getMinY(),
				envelope.getMaxY());
		return new BasicNumericDataset(
				boundsPerDimension);
	}

	/**
	 * Generate a longitude range from a JTS geometry
	 *
	 * @param geometry
	 *            The JTS geometry
	 * @return The x range
	 */
	public static NumericData xRangeFromGeometry(
			final Geometry geometry ) {
		if ((geometry == null) || geometry.isEmpty()) {
			return new NumericRange(
					0,
					0);
		}
		// Get the envelope of the geometry being held
		final Envelope env = geometry.getEnvelopeInternal();

		// Create a NumericRange object using the x axis
		return new NumericRange(
				env.getMinX(),
				env.getMaxX());
	}

	/**
	 * Generate a latitude range from a JTS geometry
	 *
	 * @param geometry
	 *            The JTS geometry
	 * @return The y range
	 */
	public static NumericData yRangeFromGeometry(
			final Geometry geometry ) {
		if ((geometry == null) || geometry.isEmpty()) {
			return new NumericRange(
					0,
					0);
		}
		// Get the envelope of the geometry being held
		final Envelope env = geometry.getEnvelopeInternal();

		// Create a NumericRange object using the y axis
		return new NumericRange(
				env.getMinY(),
				env.getMaxY());
	}

	/**
	 * Converts a JTS geometry to binary using JTS a Well Known Binary writer
	 *
	 * @param geometry
	 *            The JTS geometry
	 * @return The binary representation of the geometry
	 */
	public static byte[] geometryToBinary(
			final Geometry geometry ) {

		int dimensions = DEFAULT_DIMENSIONALITY;

		if (!geometry.isEmpty()) {
			dimensions = Double.isNaN(geometry.getCoordinate().getOrdinate(
					Coordinate.Z)) ? 2 : 3;
		}

		return new WKBWriter(
				dimensions).write(geometry);
	}

	/**
	 * Converts a byte array as well-known binary to a JTS geometry
	 *
	 * @param binary
	 *            The well known binary
	 * @return The JTS geometry
	 */
	public static Geometry geometryFromBinary(
			final byte[] binary ) {
		try {
			return new WKBReader().read(binary);
		}
		catch (final ParseException e) {
			LOGGER.warn(
					"Unable to deserialize geometry data",
					e);
		}
		return null;
	}

	/**
	 * This mehtod returns an envelope between negative infinite and positive
	 * inifinity in both x and y
	 *
	 * @return the infinite bounding box
	 */
	public static Geometry infinity() {
		// unless we make this synchronized, we will want to instantiate a new
		// geometry factory because geometry factories are not thread safe
		return new GeometryFactory().toGeometry(new Envelope(
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY,
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY));
	}

	public static class GeoConstraintsWrapper
	{
		private final Constraints constraints;
		private final boolean constraintsMatchGeometry;
		private final Geometry jtsBounds;

		public GeoConstraintsWrapper(
				final Constraints constraints,
				final boolean constraintsMatchGeometry,
				final Geometry jtsBounds ) {
			this.constraints = constraints;
			this.constraintsMatchGeometry = constraintsMatchGeometry;
			this.jtsBounds = jtsBounds;
		}

		public Constraints getConstraints() {
			return constraints;
		}

		public boolean isConstraintsMatchGeometry() {
			return constraintsMatchGeometry;
		}

		public Geometry getGeometry() {
			return jtsBounds;
		}
	}

	public static CoordinateReferenceSystem getIndexCrs(
			final Index[] indices ) {

		CoordinateReferenceSystem indexCrs = null;

		for (final Index primaryindx : indices) {

			// for first iteration
			if (indexCrs == null) {
				indexCrs = getIndexCrs(primaryindx);
			}
			else {
				if (primaryindx.getIndexModel() instanceof CustomCrsIndexModel) {
					// check if indexes have different CRS
					if (!indexCrs.equals(((CustomCrsIndexModel) primaryindx.getIndexModel()).getCrs())) {
						LOGGER.error("Multiple indices with different CRS is not supported");
						throw new RuntimeException(
								"Multiple indices with different CRS is not supported");
					}
					else {
						if (!indexCrs.equals(getDefaultCRS())) {
							LOGGER.error("Multiple indices with different CRS is not supported");
							throw new RuntimeException(
									"Multiple indices with different CRS is not supported");
						}

					}
				}
			}
		}

		return indexCrs;
	}

	public static CoordinateReferenceSystem getIndexCrs(
			final Index index ) {

		CoordinateReferenceSystem indexCrs = null;

		if (index.getIndexModel() instanceof CustomCrsIndexModel) {
			indexCrs = ((CustomCrsIndexModel) index.getIndexModel()).getCrs();
		}
		else {
			indexCrs = getDefaultCRS();
		}
		return indexCrs;
	}

	public static String getCrsCode(
			final CoordinateReferenceSystem crs ) {

		return (CRS.toSRS(crs));
	}

	/**
	 * Build a buffer around a geometry
	 *
	 * @param crs
	 * @param geometry
	 * @param distanceUnits
	 * @param distance
	 * @return
	 * @throws TransformException
	 */
	public static final Pair<Geometry, Double> buffer(
			final CoordinateReferenceSystem crs,
			final Geometry geometry,
			final String distanceUnits,
			final double distance )
			throws TransformException {
		Unit<?> unit;
		try {
			unit = lookup(distanceUnits);
		}
		catch (final Exception e) {
			unit = SI.METER;
			LOGGER.warn(
					"Cannot lookup unit of measure " + distanceUnits,
					e);
		}
		final double meterDistance = unit.getConverterTo(
				SI.METER).convert(
				distance);
		final double degrees = distanceToDegrees(
				crs,
				geometry,
				meterDistance);
		// buffer does not respect the CRS; it uses simple cartesian math.
		// nor does buffer handle dateline boundaries
		return Pair.of(
				adjustGeo(
						crs,
						geometry.buffer(degrees)),
				degrees);

	}

	private static Unit lookup(
			final String name ) {
		Unit unit = lookup(
				SI.class,
				name);
		if (unit != null) {
			return unit;
		}

		unit = lookup(
				NonSI.class,
				name);
		if (unit != null) {
			return unit;
		}

		if (name.endsWith("s") || name.endsWith("S")) {
			return lookup(name.substring(
					0,
					name.length() - 1));
		}
		// if we get here, try some aliases
		if (name.equalsIgnoreCase("feet")) {
			return lookup(
					NonSI.class,
					"foot");
		}
		// if we get here, try some aliases
		if (name.equalsIgnoreCase("meters") || name.equalsIgnoreCase("meter")) {
			return lookup(
					SI.class,
					"m");
		}
		if (name.equalsIgnoreCase("unity")) {
			return Unit.ONE;
		}
		return null;
	}

	private static Unit lookup(
			final Class class1,
			final String name ) {
		Unit unit = null;
		final Field[] fields = class1.getDeclaredFields();
		for (int i = 0; i < fields.length; i++) {
			final Field field = fields[i];
			final String name2 = field.getName();
			if ((field.getType().isAssignableFrom(
					BaseUnit.class) || field.getType().isAssignableFrom(
					DerivedUnit.class)) && name2.equalsIgnoreCase(name)) {

				try {
					unit = (Unit) field.get(unit);
					return unit;
				}
				catch (final Exception e) {}
			}
		}
		return unit;
	}

	/**
	 * Consume a geometry that may be over the ranges of the CRS (e.g date-line
	 * crossing). Adjust for crossings with a multi-polygon instance where each
	 * contained polygon represents a portion of the provided geometry longitude
	 * value. Clip hemisphere crossings (fix TBD).
	 *
	 * @param crs
	 * @param geometry
	 * @return
	 */
	public static Geometry adjustGeo(
			final CoordinateReferenceSystem crs,
			final Geometry geometry ) {
		final List<Polygon> polygons = fixRangeOfCoordinates(
				crs,
				geometry);
		if (polygons.size() == 1) {
			return polygons.get(0);
		}
		return geometry.getFactory().createMultiPolygon(
				polygons.toArray(new Polygon[polygons.size()]));
	}

	/**
	 * Adjust geometry so that coordinates fit into long/lat bounds.
	 *
	 * Split date-line crossing polygons.
	 *
	 * For now, clip hemisphere crossing portions of the polygon.
	 *
	 * @param geometry
	 * @return list valid polygons
	 */
	public static List<Polygon> fixRangeOfCoordinates(
			final CoordinateReferenceSystem crs,
			final Geometry geometry ) {

		final List<Polygon> replacements = new ArrayList<>();
		if (geometry instanceof MultiPolygon) {
			final MultiPolygon multi = (MultiPolygon) geometry;
			for (int i = 0; i < multi.getNumGeometries(); i++) {
				final Geometry geo = multi.getGeometryN(i);
				replacements.addAll(fixRangeOfCoordinates(
						crs,
						geo));
			}
			return replacements;
		} // collection is more general than multi-polygon
		else if (geometry instanceof GeometryCollection) {
			final GeometryCollection multi = (GeometryCollection) geometry;
			for (int i = 0; i < multi.getNumGeometries(); i++) {
				final Geometry geo = multi.getGeometryN(i);
				replacements.addAll(fixRangeOfCoordinates(
						crs,
						geo));
			}
			return replacements;
		}

		final Coordinate[] geoCoords = geometry.getCoordinates();
		final Coordinate modifier = findModifier(
				crs,
				geoCoords);
		replacements.addAll(constructGeometriesOverMapRegions(
				modifier,
				geometry));
		return replacements;
	}

	/**
	 *
	 * update modifier for each axis of the coordinate where the modifier's axis
	 * is less extreme than the provides coordinate
	 *
	 * @param modifier
	 * @param cood
	 */
	private static void updateModifier(
			final Coordinate coord,
			final Coordinate modifier ) {
		for (int i = 0; i < 3; i++) {
			if (Math.abs(modifier.getOrdinate(i)) < Math.abs(coord.getOrdinate(i))) {
				modifier.setOrdinate(
						i,
						coord.getOrdinate(i));
			}
		}
	}

	/**
	 * Build a modifier that, when added to the coordinates of a polygon, moves
	 * invalid sections of the polygon to a valid portion of the map.
	 *
	 * @param crs
	 * @param coords
	 * @return
	 */
	private static Coordinate findModifier(
			final CoordinateReferenceSystem crs,
			final Coordinate[] coords ) {
		final Coordinate maxModifier = new Coordinate(
				0,
				0,
				0);
		for (final Coordinate coord : coords) {
			final Coordinate modifier = diff(
					adjustCoordinateToFitInRange(
							crs,
							coord),
					coord);
			updateModifier(
					modifier,
					maxModifier);
		}
		return maxModifier;
	}

	/**
	 * Produce a set of polygons for each region of the map corrected for date
	 * line and hemisphere crossings. Due to the complexity of going around the
	 * hemisphere, clip the range.
	 *
	 * Consider a polygon that cross both the hemisphere in the north and the
	 * date line in the west (-182 92, -182 88, -178 88, -178 92, -182 92). The
	 * result is two polygons: (-180 90, -180 88, -178 88, -178 90, -180 90)
	 * (180 90, 180 88, 178 88, 178 90, 180 90)
	 *
	 * @param modifier
	 * @param geometry
	 *            - a geometry that may cross date line and/or hemispheres.
	 * @return
	 */
	public static List<Polygon> constructGeometriesOverMapRegions(
			final Coordinate modifier,
			final Geometry geometry ) {
		final Coordinate[] geoCoords = geometry.getCoordinates();
		final List<Polygon> polygons = new LinkedList<>();
		final Geometry world = world(
				geometry.getFactory(),
				GeometryUtils.getDefaultCRS());

		// First do the polygon unchanged world
		final Geometry worldIntersections = world.intersection(geometry);
		for (int i = 0; i < worldIntersections.getNumGeometries(); i++) {
			final Polygon polyToAdd = (Polygon) worldIntersections.getGeometryN(i);
			if (!polygons.contains(polyToAdd)) {
				polygons.add(polyToAdd);
			}
		}
		// now use the modifier...but just the x axis for longitude
		// optimization...do not modify if 0
		if (Math.abs(modifier.x) > 0.0000000001) {
			final Coordinate[] newCoords = new Coordinate[geoCoords.length];
			int c = 0;
			for (final Coordinate geoCoord : geoCoords) {
				newCoords[c++] = new Coordinate(
						geoCoord.x + modifier.x,
						geoCoord.y,
						geoCoord.z);
			}
			final Polygon transposedPoly = geometry.getFactory().createPolygon(
					newCoords);

			final Geometry adjustedPolyWorldIntersections = world.intersection(transposedPoly);
			for (int i = 0; i < adjustedPolyWorldIntersections.getNumGeometries(); i++) {
				final Polygon polyToAdd = (Polygon) adjustedPolyWorldIntersections.getGeometryN(i);
				if (!polygons.contains(polyToAdd)) {
					polygons.add(polyToAdd);
				}
			}
		}

		return polygons;

	}

	/**
	 * Make sure the coordinate falls in the range of provided coordinate
	 * reference systems's coordinate system. 'x' coordinate is wrapped around
	 * date line. 'y' and 'z' coordinate are clipped. At some point, this
	 * function will be adjusted to project 'y' appropriately.
	 *
	 * @param crs
	 * @param coord
	 * @return
	 */
	public static Coordinate adjustCoordinateToFitInRange(
			final CoordinateReferenceSystem crs,
			final Coordinate coord ) {
		return new Coordinate(
				adjustCoordinateDimensionToRange(
						coord.x,
						crs,
						0),
				clipRange(
						coord.y,
						crs,
						1),
				clipRange(
						coord.z,
						crs,
						2));
	}

	/**
	 *
	 * @param coord1
	 * @param coord2
	 *            subtracted from coord1
	 * @return a coordinate the supplies the difference of values for each axis
	 *         between coord1 and coord2
	 */
	private static Coordinate diff(
			final Coordinate coord1,
			final Coordinate coord2 ) {
		return new Coordinate(
				coord1.x - coord2.x,
				coord1.y - coord2.y,
				coord1.z - coord2.z);
	}

	/**
	 *
	 * @param val
	 *            the value
	 * @param crs
	 * @param axis
	 *            the coordinate axis
	 * @return
	 */
	private static double clipRange(
			final double val,
			final CoordinateReferenceSystem crs,
			final int axis ) {
		final CoordinateSystem coordinateSystem = crs.getCoordinateSystem();
		if (coordinateSystem.getDimension() > axis) {
			final CoordinateSystemAxis coordinateAxis = coordinateSystem.getAxis(axis);
			if (val < coordinateAxis.getMinimumValue()) {
				return coordinateAxis.getMinimumValue();
			}
			else if (val > coordinateAxis.getMaximumValue()) {
				return coordinateAxis.getMaximumValue();
			}
		}
		return val;
	}

	/**
	 * This is perhaps a brain dead approach to do this, but it does handle wrap
	 * around cases. Also supports cases where the wrap around occurs many
	 * times.
	 *
	 * @param val
	 *            the value
	 * @param crs
	 * @param axis
	 *            the coordinate axis
	 * @return
	 */
	public static double adjustCoordinateDimensionToRange(
			final double val,
			final CoordinateReferenceSystem crs,
			final int axis ) {
		final CoordinateSystem coordinateSystem = crs.getCoordinateSystem();
		if (coordinateSystem.getDimension() > axis) {
			final double lowerBound = coordinateSystem.getAxis(
					axis).getMinimumValue();
			final double bound = coordinateSystem.getAxis(
					axis).getMaximumValue() - lowerBound;
			final double sign = sign(val);
			// re-scale to 0 to n, then determine how many times to 'loop
			// around'
			final double mult = Math.floor(Math.abs((val + (sign * (-1.0 * lowerBound))) / bound));
			return val + (mult * bound * sign * (-1.0));
		}
		return val;
	}

	private static double sign(
			final double val ) {
		return val < 0 ? -1 : 1;
	}

	/**
	 * Return a multi-polygon representing the bounded map regions split by the
	 * axis
	 *
	 * @param factory
	 * @param crs
	 * @return
	 */
	public static Geometry world(
			final GeometryFactory factory,
			final CoordinateReferenceSystem crs ) {
		return factory.createPolygon(toPolygonCoordinates(crs.getCoordinateSystem()));
	}

	private static Coordinate[] toPolygonCoordinates(
			final CoordinateSystem coordinateSystem ) {
		final Coordinate[] coordinates = new Coordinate[(int) Math.pow(
				2,
				coordinateSystem.getDimension()) + 1];
		final BitSet greyCode = new BitSet(
				coordinateSystem.getDimension());
		final BitSet mask = getGreyCodeMask(coordinateSystem.getDimension());
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] = new Coordinate(
					getValue(
							greyCode,
							coordinateSystem.getAxis(0),
							0),
					getValue(
							greyCode,
							coordinateSystem.getAxis(1),
							1),
					coordinateSystem.getDimension() > 2 ? getValue(
							greyCode,
							coordinateSystem.getAxis(2),
							2) : Double.NaN);

			grayCode(
					greyCode,
					mask);
		}
		return coordinates;
	}

	private static BitSet getGreyCodeMask(
			final int dims ) {
		final BitSet mask = new BitSet(
				dims);
		for (int i = 0; i < dims; i++) {
			mask.set(i);
		}
		return mask;
	}

	private static void grayCode(
			final BitSet code,
			final BitSet mask ) {
		BitSetMath.grayCodeInverse(code);
		BitSetMath.increment(code);
		code.and(mask);
		BitSetMath.grayCode(code);
	}

	private static double getValue(
			final BitSet set,
			final CoordinateSystemAxis axis,
			final int dimension ) {
		return (set.get(dimension)) ? axis.getMaximumValue() : axis.getMinimumValue();
	}

	/**
	 * Convert meters to decimal degrees based on widest point
	 *
	 * @throws TransformException
	 */
	private static double distanceToDegrees(
			final CoordinateReferenceSystem crs,
			final Geometry geometry,
			final double meters )
			throws TransformException {
		final GeometryFactory factory = geometry.getFactory();
		return (geometry instanceof Point) ? geometry.distance(farthestPoint(
				crs,
				(Point) geometry,
				meters)) : distanceToDegrees(
				crs,
				geometry.getEnvelopeInternal(),
				factory == null ? new GeometryFactory() : factory,
				meters);
	}

	private static double distanceToDegrees(
			final CoordinateReferenceSystem crs,
			final Envelope env,
			final GeometryFactory factory,
			final double meters )
			throws TransformException {
		return Collections.max(Arrays.asList(
				distanceToDegrees(
						crs,
						factory.createPoint(new Coordinate(
								env.getMaxX(),
								env.getMaxY())),
						meters),
				distanceToDegrees(
						crs,
						factory.createPoint(new Coordinate(
								env.getMaxX(),
								env.getMinY())),
						meters),
				distanceToDegrees(
						crs,
						factory.createPoint(new Coordinate(
								env.getMinX(),
								env.getMinY())),
						meters),
				distanceToDegrees(
						crs,
						factory.createPoint(new Coordinate(
								env.getMinX(),
								env.getMaxY())),
						meters)));
	}

	/** farther point in longitudinal axis given a latitude */

	private static Point farthestPoint(
			final CoordinateReferenceSystem crs,
			final Point point,
			final double meters ) {
		final GeodeticCalculator calc = new GeodeticCalculator(
				crs);
		calc.setStartingGeographicPoint(
				point.getX(),
				point.getY());
		calc.setDirection(
				90,
				meters);
		Point2D dest2D = calc.getDestinationGeographicPoint();
		// if this flips over the date line then try the other direction
		if (dest2D.getX() < point.getX()) {
			calc.setDirection(
					-90,
					meters);
			dest2D = calc.getDestinationGeographicPoint();
		}
		return point.getFactory().createPoint(
				new Coordinate(
						dest2D.getX(),
						dest2D.getY()));
	}

	public static SimpleFeature crsTransform(
			final SimpleFeature entry,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		SimpleFeature crsEntry = entry;

		if (transform != null) {
			// we can use the transform we have already calculated for this
			// feature
			try {

				// this will clone the feature and retype it to Index CRS
				crsEntry = SimpleFeatureBuilder.retype(
						entry,
						reprojectedType);

				// this will transform the geometry
				crsEntry.setDefaultGeometry(JTS.transform(
						(Geometry) entry.getDefaultGeometry(),
						transform));
			}
			catch (MismatchedDimensionException | TransformException e) {
				LOGGER
						.warn(
								"Unable to perform transform to specified CRS of the index, the feature geometry will remain in its original CRS",
								e);
			}
		}

		return crsEntry;
	}

}
