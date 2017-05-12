package mil.nga.giat.geowave.adapter.vector.utils;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.ows.bindings.UnitBinding;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.operation.TransformException;

import com.google.uzaygezen.core.BitSetMath;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class GeometryUtils
{

	private static Logger LOGGER = LoggerFactory.getLogger(GeometryUtils.class);

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
			unit = (Unit<?>) new UnitBinding().parse(
					null,
					distanceUnits);
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

		final List<Polygon> replacements = new ArrayList<Polygon>();
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
		final List<Polygon> polygons = new LinkedList<Polygon>();
		final Geometry world = GeometryUtils.world(
				geometry.getFactory(),
				GeoWaveGTDataStore.DEFAULT_CRS);

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
			if (val < coordinateAxis.getMinimumValue())
				return coordinateAxis.getMinimumValue();
			else if (val > coordinateAxis.getMaximumValue()) return coordinateAxis.getMaximumValue();
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

}
