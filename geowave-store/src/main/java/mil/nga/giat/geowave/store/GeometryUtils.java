package mil.nga.giat.geowave.store;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.store.query.BasicQuery.Constraints;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
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
	private final static Logger LOGGER = Logger.getLogger(GeometryUtils.class);

	/**
	 * This utility method will convert a JTS geometry to contraints that can be
	 * used in a GeoWave query.
	 * 
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static Constraints basicConstraintsFromGeometry(
			final Geometry geometry ) {

		// Get the envelope of the geometry being held
		final Envelope env = geometry.getEnvelopeInternal();
		return basicConstraintsFromEnvelope(env);
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
		// Create a NumericRange object using the x axis
		final NumericRange rangeLongitude = new NumericRange(
				env.getMinX(),
				env.getMaxX());

		// Create a NumericRange object using the y axis
		final NumericRange rangeLatitude = new NumericRange(
				env.getMinY(),
				env.getMaxY());

		final Map<Class<? extends NumericDimensionDefinition>, NumericData> constraintsPerDimension = new HashMap<Class<? extends NumericDimensionDefinition>, NumericData>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				rangeLongitude);
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				rangeLatitude);
		return new Constraints(
				constraintsPerDimension);
	}

	/**
	 * This utility method will convert a JTS envelope to contraints that can be
	 * used in a GeoWave query.
	 * 
	 * @return Constraints as a mapping of NumericData objects representing
	 *         ranges for a latitude dimension and a longitude dimension
	 */
	public static Constraints basicConstraintsFromPoint(
			final double latitudeDegrees,
			final double longitudeDegrees ) {
		// Create a NumericData object using the x axis
		final NumericData latitude = new NumericValue(
				latitudeDegrees);

		// Create a NumericData object using the y axis
		final NumericData longitude = new NumericValue(
				longitudeDegrees);

		final Map<Class<? extends NumericDimensionDefinition>, NumericData> constraintsPerDimension = new HashMap<Class<? extends NumericDimensionDefinition>, NumericData>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				longitude);
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				latitude);
		return new Constraints(
				constraintsPerDimension);
	}

	/**
	 * Generate a longitude range from a JTS geometry
	 * 
	 * @param geometry
	 *            The JTS geometry
	 * @return The longitude range in EPSG:4326
	 */
	public static NumericData longitudeRangeFromGeometry(
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
	 * @return The latitude range in EPSG:4326
	 */
	public static NumericData latitudeRangeFromGeometry(
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
		return new WKBWriter().write(geometry);
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
}
