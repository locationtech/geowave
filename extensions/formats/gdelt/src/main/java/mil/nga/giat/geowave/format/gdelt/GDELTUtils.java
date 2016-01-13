package mil.nga.giat.geowave.format.gdelt;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.utils.GeometryUtils;

/**
 * This is a convenience class for performing common GDELT static utility
 * methods such as schema validation, file parsing, and SimpleFeatureType
 * definition.
 */
public class GDELTUtils
{

	private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(
					"yyyyMMdd");
		}
	};

	public static Date parseDate(
			final String source )
					throws ParseException {
		return dateFormat.get().parse(
				source);
	}

	public static final String GDELT_EVENT_FEATURE = "gdeltevent";

	public static final String GDELT_GEOMETRY_ATTRIBUTE = "geometry";

	public static final String GDELT_EVENT_ID_ATTRIBUTE = "eventid";
	public static final int GDELT_EVENT_ID_COLUMN_ID = 0;

	public static final String GDELT_TIMESTAMP_ATTRIBUTE = "Timestamp";
	public static final int GDELT_TIMESTAMP_COLUMN_ID = 1;

	public static final String GDELT_LATITUDE_ATTRIBUTE = "Latitude";
	public static final String GDELT_LONGITUDE_ATTRIBUTE = "Longitude";

	private static final int GDELT_ACTION_LATITUDE_COLUMN_ID = 53;
	private static final int GDELT_ACTION_LONGITUDE_COLUMN_ID = 54;

	private static final int GDELT_ACTOR1_LATITUDE_COLUMN_ID = 39;
	private static final int GDELT_ACTOR1_LONGITUDE_COLUMN_ID = 40;

	private static final int GDELT_ACTOR2_LATITUDE_COLUMN_ID = 46;
	private static final int GDELT_ACTOR2_LONGITUDE_COLUMN_ID = 47;

	public static final int GDELT_ACTION_GEO_TYPE_COLUMN_ID = 49;

	public static final int GDELT_MIN_COLUMNS = 57;
	public static final int GDELT_MAX_COLUMNS = 58;

	public static SimpleFeatureType createGDELTEventDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(
				GDELT_EVENT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(
				attributeTypeBuilder
						.binding(
								Point.class)
						.nillable(
								false)
						.buildDescriptor(
								GDELT_GEOMETRY_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(
				attributeTypeBuilder
						.binding(
								Integer.class)
						.nillable(
								false)
						.buildDescriptor(
								GDELT_EVENT_ID_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(
				attributeTypeBuilder
						.binding(
								Date.class)
						.nillable(
								true)
						.buildDescriptor(
								GDELT_TIMESTAMP_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(
				attributeTypeBuilder
						.binding(
								Double.class)
						.nillable(
								true)
						.buildDescriptor(
								GDELT_LATITUDE_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(
				attributeTypeBuilder
						.binding(
								Double.class)
						.nillable(
								true)
						.buildDescriptor(
								GDELT_LONGITUDE_ATTRIBUTE));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static Pair<Double, Double> parseLatLon(
			final String[] vals,
			final CoordinateReferenceSystem crs ) {

		String latString = vals[GDELTUtils.GDELT_ACTION_LATITUDE_COLUMN_ID];
		String lonString = vals[GDELTUtils.GDELT_ACTION_LONGITUDE_COLUMN_ID];

		if ((latString == null) || (lonString == null) || (latString.length() < 1) || (lonString.length() < 1)) {
			latString = vals[GDELTUtils.GDELT_ACTOR1_LATITUDE_COLUMN_ID];
			lonString = vals[GDELTUtils.GDELT_ACTOR1_LONGITUDE_COLUMN_ID];
		}

		if ((latString == null) || (lonString == null) || (latString.length() < 1) || (lonString.length() < 1)) {
			latString = vals[GDELTUtils.GDELT_ACTOR2_LATITUDE_COLUMN_ID];
			lonString = vals[GDELTUtils.GDELT_ACTOR2_LONGITUDE_COLUMN_ID];
		}

		final Double lat = GeometryUtils.adjustCoordinateDimensionToRange(
				Double.parseDouble(
						latString),
				crs,
				1);
		final Double lon = GeometryUtils.adjustCoordinateDimensionToRange(
				Double.parseDouble(
						lonString),
				crs,
				0);

		return Pair.of(
				lat,
				lon);

	}

	public static boolean validate(
			final File file ) {
		return true;
	}
}
