package mil.nga.giat.geowave.format.gdelt;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

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
	public static final int GDELT_LATITUDE_COLUMN_ID = 53;
	
	public static final String GDELT_LONGITUDE_ATTRIBUTE = "Longitude";
	public static final int GDELT_LONGITUDE_COLUMN_ID = 54;

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
								Geometry.class)
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

	public static boolean validate(
			final File file ) {
		return true;
	}
}
