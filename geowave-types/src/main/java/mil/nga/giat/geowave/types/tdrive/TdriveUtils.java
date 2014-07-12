package mil.nga.giat.geowave.types.tdrive;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.mortbay.log.Log;
import org.opengis.feature.simple.SimpleFeatureType;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This is a convenience class for performing common GPX static utility methods
 * such as schema validation, file parsing, and SimpleFeatureType definition.
 */
public class TdriveUtils
{
	private final static Logger LOGGER = Logger.getLogger(TdriveUtils.class);
	
	public static final DateFormat TIME_FORMAT_MILLIS = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	public static final DateFormat TIME_FORMAT_SECONDS = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");
	public static String TDRIVE_POINT_FEATURE = "tdrivepoint";



	public static SimpleFeatureType createTdrivePointDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(TDRIVE_POINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"taxiid"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"pointinstance"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"Timestamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Lattitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Longitude"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	

	public static boolean validate(final File file ) {
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
			String line = scanner.nextLine();
			return line.split(",").length == 4; 
		} catch (FileNotFoundException e) {
			Log.warn("Error validating file: " + file.getName(),e);
			return false;
		} finally {
			IOUtils.closeQuietly(scanner);
		}
	}
}
