package mil.nga.giat.geowave.types.gpx;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

public class GpxUtils
{
	private final static Logger LOGGER = Logger.getLogger(GpxUtils.class);
	public static final DateFormat TIME_FORMAT_MILLIS = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	public static final DateFormat TIME_FORMAT_SECONDS = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");
	public static String GPX_POINT_FEATURE = "gpxpoint";
	public static String GPX_TRACK_FEATURE = "gpxtrack";
	public static String GPX_WAYPOINT_FEATURE = "gpxwaypoint";

	public static Map<Long, GpxTrack> parseOsmMetadata(
			final File metadataFile )
			throws FileNotFoundException,
			XMLStreamException {
		final Map<Long, GpxTrack> metadata = new HashMap<Long, GpxTrack>();
		final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLEventReader eventReader = null;
		InputStream in = null;
		in = new BufferedInputStream(
				new FileInputStream(
						metadataFile));
		eventReader = inputFactory.createXMLEventReader(in);
		while (eventReader.hasNext()) {
			XMLEvent event = eventReader.nextEvent();
			if (event.isStartElement()) {
				StartElement node = event.asStartElement();
				switch (node.getName().getLocalPart()) {
					case "gpxFile": {
						final GpxTrack gt = new GpxTrack();
						node = event.asStartElement();
						final Iterator<Attribute> attributes = node.getAttributes();
						while (attributes.hasNext()) {
							final Attribute a = attributes.next();
							switch (a.getName().getLocalPart()) {
								case "id": {
									gt.setTrackid(Long.parseLong(a.getValue()));
									break;
								}
								case "timestamp": {
									try {
										gt.setTimestamp(TIME_FORMAT_SECONDS.parse(
												a.getValue()).getTime());

									}
									catch (final Exception t) {
										try {
											gt.setTimestamp(TIME_FORMAT_MILLIS.parse(
													a.getValue()).getTime());
										}
										catch (final Exception t2) {
											LOGGER.warn(
													"Unable to format time: " + a.getValue(),
													t2);
										}
									}
									break;
								}
								case "points": {
									gt.setPoints(Long.parseLong(a.getValue()));
									break;
								}
								case "visibility": {
									gt.setVisibility(a.getValue());
									break;
								}
								case "uid": {
									gt.setUserid(Long.parseLong(a.getValue()));
									break;
								}
								case "user": {
									gt.setUser(a.getValue());
									break;
								}

							}
						}
						while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
								"gpxFile"))) {
							if (event.isStartElement()) {
								node = event.asStartElement();
								switch (node.getName().getLocalPart()) {
									case "description": {
										event = eventReader.nextEvent();
										if (event.isCharacters()) {
											gt.setDescription(event.asCharacters().getData());
										}
										break;
									}
									case "tags": {
										final List<CharSequence> tags = new ArrayList<CharSequence>();
										while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
												"tags"))) {
											if (event.isStartElement()) {
												node = event.asStartElement();
												if (node.getName().getLocalPart().equals(
														"tag")) {
													event = eventReader.nextEvent();
													if (event.isCharacters()) {
														tags.add(event.asCharacters().getData());
													}
												}
											}
											event = eventReader.nextEvent();
										}
										gt.setTags(tags);
										break;
									}

								}
							}
							event = eventReader.nextEvent();
						}
						metadata.put(
								gt.getTrackid(),
								gt);
						break;
					}

				}
			}
		}
		return metadata;
	}

	public static SimpleFeatureType createGPXTrackDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GPX_TRACK_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				true).buildDescriptor(
				"geometry"));

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"StartTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"EndTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Long.class).nillable(
				true).buildDescriptor(
				"Duration"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Long.class).nillable(
				true).buildDescriptor(
				"NumberPoints"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"TrackId"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Long.class).nillable(
				true).buildDescriptor(
				"UserId"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"User"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Description"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Tags"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static SimpleFeatureType createGPXPointDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GPX_POINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				true).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Latitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Longitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Elevation"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"Timestamp"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static SimpleFeatureType createGPXWaypointDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GPX_WAYPOINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				true).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Latitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Longitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Elevation"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Name"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Comment"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Description"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Symbol"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}
}
