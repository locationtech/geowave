package mil.nga.giat.geowave.ingest.mapreduce.gpx;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;

import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;

import com.vividsolutions.jts.geom.Coordinate;

public class GPXMapper extends Mapper<AvroKey<GPXTrack>, NullWritable, ByteArrayId, Object> {

	private SimpleFeatureBuilder pointBuilder;
	private SimpleFeatureBuilder waypointBuilder;
	private SimpleFeatureBuilder trackBuilder;
	private ByteArrayId pointKey;
	private ByteArrayId waypointKey;
	private ByteArrayId trackKey;
	private final static Logger log = Logger.getLogger(GPXMapper.class);
	public final static String TAG_SEPARATOR = " ||| ";

	@Override
	protected void setup( final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);

		pointBuilder = new SimpleFeatureBuilder(GPXJobRunner.createGPXPointDataType());
		waypointBuilder = new SimpleFeatureBuilder(GPXJobRunner.createGPXWaypointDataType());
		trackBuilder = new SimpleFeatureBuilder(GPXJobRunner.createGPXTrackDataType());

		pointKey = new ByteArrayId(StringUtils.stringToBinary(GPXJobRunner.GPX_POINT_FEATURE));
		waypointKey = new ByteArrayId(StringUtils.stringToBinary(GPXJobRunner.GPX_WAYPOINT_FEATURE));
		trackKey = new ByteArrayId(StringUtils.stringToBinary(GPXJobRunner.GPX_TRACK_FEATURE));
	};

	@Override
	protected void map( final AvroKey<GPXTrack> key, final NullWritable value, final Context context )
			throws IOException,
			InterruptedException {

		try {
			processTrack(key.datum(), context);
		} catch (final XMLStreamException e) {
			log.error("Couldn't process file for track: " + key.datum().getTrackid(), e);
		}
	}

	protected void processTrack( final GPXTrack gpxTrack, final Mapper<AvroKey<GPXTrack>, NullWritable, ByteArrayId, Object>.Context context )
			throws XMLStreamException,
			IOException,
			InterruptedException {

		final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLEventReader eventReader = null;
		InputStream in = null;

		Long timestamp = gpxTrack.getTimestamp();
		Double elevation = null;
		Double lat = null;
		Double lon = null;
		Long minTime = gpxTrack.getTimestamp();
		long maxTime = gpxTrack.getTimestamp();
		long trackPoint = 0;
		final List<Coordinate> coordinateSequence = new ArrayList<Coordinate>();
		String name = null;
		String cmt = null;
		String desc = null;
		String sym = null;

		try {
			in = new ByteArrayInputStream(gpxTrack.getGpxfile().array());
			eventReader = inputFactory.createXMLEventReader(in);

			while (eventReader.hasNext()) {
				XMLEvent event = eventReader.nextEvent();
				if (event.isStartElement()) {
					StartElement node = event.asStartElement();
					switch (node.getName().getLocalPart()) {
						case "wpt": {
							node = event.asStartElement();
							final Iterator<Attribute> attributes = node.getAttributes();
							while (attributes.hasNext()) {
								final Attribute a = attributes.next();
								if (a.getName().getLocalPart().equals("lon")) {
									lon = Double.parseDouble(a.getValue());
								} else {
									lat = Double.parseDouble(a.getValue());
								}
							}
							while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals("wpt"))) {
								if (event.isStartElement()) {
									node = event.asStartElement();
									switch (node.getName().getLocalPart()) {
										case "ele": {
											event = eventReader.nextEvent();
											elevation = Double.parseDouble(event.asCharacters().getData());
											break;
										}
										case "name": {
											event = eventReader.nextEvent();
											name = event.asCharacters().getData();
											break;
										}
										case "cmt": {
											event = eventReader.nextEvent();
											cmt = event.asCharacters().getData();
											break;
										}
										case "desc": {
											event = eventReader.nextEvent();
											desc = event.asCharacters().getData();
											break;
										}
										case "sym": {
											event = eventReader.nextEvent();
											sym = event.asCharacters().getData();
											break;
										}

									}
								}
								event = eventReader.nextEvent();
							}
							if ((lon != null) && (lat != null)) {
								final Coordinate p = new Coordinate(lon, lat);
								waypointBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
								waypointBuilder.set("Latitude", lat);
								waypointBuilder.set("Longitude", lon);
								waypointBuilder.set("Elevation", elevation);
								waypointBuilder.set("Name", name);
								waypointBuilder.set("Comment", cmt);
								waypointBuilder.set("Description", desc);
								waypointBuilder.set("Symbol", sym);
								context.write(waypointKey, waypointBuilder.buildFeature(name + gpxTrack.getTrackid() + "_" + lat.hashCode() + "_" + lon.hashCode()));
								trackPoint++;
								lat = null;
								lon = null;
								elevation = null;
								name = null;
								cmt = null;
								desc = null;
								sym = null;
							}
							break;

						}
						case "trkpt": {
							node = event.asStartElement();
							final Iterator<Attribute> attributes = node.getAttributes();
							while (attributes.hasNext()) {
								final Attribute a = attributes.next();
								if (a.getName().getLocalPart().equals("lon")) {
									lon = Double.parseDouble(a.getValue());
								} else {
									lat = Double.parseDouble(a.getValue());
								}
							}
							while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals("trkpt"))) {
								if (event.isStartElement()) {
									node = event.asStartElement();
									switch (node.getName().getLocalPart()) {
										case "ele": {
											event = eventReader.nextEvent();
											elevation = Double.parseDouble(event.asCharacters().getData());
											break;
										}
										case "time": {
											event = eventReader.nextEvent();
											try {
												timestamp = StageGPXToHDFS.TIME_FORMAT_SEC.parse(event.asCharacters().getData()).getTime();

											} catch (final Exception t) {
												try {
													timestamp = StageGPXToHDFS.TIME_FORMAT_MILISEC.parse(event.asCharacters().getData()).getTime();
												} catch (final Exception t2) {

												}
											}

											if (timestamp != null) {
												if (timestamp < minTime) {
													minTime = timestamp;
												}
												if (timestamp > maxTime) {
													maxTime = timestamp;
												}
											}
											break;
										}
									}
								}
								event = eventReader.nextEvent();
							}

							if ((lon != null) && (lat != null)) {
								final Coordinate p = new Coordinate(lon, lat);
								coordinateSequence.add(p);
								pointBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
								pointBuilder.set("Latitude", lat);
								pointBuilder.set("Longitude", lon);
								pointBuilder.set("Elevation", elevation);
								pointBuilder.set("Timestamp", new Date(timestamp));

								context.write(pointKey, pointBuilder.buildFeature(gpxTrack.getTrackid().toString() + "_" + trackPoint));

								lat = null;
								lon = null;
								elevation = null;
								timestamp = null;
							}
							trackPoint++;
							break;
						}

					}
				}
			}
		} catch (final Exception e) {
			log.error(e);
		} finally {
			try {
				eventReader.close();
			} catch (final Exception e2) {}
			IOUtils.closeQuietly(in);
		}

		try {
			trackBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinateSequence.toArray(new Coordinate[coordinateSequence.size()])));
			trackBuilder.set("StartTimeStamp", new Date(minTime));
			trackBuilder.set("EndTimeStamp", new Date(maxTime));
			trackBuilder.set("Duration", maxTime - minTime);
			trackBuilder.set("NumberPoints", trackPoint);
			trackBuilder.set("TrackId", gpxTrack.getTrackid().toString());
			trackBuilder.set("UserId", gpxTrack.getUserid());
			trackBuilder.set("User", gpxTrack.getUser());
			trackBuilder.set("Description", gpxTrack.getDescription());

			if ((gpxTrack.getTags() != null) && (gpxTrack.getTags().size() > 0)) {
				final String tags = org.apache.commons.lang.StringUtils.join(gpxTrack.getTags(), TAG_SEPARATOR);
				trackBuilder.set("Tags", tags);
			} else {
				trackBuilder.set("Tags", null);
			}

			context.write(trackKey, trackBuilder.buildFeature(gpxTrack.getTrackid().toString()));
		} catch (final IllegalArgumentException e) {
			log.warn("Track: " + gpxTrack.getTrackid() + " only had 1 point");

		}

	}

}
