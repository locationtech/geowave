package mil.nga.giat.geowave.types.gpx;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.xml.sax.SAXException;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This plugin is used for ingesting any GPX formatted data from a local file
 * system into GeoWave as GeoTools' SimpleFeatures. It supports the default
 * configuration of spatial and spatial-temporal indices and it will support
 * wither directly ingesting GPX data from a local file system to GeoWave or to
 * stage the data in an intermediate format in HDFS and then to ingest it into
 * GeoWave using a map-reduce job. It supports OSM metadata.xml files if the
 * file is directly in the root base directory that is passed in command-line to
 * the ingest framework.
 */
public class GpxIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<GpxTrack, SimpleFeature>,
		StageToHdfsPlugin<GpxTrack>
{

	private final static Logger LOGGER = Logger.getLogger(GpxIngestPlugin.class);

	private final static String TAG_SEPARATOR = " ||| ";

	private Map<Long, GpxTrack> metadata = null;
	private static long currentFreeTrackId = 0;

	private final SimpleFeatureBuilder pointBuilder;
	private final SimpleFeatureBuilder waypointBuilder;
	private final SimpleFeatureBuilder trackBuilder;
	private final SimpleFeatureType pointType;
	private final SimpleFeatureType waypointType;
	private final SimpleFeatureType trackType;

	private final ByteArrayId pointKey;
	private final ByteArrayId waypointKey;
	private final ByteArrayId trackKey;

	private final Index[] supportedIndices;

	public GpxIngestPlugin() {
		pointType = GpxUtils.createGPXPointDataType();
		waypointType = GpxUtils.createGPXWaypointDataType();
		trackType = GpxUtils.createGPXTrackDataType();

		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_POINT_FEATURE));
		waypointKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_WAYPOINT_FEATURE));
		trackKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_TRACK_FEATURE));
		pointBuilder = new SimpleFeatureBuilder(
				pointType);
		waypointBuilder = new SimpleFeatureBuilder(
				waypointType);
		trackBuilder = new SimpleFeatureBuilder(
				trackType);
		supportedIndices = new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
		};

	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"xml",
			"gpx"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {
		final File f = new File(
				baseDirectory,
				"metadata.xml");
		metadata = null;
		if (!f.exists()) {
			LOGGER.info("No metadata file found - looked at: " + f.getAbsolutePath());
			LOGGER.info("No metadata will be loaded");
		}
		else {
			try {
				long time = System.currentTimeMillis();
				metadata = GpxUtils.parseOsmMetadata(f);
				time = System.currentTimeMillis() - time;
				final String timespan = String.format(
						"%d min, %d sec",
						TimeUnit.MILLISECONDS.toMinutes(time),
						TimeUnit.MILLISECONDS.toSeconds(time) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
				LOGGER.info("Metadata parsed in in " + timespan + " for " + metadata.size() + " tracks");
			}
			catch (final XMLStreamException | FileNotFoundException e) {
				LOGGER.warn(
						"Unable to read OSM metadata file: " + f.getAbsolutePath(),
						e);
			}
		}

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		// if its a gpx extension assume it is supported
		if (file.getName().toLowerCase().endsWith(
				"gpx")) {
			return true;
		}
		// otherwise take a quick peek at the file to ensure it matches the GPX
		// schema
		try {
			return GpxUtils.validateGpx(file);
		}
		catch (SAXException | IOException e) {
			LOGGER.warn(
					"Unable to read file:" + file.getAbsolutePath(),
					e);
		}
		return false;
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility.isEmpty()) ? new GlobalVisibilityHandler<SimpleFeature, Object>(
				globalVisibility) : null;
		return new WritableDataAdapter[] {
			new FeatureDataAdapter(
					pointType,
					fieldVisiblityHandler),
			new FeatureDataAdapter(
					waypointType,
					fieldVisiblityHandler),
			new FeatureDataAdapter(
					trackType,
					fieldVisiblityHandler)
		};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final GpxTrack[] gpxTracks = toHdfsObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final GpxTrack track : gpxTracks) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					track,
					primaryIndexId,
					globalVisibility);
			allData.add(geowaveData);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
	}

	@Override
	public Schema getAvroSchemaForHdfsType() {
		return GpxTrack.getClassSchema();
	}

	@Override
	public GpxTrack[] toHdfsObjects(
			final File input ) {
		GpxTrack track = null;
		if (metadata != null) {
			try {
				final long id = Long.parseLong(FilenameUtils.removeExtension(input.getName()));
				track = metadata.remove(id);
			}
			catch (final NumberFormatException e) {
				LOGGER.info(
						"OSM metadata found, but track file name is not a numeric ID",
						e);
			}
		}
		if (track == null) {
			track = new GpxTrack();
			track.setTrackid(currentFreeTrackId++);
		}

		try {
			track.setGpxfile(ByteBuffer.wrap(Files.readAllBytes(input.toPath())));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GPX file: " + input.getAbsolutePath(),
					e);
		}

		return new GpxTrack[] {
			track
		};
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<GpxTrack, SimpleFeature> ingestWithMapper() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<GpxTrack, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	private CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final GpxTrack gpxTrack,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLEventReader eventReader = null;
		InputStream in = null;

		Long timestamp = gpxTrack.getTimestamp();
		Double elevation = null;
		Double lat = null;
		Double lon = null;
		Long minTime = gpxTrack.getTimestamp();
		Long maxTime = gpxTrack.getTimestamp();
		long trackPoint = 0;
		long wayPoint = 0;
		final List<Coordinate> coordinateSequence = new ArrayList<Coordinate>();
		String name = null;
		String cmt = null;
		String desc = null;
		String sym = null;
		final List<GeoWaveData<SimpleFeature>> featureData = new ArrayList<GeoWaveData<SimpleFeature>>();
		try {
			in = new ByteArrayInputStream(
					gpxTrack.getGpxfile().array());
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
								if (a.getName().getLocalPart().equals(
										"lon")) {
									lon = Double.parseDouble(a.getValue());
								}
								else {
									lat = Double.parseDouble(a.getValue());
								}
							}
							while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
									"wpt"))) {
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
								final Coordinate p = new Coordinate(
										lon,
										lat);
								waypointBuilder.set(
										"geometry",
										GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
								waypointBuilder.set(
										"Latitude",
										lat);
								waypointBuilder.set(
										"Longitude",
										lon);
								waypointBuilder.set(
										"Elevation",
										elevation);
								waypointBuilder.set(
										"Name",
										name);
								waypointBuilder.set(
										"Comment",
										cmt);
								waypointBuilder.set(
										"Description",
										desc);
								waypointBuilder.set(
										"Symbol",
										sym);
								featureData.add(new GeoWaveData<SimpleFeature>(
										waypointKey,
										primaryIndexId,
										waypointBuilder.buildFeature(name + "_" + gpxTrack.getTrackid() + "_" + wayPoint)));
								wayPoint++;
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
								if (a.getName().getLocalPart().equals(
										"lon")) {
									lon = Double.parseDouble(a.getValue());
								}
								else {
									lat = Double.parseDouble(a.getValue());
								}
							}
							while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
									"trkpt"))) {
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
												timestamp = GpxUtils.TIME_FORMAT_SECONDS.parse(
														event.asCharacters().getData()).getTime();

											}
											catch (final Exception t) {
												try {
													timestamp = GpxUtils.TIME_FORMAT_MILLIS.parse(
															event.asCharacters().getData()).getTime();
												}
												catch (final Exception t2) {

												}
											}

											if (timestamp != null) {
												if ((minTime == null) || (timestamp < minTime)) {
													minTime = timestamp;
												}
												if ((maxTime == null) || (timestamp > maxTime)) {
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
								final Coordinate p = new Coordinate(
										lon,
										lat);
								coordinateSequence.add(p);
								pointBuilder.set(
										"geometry",
										GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
								pointBuilder.set(
										"Latitude",
										lat);
								pointBuilder.set(
										"Longitude",
										lon);
								pointBuilder.set(
										"Elevation",
										elevation);
								if (timestamp != null) {
									pointBuilder.set(
											"Timestamp",
											new Date(
													timestamp));
								}
								else {
									pointBuilder.set(
											"Timestamp",
											null);
								}

								featureData.add(new GeoWaveData<SimpleFeature>(
										pointKey,
										primaryIndexId,
										pointBuilder.buildFeature(gpxTrack.getTrackid().toString() + "_" + trackPoint)));

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
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error reading track XML stream",
					e);
		}
		finally {
			try {
				eventReader.close();
			}
			catch (final Exception e2) {
				LOGGER.warn(
						"Unable to close track XML stream",
						e2);
			}
			IOUtils.closeQuietly(in);
		}
		if (!coordinateSequence.isEmpty()) {
			try {
				trackBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinateSequence.toArray(new Coordinate[coordinateSequence.size()])));
				boolean setDuration = true;
				if (minTime != null) {
					trackBuilder.set(
							"StartTimeStamp",
							new Date(
									minTime));
				}
				else {
					setDuration = false;

					trackBuilder.set(
							"StartTimeStamp",
							null);
				}
				if (maxTime != null) {
					trackBuilder.set(
							"EndTimeStamp",
							new Date(
									maxTime));
				}
				else {
					setDuration = false;

					trackBuilder.set(
							"EndTimeStamp",
							null);
				}
				if (setDuration) {
					trackBuilder.set(
							"Duration",
							maxTime - minTime);
				}
				else {
					trackBuilder.set(
							"Duration",
							null);
				}

				trackBuilder.set(
						"NumberPoints",
						trackPoint);
				trackBuilder.set(
						"TrackId",
						gpxTrack.getTrackid().toString());
				trackBuilder.set(
						"UserId",
						gpxTrack.getUserid());
				trackBuilder.set(
						"User",
						gpxTrack.getUser());
				trackBuilder.set(
						"Description",
						gpxTrack.getDescription());

				if ((gpxTrack.getTags() != null) && (gpxTrack.getTags().size() > 0)) {
					final String tags = org.apache.commons.lang.StringUtils.join(
							gpxTrack.getTags(),
							TAG_SEPARATOR);
					trackBuilder.set(
							"Tags",
							tags);
				}
				else {
					trackBuilder.set(
							"Tags",
							null);
				}

				featureData.add(new GeoWaveData<SimpleFeature>(
						trackKey,
						primaryIndexId,
						trackBuilder.buildFeature(gpxTrack.getTrackid().toString())));
			}
			catch (final IllegalArgumentException e) {
				LOGGER.warn(
						"Track: " + gpxTrack.getTrackid() + " only had 1 point",
						e);

			}
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				featureData.iterator());
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	public static class IngestGpxTrackFromHdfs implements
			IngestWithMapper<GpxTrack, SimpleFeature>
	{
		private final GpxIngestPlugin parentPlugin;

		public IngestGpxTrackFromHdfs() {
			this(
					new GpxIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestGpxTrackFromHdfs(
				final GpxIngestPlugin parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
				final GpxTrack input,
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			return parentPlugin.toGeoWaveDataInternal(
					input,
					primaryIndexId,
					globalVisibility);
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}
}
