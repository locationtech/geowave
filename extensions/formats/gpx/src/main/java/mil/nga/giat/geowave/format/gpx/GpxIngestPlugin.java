package mil.nga.giat.geowave.format.gpx;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.stream.XMLStreamException;

import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.xml.sax.SAXException;

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
public class GpxIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<GpxTrack>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GpxIngestPlugin.class);

	private final static String TAG_SEPARATOR = " ||| ";

	private Map<Long, GpxTrack> metadata = null;
	private static final AtomicLong currentFreeTrackId = new AtomicLong(
			0);

	public GpxIngestPlugin() {}

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
						TimeUnit.MILLISECONDS.toSeconds(time)
								- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
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
		if (file.getName().toLowerCase(
				Locale.ENGLISH).endsWith(
				"gpx")) {
			return true;
		}
		if ("metadata.xml".equals(file.getName())) {
			return false;
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
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.pointType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.waypointType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.trackType),
			SimpleFeatureUserDataConfigurationSet.configureType(GPXConsumer.routeType)
		};
	}

	@Override
	public Schema getAvroSchema() {
		return GpxTrack.getClassSchema();
	}

	@Override
	public GpxTrack[] toAvroObjects(
			final File input ) {
		GpxTrack track = null;
		if (metadata != null) {
			try {
				final long id = Long.parseLong(FilenameUtils.removeExtension(input.getName()));
				track = metadata.remove(id);
			}
			catch (final NumberFormatException e) {
				LOGGER.info("OSM metadata found, but track file name is not a numeric ID");
			}
		}
		if (track == null) {
			track = new GpxTrack();
			track.setTrackid(currentFreeTrackId.getAndIncrement());
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

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final GpxTrack gpxTrack,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final InputStream in = new ByteArrayInputStream(
				gpxTrack.getGpxfile().array());
		// LOGGER.debug("Processing track [" + gpxTrack.getTimestamp() + "]");
		try {
			return new GPXConsumer(
					in,
					primaryIndexIds,
					gpxTrack.getTrackid() == null ? "" : gpxTrack.getTrackid().toString(),
					getAdditionalData(gpxTrack),
					false, // waypoints, even dups, are unique, due to QGis
							// behavior
					globalVisibility);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to convert GpxTrack to GeoWaveData",
					e);
			return null;
		}
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	private Map<String, Map<String, String>> getAdditionalData(
			final GpxTrack gpxTrack ) {
		final Map<String, Map<String, String>> pathDataSet = new HashMap<String, Map<String, String>>();
		final Map<String, String> dataSet = new HashMap<String, String>();
		pathDataSet.put(
				"gpx.trk",
				dataSet);

		if (gpxTrack.getTrackid() != null) {
			dataSet.put(
					"TrackId",
					gpxTrack.getTrackid().toString());
		}
		if (gpxTrack.getUserid() != null) {
			dataSet.put(
					"UserId",
					gpxTrack.getUserid().toString());
		}
		if (gpxTrack.getUser() != null) {
			dataSet.put(
					"User",
					gpxTrack.getUser().toString());
		}
		if (gpxTrack.getDescription() != null) {
			dataSet.put(
					"Description",
					gpxTrack.getDescription().toString());
		}

		if ((gpxTrack.getTags() != null) && (gpxTrack.getTags().size() > 0)) {
			final String tags = org.apache.commons.lang.StringUtils.join(
					gpxTrack.getTags(),
					TAG_SEPARATOR);
			dataSet.put(
					"Tags",
					tags);
		}
		else {
			dataSet.put(
					"Tags",
					null);
		}
		return pathDataSet;
	}

	public static class IngestGpxTrackFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<GpxTrack>
	{
		public IngestGpxTrackFromHdfs() {
			this(
					new GpxIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestGpxTrackFromHdfs(
				final GpxIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public IngestPluginBase<GpxTrack, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

}
