package mil.nga.giat.geowave.test.kafka;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.ingest.kafka.IngestFromKafkaDriver;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.format.gpx.GPXConsumer;
import mil.nga.giat.geowave.format.gpx.GpxIngestFormat;
import mil.nga.giat.geowave.format.gpx.GpxTrack;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

public class BasicKafkaIT extends
		KafkaTestBase<GpxTrack>
{
	private final static Logger LOGGER = Logger.getLogger(BasicKafkaIT.class);

	private static final Index INDEX = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private static final GpxIngestFormat gpxIngestFormat = new GpxIngestFormat();
	private static final ArrayList<GpxTrack> gpxTracksList = new ArrayList<GpxTrack>();
	private static final Map<Long, GPXConsumer> trackGeometriesMap = new HashMap<Long, GPXConsumer>();

	@BeforeClass
	static public void setupGpxTracks()
			throws Exception {

		final File gpxInputDir = new File(
				OSM_GPX_INPUT_DIR);

		final GpxIngestPluginUtil gpxIngestPlugin = new GpxIngestPluginUtil();
		gpxIngestPlugin.init(gpxInputDir);

		final String[] extensions = gpxIngestPlugin.getFileExtensionFilters();
		final Collection<File> gpxFiles = FileUtils.listFiles(
				gpxInputDir,
				extensions,
				true);

		for (final File gpxFile : gpxFiles) {
			final GpxTrack[] tracks = gpxIngestPlugin.toAvroObjects(gpxFile);
			for (final GpxTrack track : tracks) {
				GPXConsumer gpxConsumer;
				try {
					gpxConsumer = gpxIngestPlugin.toGPXConsumer(
							track,
							INDEX.getId(),
							null);
				}
				catch (final Exception e) {
					continue;
				}

				trackGeometriesMap.put(
						track.getTrackid(),
						gpxConsumer);

				gpxTracksList.add(track);
			}
		}
	}

	@Test
	public void testBasicIngestGpx()
			throws Exception {

		final ExecutorService es = IngestFromKafkaDriver.getExecutorService();
		es.execute(new Runnable() {

			@Override
			public void run() {
				testKafkaIngest(
						IndexType.SPATIAL_VECTOR,
						OSM_GPX_INPUT_DIR);
			}
		});

		boolean proceedWithTest = true;

		int waitCounter = 0;
		while (!IngestFromKafkaDriver.allPluginsConfiguredAndListening()) {
			if (waitCounter > 30) {
				proceedWithTest = false;
				break;
			}
			Thread.sleep(1000);
			waitCounter++;
		}

		if (!proceedWithTest) {
			Assert.fail("Kafka topic consumers not completely configured, check configuration...");
		}

		Thread.sleep(2000);

		es.execute(new Runnable() {
			@Override
			public void run() {
				final Properties props = new Properties();
				props.put(
						"metadata.broker.list",
						"localhost:9092");
				props.put(
						"serializer.class",
						"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
				final ProducerConfig config = new ProducerConfig(
						props);

				final Producer<String, GpxTrack> producer = new Producer<String, GpxTrack>(
						config);

				try {
					Thread.sleep(1000);
					for (final GpxTrack track : gpxTracksList) {
						final KeyedMessage<String, GpxTrack> gpxKafkaMessage = new KeyedMessage<String, GpxTrack>(
								gpxIngestFormat.getIngestFormatName(),
								track);
						LOGGER.debug("Sending message... [" + track.getTimestamp() + "]");
						producer.send(gpxKafkaMessage);
					}
				}
				catch (final Exception ex) {
					LOGGER.warn(
							"Error sending messages to Kafka topic",
							ex);
				}
				finally {
					producer.close();
				}
			}
		});

		Thread.sleep(3000);

		es.awaitTermination(
				60,
				TimeUnit.SECONDS);

		final List<String> receivedMessages = testQuery();
		assertNotNull(receivedMessages);
		assertTrue(receivedMessages.size() > 0);
	}

	@Test
	public void testBasicStageGpx()
			throws Exception {
		testKafkaStage(OSM_GPX_INPUT_DIR);
	}

	private List<String> testQuery()
			throws Exception {
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);

		final List<String> receivedMessagesList = new ArrayList<String>();

		final Set<Long> trackIds = trackGeometriesMap.keySet();
		final Iterator<Long> trackIterator = trackIds.iterator();
		while (trackIterator.hasNext()) {
			final Long trackId = trackIterator.next();
			final GPXConsumer gpxConsumer = trackGeometriesMap.get(trackId);
			final List<Geometry> expectedTrackPoints = getTrackPoints(gpxConsumer);
			LOGGER.debug("[" + trackId + "] has [" + expectedTrackPoints.size() + "] geometries");

			final GeometryFactory factory = new GeometryFactory();
			final Geometry spatialFilter = factory.buildGeometry(
					expectedTrackPoints).getEnvelope();

			final DistributableQuery query = new SpatialQuery(
					spatialFilter);

			final CloseableIterator<?> accumuloResults = geowaveStore.query(
					INDEX,
					query);

			SimpleFeature accumuloResultSimpleFeature = null;
			while (accumuloResults.hasNext()) {
				final Object obj = accumuloResults.next();
				if ((accumuloResultSimpleFeature == null) && (obj instanceof SimpleFeature)) {
					accumuloResultSimpleFeature = (SimpleFeature) obj;

					final Object accumuloResultTrackIdObj = accumuloResultSimpleFeature.getAttribute("TrackId");
					if (accumuloResultTrackIdObj == null) {
						continue;
					}
					final Long accumuloResultTrackIdLong = Long.parseLong(accumuloResultTrackIdObj.toString());
					// verifying at least some of the messages sent were
					// received and ingested into GeoWave
					if (accumuloResultTrackIdLong.longValue() == trackId.longValue()) {
						final Object defaultGeometry = accumuloResultSimpleFeature.getDefaultGeometry();
						Integer accumuloResultTrackPoints = 0;
						if (defaultGeometry instanceof LineString) {
							final LineString accumuloResultLineString = (LineString) defaultGeometry;
							accumuloResultTrackPoints = accumuloResultLineString.getCoordinates().length;
						}

						LOGGER.debug("[" + accumuloResultTrackIdLong + "] message successfully ingested into Accumulo with [" + accumuloResultTrackPoints + "] geometries");
						receivedMessagesList.add(accumuloResultTrackIdObj.toString());
					}
				}
			}
			accumuloResults.close();
		}

		LOGGER.info("[" + receivedMessagesList + "] Kafka messages [total: " + receivedMessagesList.size() + "] ingested into Accumulo");
		return receivedMessagesList;

	}

	static private List<Geometry> getTrackPoints(
			final GPXConsumer gpxConsumer ) {
		final List<Geometry> trackGeometries = new ArrayList<Geometry>();

		while (gpxConsumer.hasNext()) {
			final Geometry pointGeom = (Geometry) gpxConsumer.next().getValue().getDefaultGeometry();
			trackGeometries.add(pointGeom);
		}

		return trackGeometries;
	}
}
