package mil.nga.giat.geowave.examples.ingest;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

public class SimpleIngestProducerConsumer extends
		SimpleIngest
{

	private static Logger log = Logger.getLogger(SimpleIngestProducerConsumer.class);
	private final FeatureCollection features = new FeatureCollection();

	public static void main(
			final String[] args ) {
		if (args.length != 5) {
			log.error("Invalid arguments, expected: zookeepers, accumuloInstance, accumuloUser, accumuloPass, geowaveNamespace");
			System.exit(1);
		}

		final SimpleIngestProducerConsumer si = new SimpleIngestProducerConsumer();

		try {
			final BasicAccumuloOperations bao = si.getAccumuloOperationsInstance(
					args[0],
					args[1],
					args[2],
					args[3],
					args[4]);
			si.generateGrid(bao);
		}
		catch (final Exception e) {
			log.error(
					"Error creating BasicAccumuloOperations",
					e);
			System.exit(1);
		}

		System.out.println("Finished ingesting data to namespace: " + args[4] + " at accumulo instance: " + args[1]);

	}

	/***
	 * Here we will change the ingest mechanism to use a producer/consumer
	 * pattern
	 */
	@Override
	protected void generateGrid(
			final BasicAccumuloOperations bao ) {

		// create our datastore object
		final DataStore geowaveDataStore = getGeowaveDataStore(bao);

		// In order to store data we need to determine the type of data store
		final SimpleFeatureType point = createPointFeatureType();

		// This a factory class that builds simple feature objects based on the
		// type passed
		final SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(
				point);

		// This is an adapter, that is needed to describe how to persist the
		// data type passed
		final FeatureDataAdapter adapter = createDataAdapter(point);

		// This describes how to index the data
		final Index index = createSpatialIndex();

		// features require a featureID - this should be unqiue as it's a
		// foreign key on the feature
		// (i.e. sending in a new feature with the same feature id will
		// overwrite the existing feature)
		int featureId = 0;

		final Thread ingestThread = new Thread(
				new Runnable() {
					@Override
					public void run() {
						geowaveDataStore.ingest(
								adapter,
								index,
								features);
					}
				},
				"Ingestion Thread");

		ingestThread.start();

		// build a grid of points across the globe at each whole
		// lattitude/longitude intersection
		for (int longitude = -180; longitude <= 180; longitude++) {
			for (int latitude = -90; latitude <= 90; latitude++) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				featureId++;
				features.add(sft);
			}
		}
		features.ingestCompleted = true;
		try {
			ingestThread.join();
		}
		catch (final InterruptedException e) {
			log.error(
					"Error joining ingest thread",
					e);
		}
	}

	protected class FeatureCollection implements
			Iterator<SimpleFeature>
	{
		private final BlockingQueue<SimpleFeature> queue = new LinkedBlockingQueue<SimpleFeature>(
				10000);
		public boolean ingestCompleted = false;

		public void add(
				final SimpleFeature sft ) {
			try {
				queue.put(sft);
			}
			catch (final InterruptedException e) {
				log.error(
						"Error inserting next item into queue",
						e);
			}
		}

		@Override
		public boolean hasNext() {
			return !(ingestCompleted && queue.isEmpty());
		}

		@Override
		public SimpleFeature next() {
			try {
				return queue.take();
			}
			catch (final InterruptedException e) {
				log.error(
						"Error getting next item from queue",
						e);
			}
			return null;
		}

		@Override
		public void remove() {
			log.error("Remove called, method not implemented");
		}
	}
}
