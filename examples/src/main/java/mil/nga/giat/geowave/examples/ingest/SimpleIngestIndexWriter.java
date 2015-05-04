package mil.nga.giat.geowave.examples.ingest;

import java.io.IOException;
import java.util.Date;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

public class SimpleIngestIndexWriter extends
		SimpleIngest
{
	private static Logger log = Logger.getLogger(SimpleIngestIndexWriter.class);

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
			final DataStore geowaveDataStore = si.getGeowaveDataStore(bao);
			si.generateGrid(geowaveDataStore);
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
			final DataStore geowaveDataStore ) {

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

		// get a handle on a GeoWave index writer which wraps the Accumulo
		// BatchWriter, make sure to close it (here we use a try with resources
		// block to close it automatically)
		try (IndexWriter indexWriter = geowaveDataStore.createIndexWriter(index)) {
			// build a grid of points across the globe at each whole
			// lattitude/longitude intersection

			for (SimpleFeature sft : getGriddedFeatures(
					pointBuilder,
					1000)) {
				indexWriter.write(
						adapter,
						sft);
			}
		}
		catch (final IOException e) {
			log.warn(
					"Unable to close index writer",
					e);
		}
	}
}
