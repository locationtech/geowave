package mil.nga.giat.geowave.examples.ingest;

import java.io.IOException;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleIngestIndexWriter extends
		SimpleIngest
{
	private static Logger log = LoggerFactory.getLogger(SimpleIngestIndexWriter.class);

	public static void main(
			final String[] args ) {

		if ((args == null) || (args.length == 0)) {
			log.error("Invalid arguments, expected: dataStoreOptions");
			System.exit(1);
		}

		final SimpleIngestIndexWriter si = new SimpleIngestIndexWriter();
		DataStore geowaveDataStore = null;
		String namespace = null;
		String instance = null;

		if (args.length != 5) {
			log
					.error("Invalid arguments, expected: zookeepers, accumuloInstance, accumuloUser, accumuloPass, geowaveNamespace");
			System.exit(1);
		}
		namespace = args[4];
		instance = args[1];
		try {
			final BasicAccumuloOperations bao = si.getAccumuloOperationsInstance(
					args[0],
					args[1],
					args[2],
					args[3],
					args[4]);
			geowaveDataStore = si.getAccumuloGeowaveDataStore(bao);
		}
		catch (final Exception e) {
			log.error(
					"Error creating BasicAccumuloOperations",
					e);
			System.exit(1);
		}

		si.generateGrid(geowaveDataStore);
		System.out
				.println("Finished ingesting data to namespace: " + namespace + " at datastore instance: " + instance);

	}

	/***
	 * Here we will change the ingest mechanism to use a producer/consumer
	 * pattern
	 */
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
		final GeotoolsFeatureDataAdapter adapter = createDataAdapter(point);

		// This describes how to index the data
		final PrimaryIndex index = createSpatialIndex();

		// features require a featureID - this should be unqiue as it's a
		// foreign key on the feature
		// (i.e. sending in a new feature with the same feature id will
		// overwrite the existing feature)
		final int featureId = 0;

		// get a handle on a GeoWave index writer which wraps the Accumulo
		// BatchWriter, make sure to close it (here we use a try with resources
		// block to close it automatically)
		try (IndexWriter indexWriter = geowaveDataStore.createWriter(
				adapter,
				index)) {
			// build a grid of points across the globe at each whole
			// lattitude/longitude intersection

			for (final SimpleFeature sft : getGriddedFeatures(
					pointBuilder,
					1000)) {
				indexWriter.write(sft);
			}
		}
		catch (final IOException e) {
			log.warn(
					"Unable to close index writer",
					e);
		}
	}
}
