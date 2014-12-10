package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

public class DataForTesting
{
	final static Logger log = LoggerFactory.getLogger(DataForTesting.class);

	public static void createData(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String tableNamespace ) {
		/*
		 * Data is defined as 4 clusters of 15 points each therefore, clustering
		 * should give 4 clusters as the optimum result.
		 */
		final List<Coordinate> data = new ArrayList<Coordinate>();

		// upper left clump
		for (double lon = -45.2; lon <= -44.8; lon += 0.1) {
			for (double lat = 14.9; lat <= 15.1; lat += 0.1) {
				data.add(new Coordinate(
						lon,
						lat));
			}
		}

		// upper right clump
		for (double lon = 44.8; lon <= 45.2; lon += 0.1) {
			for (double lat = 14.9; lat <= 15.1; lat += 0.1) {
				data.add(new Coordinate(
						lon,
						lat));
			}
		}

		// lower left clump
		for (double lon = -45.2; lon <= -44.8; lon += 0.1) {
			for (double lat = -15.1; lat <= -14.9; lat += 0.1) {
				data.add(new Coordinate(
						lon,
						lat));
			}
		}

		// lower right clump
		for (double lon = 44.8; lon <= 45.2; lon += 0.1) {
			for (double lat = -15.1; lat <= -14.9; lat += 0.1) {
				data.add(new Coordinate(
						lon,
						lat));
			}
		}

		final Instance zookeeperInstance = new ZooKeeperInstance(
				instanceName,
				zooservers);
		try {
			final Connector accumuloConnector = zookeeperInstance.getConnector(
					user,
					new PasswordToken(
							password));
			log.info("connected to accumulo!");

			final DataStore dataStore = new AccumuloDataStore(
					new BasicAccumuloOperations(
							accumuloConnector,
							tableNamespace));
			final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			final SimpleFeatureType type = DataUtilities.createType(
					"Location",
					"location:Point:srid=4326," + // <- the geometry attribute:
													// Point type
							"name:String");
			final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
					type);
			final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
					type);

			Integer idCounter = 0;
			for (final Coordinate coord : data) {
				final Coordinate[] coords = {
					coord
				};
				final CoordinateArraySequence cas = new CoordinateArraySequence(
						coords);
				final Point point = new Point(
						cas,
						new GeometryFactory());

				featureBuilder.add(point);
				featureBuilder.add(idCounter.toString()); // needs to be unique
															// point id
				final SimpleFeature feature = featureBuilder.buildFeature(null);
				dataStore.ingest(
						adapter,
						index,
						feature);
				featureBuilder.reset();

				idCounter++;
			}

			log.info("created point count: " + data.size());

		}
		catch (AccumuloException | SchemaException | AccumuloSecurityException e) {
			e.printStackTrace();
		}
	}

	public static void main(
			final String[] args ) {
		// String tableNamespace = "clustering_data_temp";
		final String instanceName = args[0];
		final String zooservers = args[1];
		final String user = args[2];
		final String password = args[3];
		final String tableNamespace = args[4];

		DataForTesting.createData(
				instanceName,
				zooservers,
				user,
				password,
				tableNamespace);

	}

}
