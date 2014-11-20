package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class KMeansMapper
{
	public static class Map extends
			Mapper<GeoWaveInputKey, SimpleFeatureImpl, IntWritable, Text>
	{
		protected String runId;
		protected List<DataPoint> centroids;
		protected Index index;
		protected SimpleFeatureType inputType;
		protected WritableDataAdapter<SimpleFeature> adapter;

		@Override
		protected void setup(
				final Context context ) {
			runId = context.getConfiguration().get(
					"run.id");
			centroids = new ArrayList<DataPoint>();

			final Integer lastIter = Integer.parseInt(context.getConfiguration().get(
					"iteration.number")) - 1;
			final String dataTypeId = context.getConfiguration().get(
					"dataTypeId");

			// retrieve centroid information from table
			final String userName = context.getConfiguration().get(
					"accumulo.userName");
			final String userPassword = context.getConfiguration().get(
					"accumulo.password");
			final String instanceName = context.getConfiguration().get(
					"zookeeper.instanceName");
			final String zooservers = context.getConfiguration().get(
					"zookeeper.zooservers");
			final String tableName = context.getConfiguration().get(
					"kmeans.table");

			final ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(
					instanceName,
					zooservers);
			try {
				final Connector accumuloConnector = zookeeperInstance.getConnector(
						userName,
						new PasswordToken(
								userPassword));

				final Scanner scanner = accumuloConnector.createScanner(
						tableName,
						new Authorizations());

				final IteratorSetting iter = ClusteringUtils.createScanIterator(
						"GeoSearch Iterator",
						runId,
						lastIter.toString(),
						null,
						null,
						false);
				scanner.addScanIterator(iter);

				for (final Entry<Key, Value> entry : scanner) {
					// key: runId | iteration | pt_id
					// value: jts coordinate string
					final int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());

					String ptStr = entry.getValue().toString();
					ptStr = ptStr.substring(
							1,
							ptStr.length() - 1);
					final String[] splits = ptStr.split(",");

					final DataPoint dp = new DataPoint(
							id,
							Double.parseDouble(splits[0]),
							Double.parseDouble(splits[1]),
							id,
							true);

					centroids.add(dp);
				}
				scanner.close();

				// set up global variables
				inputType = ClusteringUtils.createPointSimpleFeatureType(dataTypeId);
				adapter = new FeatureDataAdapter(
						inputType);
				index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

			}
			catch (final AccumuloException e) {
				e.printStackTrace();
			}
			catch (final AccumuloSecurityException e) {
				e.printStackTrace();
			}
			catch (final TableNotFoundException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Mapper: - iterate through each point, determine closest centroid,
		 * output to centroid id
		 */

		@Override
		public void map(
				final GeoWaveInputKey key,
				final SimpleFeatureImpl value,
				final Context context )
				throws IOException,
				InterruptedException {

			final Integer pointId = Integer.parseInt(value.getAttribute(
					"name").toString());
			final Geometry geometry = (Geometry) value.getDefaultGeometry();

			final Coordinate coord = geometry.getCentroid().getCoordinate();

			final DataPoint dp = new DataPoint(
					pointId,
					coord.x,
					coord.y,
					-1,
					false);

			double minDist = Double.MAX_VALUE;
			Integer closestCentroidId = -1;
			for (final DataPoint centroid : centroids) {
				final double ptDist = centroid.calculateDistance(dp);
				if (ptDist < minDist) {
					minDist = ptDist;
					closestCentroidId = centroid.id;
				}
			}
			context.write(
					new IntWritable(
							closestCentroidId),
					new Text(
							coord.toString()));
		}
	}

}
