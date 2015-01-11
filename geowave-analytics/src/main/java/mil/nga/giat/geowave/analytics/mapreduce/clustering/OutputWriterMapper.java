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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class OutputWriterMapper extends
		Mapper<GeoWaveInputKey, SimpleFeatureImpl, Text, Mutation>
{
	// protected String runId;
	protected List<DataPoint> centroids;
	protected Index index;
	protected SimpleFeatureType inputType;
	protected WritableDataAdapter<SimpleFeature> adapter;

	Text outKey;

	final Logger log = LoggerFactory.getLogger(OutputWriterMapper.class);

	@Override
	protected void setup(
			final Context context ) {
		centroids = new ArrayList<DataPoint>();

		// retrieve centroid information from table
		final String userName = context.getConfiguration().get(
				"accumulo.userName");
		final String userPassword = context.getConfiguration().get(
				"accumulo.password");
		final String instanceName = context.getConfiguration().get(
				"zookeeper.instanceName");
		final String zooservers = context.getConfiguration().get(
				"zookeeper.zooservers");

		final String tempRunTableName = context.getConfiguration().get(
				"kmeans.table");
		final String runId = context.getConfiguration().get(
				"run.id");
		final String iteration = context.getConfiguration().get(
				"iteration.number");

		final ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(
				instanceName,
				zooservers);
		try {
			final Connector accumuloConnector = zookeeperInstance.getConnector(
					userName,
					new PasswordToken(
							userPassword));

			final Scanner scanner = accumuloConnector.createScanner(
					tempRunTableName,
					new Authorizations());
			final IteratorSetting iter = ClusteringUtils.createScanIterator(
					"GeoSearch Filter",
					runId,
					iteration,
					null,
					null,
					false);
			scanner.addScanIterator(iter);

			for (final Entry<Key, Value> entry : scanner) {
				// key: runId | iter# | centroid_id
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
			final String dataTypeId = context.getConfiguration().get(
					"dataTypeId"); // "Location"
			inputType = ClusteringUtils.createPointSimpleFeatureType(dataTypeId);
			adapter = new FeatureDataAdapter(
					inputType);
			index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

			outKey = new Text(
					context.getConfiguration().get(
							"kmeans.table"));

		}
		catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			log.error(
					"Output writer mapper: " + e.getMessage(),
					e);
		}
	}

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
		final String outputRowId = context.getConfiguration().get(
				"outputRowId");

		// find the closest centroid to dp
		DataPoint assignedCentroid = null;
		double leastDist = Double.MAX_VALUE;
		for (final DataPoint centroid : centroids) {
			final double dist = centroid.calculateDistance(dp);
			if (dist < leastDist) {
				assignedCentroid = centroid;
				leastDist = dist;
			}
		}

		final Mutation m = new Mutation(
				outputRowId);
		m.put(
				new Text(
						assignedCentroid.id.toString()),
				new Text(
						pointId.toString()),
				new Value(
						coord.toString().getBytes()));

		context.write(
				outKey,
				m);
	}
}