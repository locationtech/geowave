package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class DistortionMapper extends Mapper<Key, Value, IntWritable, DoubleWritable>
{
	protected String runId;
	protected List<DataPoint> centroids;
	protected DataStore kmeansDataStore;
	protected Index index;
	protected SimpleFeatureType dataType;
	protected WritableDataAdapter<SimpleFeature> adapter;

	@Override
	protected void setup(Context context)
	{			
		runId = context.getConfiguration().get("run.id");
		Integer currentIteration = Integer.parseInt(context.getConfiguration().get("iteration.number"));
		String dataTypeId = context.getConfiguration().get("dataTypeId");
		
		centroids = new ArrayList<DataPoint>();
		
		// retrieve centroid information from table
		String userName = context.getConfiguration().get("accumulo.userName");
		String userPassword = context.getConfiguration().get("accumulo.password");
		String instanceName = context.getConfiguration().get("zookeeper.instanceName");
		String zooservers = context.getConfiguration().get("zookeeper.zooservers");
		String kmeansTableName = context.getConfiguration().get("kmeans.table");

		ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(instanceName, zooservers);
		try {
			Connector accumuloConnector = zookeeperInstance.getConnector(userName, new PasswordToken(userPassword));

			Scanner scanner = accumuloConnector.createScanner(kmeansTableName, new Authorizations());
			scanner.addScanIterator(ClusteringUtils.createScanIterator("GeoSearch filter", runId, currentIteration.toString(), null, null, false));

			for(final Entry<Key,Value> entry : scanner)
			{
				// key: runId | iter# | centroid_id
				// value: jts coordinate string											
				int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());

				String ptStr = entry.getValue().toString();
				ptStr = ptStr.substring(1, ptStr.length() -1);
				String[] splits = ptStr.split(",");

				DataPoint dp = new DataPoint(id, Double.parseDouble(splits[0]), Double.parseDouble(splits[1]), id, true);
				centroids.add(dp);
			}
			scanner.close();
			
			// set up global variables
			dataType = ClusteringUtils.createSimpleFeatureType(dataTypeId);
			adapter = new FeatureDataAdapter(dataType);
			index = IndexType.SPATIAL_VECTOR.createDefaultIndex();			

		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Mapper: 
	 *  - iterate through each point, determine closest centroid, calculate distortion, and aggregate
	 */

	@Override
	public void  map(Key key, Value value, Context context) throws IOException, InterruptedException {
		// let GeoWaveUtils decode input
		SimpleFeature feature = (SimpleFeature) AccumuloUtils.decodeRow(
				key,
				value,
				adapter,
				index);

		Integer pointId = Integer.parseInt(feature.getAttribute("name").toString());
		Geometry geometry = (Geometry) feature.getDefaultGeometry();

		Point point = geometry.getCentroid();

		DataPoint dp = new DataPoint(pointId, point.getX(), point.getY(), -1, false);

		// step 2.1 - find the closest centroid to dp
		DataPoint assignedCentroid = null;
		double leastDist = Double.MAX_VALUE;
		for(DataPoint centroid : centroids)
		{
			double dist = centroid.calculateDistance(dp);
			if(dist < leastDist)
			{
				assignedCentroid = centroid;
				leastDist = dist;
			}
		}

		// step 2.2 - calculate error for dp
		// using identity matrix for the common covariance, therefore 
		// E[(p - c)^-1 * cov * (p - c)] => (px - cx)^2 + (py - cy)^2
		double x2 = Math.pow(dp.x - assignedCentroid.x, 2);
		double y2 = Math.pow(dp.y - assignedCentroid.y, 2);	
		Double expectation = x2 + y2;

		context.write(new IntWritable(1), new DoubleWritable(expectation));
		//TODO create a combiner to aggregate all expectation parts prior to reducing
	}
}