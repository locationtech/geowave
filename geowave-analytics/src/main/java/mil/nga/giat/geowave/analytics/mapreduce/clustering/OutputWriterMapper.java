package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class OutputWriterMapper extends Mapper<Key, Value, Text, Mutation>
{
//	protected String runId;
	protected List<DataPoint> centroids;
	protected Index index;
	protected SimpleFeatureType inputType;
	protected WritableDataAdapter<SimpleFeature> adapter;
	
	Text outKey;

	@Override
	protected void setup(Context context)
	{
		centroids = new ArrayList<DataPoint>();

		// retrieve centroid information from table
		String userName = context.getConfiguration().get("accumulo.userName");
		String userPassword = context.getConfiguration().get("accumulo.password");
		String instanceName = context.getConfiguration().get("zookeeper.instanceName");
		String zooservers = context.getConfiguration().get("zookeeper.zooservers");

		String tempRunTableName = context.getConfiguration().get("kmeans.table");
		String runId = context.getConfiguration().get("run.id");
		String iteration = context.getConfiguration().get("iteration.number");

		ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(instanceName, zooservers);
		try {
			Connector accumuloConnector = zookeeperInstance.getConnector(userName, new PasswordToken(userPassword));


			Scanner scanner = accumuloConnector.createScanner(tempRunTableName, new Authorizations());
			IteratorSetting iter = ClusteringUtils.createScanIterator("GeoSearch Filter", runId, iteration, null, null, false);
			scanner.addScanIterator(iter);

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
			String dataTypeId = context.getConfiguration().get("dataTypeId");  // "Location"
			inputType = ClusteringUtils.createSimpleFeatureType(dataTypeId);
			adapter = new FeatureDataAdapter(inputType);
			index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			
			outKey = new Text(context.getConfiguration().get("kmeans.table"));

		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void  map(Key key, Value value, Context context) throws IOException, InterruptedException 
	{
		// key : token | cc | pt_id   - token and cc should be constant since this mapper processes a point cloud for token+cc
		// value : String representation of JTS Coordinate (x,y,z), example: (34.75, 31.5, NaN) 		
		String outputRowId = context.getConfiguration().get("outputRowId");
		
		String ptStr = value.toString();
		int endIdx = ptStr.length() -1;
		ptStr = ptStr.substring(1, endIdx);
		String[] splits = ptStr.split(",");

		int id = Integer.parseInt(key.getColumnQualifier().toString());

		DataPoint dp = new DataPoint(id, Double.parseDouble(splits[0]), Double.parseDouble(splits[1]), -1, false);

		// find the closest centroid to dp
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
		
		Mutation m = new Mutation(outputRowId);
		m.put(new Text(new Integer(assignedCentroid.id).toString()), key.getColumnQualifier(), new Value(value.toString().getBytes()));
		
		context.write(outKey, m);
	}
}