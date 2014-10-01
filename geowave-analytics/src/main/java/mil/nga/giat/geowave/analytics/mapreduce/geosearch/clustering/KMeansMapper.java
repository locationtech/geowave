package mil.nga.giat.geowave.analytics.mapreduce.geosearch.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.AccumuloUtils;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

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
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class KMeansMapper {
	public static class Map extends Mapper<Key, Value, IntWritable, Text>
	{
		protected String runId;
		protected List<DataPoint> centroids;
//		protected DataStore inputDataStore;
		protected Index index;
		protected SimpleFeatureType inputType;
		protected WritableDataAdapter<SimpleFeature> adapter;

		@Override
		protected void setup(Context context) 
		{
			runId = context.getConfiguration().get("run.id");
			centroids = new ArrayList<DataPoint>();
			
			Integer lastIter = Integer.parseInt(context.getConfiguration().get("iteration.number")) - 1;
			String dataTypeId = context.getConfiguration().get("dataTypeId");
//			String dataTableNamespace = context.getConfiguration().get("data.table");
			
			// retrieve centroid information from table
			String userName = context.getConfiguration().get("accumulo.userName");
			String userPassword = context.getConfiguration().get("accumulo.password");
			String instanceName = context.getConfiguration().get("zookeeper.instanceName");
			String zooservers = context.getConfiguration().get("zookeeper.zooservers");
			String tableName = context.getConfiguration().get("kmeans.table");

			ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(instanceName, zooservers);
			try {
				Connector accumuloConnector = zookeeperInstance.getConnector(userName, new PasswordToken(userPassword));

				Scanner scanner = accumuloConnector.createScanner(tableName, new Authorizations());

				IteratorSetting iter = new IteratorSetting(15, "GeoSearch Iterator", RegExFilter.class);
				RegExFilter.setRegexs(iter, runId, lastIter.toString(), null, null, false);
				scanner.addScanIterator(iter);
				for(final Entry<Key,Value> entry : scanner)
				{
					// key: runId | cc | pt_id
					// value: jts coordinate string					
//					System.out.println("key: " + entry.getKey().toString());	
					
					int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
					
					String ptStr = entry.getValue().toString();
					ptStr = ptStr.substring(1, ptStr.length() -1);
					String[] splits = ptStr.split(",");
					
					DataPoint dp = new DataPoint(id, Double.parseDouble(splits[0]), Double.parseDouble(splits[1]), id, true);
					centroids.add(dp);
					
//					System.out.println("set up, last iter: " + lastIter + ", centroid: " + id + " " + ptStr);
				}
				scanner.close();
				
				// set up global variables
//				inputDataStore = new AccumuloDataStore(new BasicAccumuloOperations(accumuloConnector, dataTableNamespace));
				inputType = ClusteringUtils.createSimpleFeatureType(dataTypeId);
				adapter = new FeatureDataAdapter(inputType);
				index = IndexType.SPATIAL.createDefaultIndex();
				
			} catch (AccumuloException e) {
				e.printStackTrace();
			} catch (AccumuloSecurityException e) {
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			}
			
			// retrieve centroids from previous run			
//			runId = context.getConfiguration().get("run.id");
//			Integer currentIteration = Integer.parseInt(context.getConfiguration().get("iteration.number"));
//			
//			centroids = new ArrayList<DataPoint>();
//			
//			Integer lastIteration = currentIteration - 1;
//			
//			// retrieve centroid information from table
//			String userName = context.getConfiguration().get("accumulo.userName");
//			String userPassword = context.getConfiguration().get("accumulo.password");
//			String instanceName = context.getConfiguration().get("zookeeper.instanceName");
//			String zooservers = context.getConfiguration().get("zookeeper.zooservers");
//			String dataTableName = context.getConfiguration().get("data.table");
//			String kmeansTableName = context.getConfiguration().get("kmeans.table");
//
//			ZooKeeperInstance zookeeperInstance = new ZooKeeperInstance(instanceName, zooservers);
//			try {
//				Connector accumuloConnector = zookeeperInstance.getConnector(userName, new PasswordToken(userPassword));
//				
//				DataStore kmeansDataStore = new AccumuloDataStore(new BasicAccumuloOperations(accumuloConnector, kmeansTableName));
//				index = IndexType.SPATIAL.createDefaultIndex();
//				
//				// type definition for previous run
//				SimpleFeatureType previousType = ClusteringUtils.createSimpleFeatureType(runId + "_" + lastIteration);
//
//				// stuff to interact with GeoWave
//				WritableDataAdapter<SimpleFeature> oldAdapter = new FeatureDataAdapter(previousType);
//
//				// retrieve centroids from previous run
//				centroids = ClusteringUtils.getData(kmeansDataStore, oldAdapter, index, ClusteringUtils.generateWorldPolygon());
//				
//				// set up global variables
//				inputDataStore = new AccumuloDataStore(new BasicAccumuloOperations(accumuloConnector, dataTableName));
//				inputType = ClusteringUtils.createSimpleFeatureType(runId + "_" + currentIteration);
//				adapter = new FeatureDataAdapter(inputType);
//				
//			} catch (AccumuloException | AccumuloSecurityException e) {
//				e.printStackTrace();
//			} 
		}

		/*
		 * Mapper: 
		 *  - iterate through each point, determine closest centroid, output to centroid id
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
			
			double minDist = Double.MAX_VALUE;
			Integer closestCentroidId = -1;
			for(DataPoint centroid : centroids)
			{
				double ptDist = centroid.calculateDistance(dp);
				if(ptDist < minDist)
				{
					minDist = ptDist;
					closestCentroidId = centroid.id;
				}
			}
//			System.out.println("ptid: " + dp.id + ", " + ptStr + ", closest centroid: " + closestCentroidId);
			context.write(new IntWritable(closestCentroidId), new Text(value.toString()));
		}
	}

}
