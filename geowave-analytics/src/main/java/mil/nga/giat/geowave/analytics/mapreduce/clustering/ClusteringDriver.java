package mil.nga.giat.geowave.analytics.mapreduce.clustering;

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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Polygon;


public class ClusteringDriver
{
	// configuration for Accumulo
	private final String instanceName;
	private final String zooservers;
	private final String user;
	private final String password;
	private Instance zookeeperInstance;
	private Connector accumuloConnector;
	private final String dataTableNamespace;
	private final String tempKMeansTableNamespace;
		
	@SuppressWarnings("deprecation")
	public ClusteringDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableNamespace ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableNamespace = tempKMeansTableNamespace;

		zookeeperInstance = new ZooKeeperInstance(
				instanceName,
				zooservers);

		try {
			accumuloConnector = zookeeperInstance.getConnector(
					user,
					password);
		}
		catch (final AccumuloException e) {
			e.printStackTrace();
		}
		catch (final AccumuloSecurityException e) {
			e.printStackTrace();
		};
	}
	
	public ClusteringDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableNamespace,
			final Connector accumuloConnector) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableNamespace = tempKMeansTableNamespace;
		this.accumuloConnector = accumuloConnector;
	}
	
	public void generatePolygonsForPoints()
	{
		System.out.println("Initiating clustering and polygon generation...");
		
		String dataTypeId = "Location";
		Polygon polygon = ClusteringUtils.generateWorldPolygon();
		
		// spatial index definition and query stuff
		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final SimpleFeatureType type = ClusteringUtils.createPointSimpleFeatureType(dataTypeId);
		final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
				type);
		final DataStore inputDataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumuloConnector,
						dataTableNamespace));

		int numPts = ClusteringUtils.getPointCount(inputDataStore, adapter, index, polygon);
		System.out.println("Input point count: " + numPts);
		
		// no point (pun intended) to process < 3 pts since there wont be a polygon
		if(numPts >= 3)	
		{
			int maxNumClusters;
			if(numPts < 10) maxNumClusters = numPts / 2;
			else if(numPts < 1000) maxNumClusters = numPts / 10;
			else if(numPts < 100000) maxNumClusters = numPts / 1000;
			else maxNumClusters = 100;
			
			JumpDriver jumpDriver = new JumpDriver(
					instanceName,
					zooservers,
					user,
					password,
					dataTableNamespace,
					tempKMeansTableNamespace,
					accumuloConnector);
			
			String clusterAssignmentRowId = jumpDriver.runKMeansWithJumpMethod(maxNumClusters);
			
			PolygonGenerationDriver polygonGenerationDriver = new PolygonGenerationDriver(
					instanceName,
					zooservers,
					user,
					password,
					tempKMeansTableNamespace,
					dataTableNamespace,
					accumuloConnector );
			String outputRowId = polygonGenerationDriver.generatePolygons(clusterAssignmentRowId);
			System.out.println("Output row id: [" + outputRowId + "] in temp table");
			System.out.println("Output data type id: [MultiPolygon] in input table namespace: " + dataTableNamespace);
		}
		else
		{
			System.out.println("Not enough data for polygon genration");
		}
	}

	
	/**
	 * Main entry for running GeoSearch (clustering and polygon generation for point cloud data)	
	 * @param args Array of input parameters:
	 *  args[0] == GeoWave instance name
	 *  args[1] == CSV list of Zookeeper nodes
	 *  args[2] == user name for GeoWave instance
	 *  args[3] == password for user
	 *  args[4] == input table namespace in GeoWave, created to only store input point data for this process
	 *  args[5] == namespace in GeoWave to store temporary data
	 */
	public static void main(String[] args)
	{
		String instanceName = args[0];
		String zooservers = args[1];
		String user = args[2];
		String password = args[3];
		String dataTableNamespace = args[4];		
		String tempKMeansTableNamespace = args[5];
//		boolean destroyTempTable = Boolean.parseBoolean(args[6]);
		
		ClusteringDriver clusteringDriver = new ClusteringDriver(
				instanceName,
				zooservers,
				user,
				password,
				dataTableNamespace,
				tempKMeansTableNamespace );
		clusteringDriver.generatePolygonsForPoints();
	}
}
