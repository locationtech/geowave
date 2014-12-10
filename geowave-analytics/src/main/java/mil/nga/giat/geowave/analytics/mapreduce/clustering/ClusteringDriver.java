package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	final String dataTypeId = "Location";

	final Logger log = LoggerFactory.getLogger(ClusteringDriver.class);

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
		catch (final AccumuloException | AccumuloSecurityException e) {
			log.error(
					"Clustering driver: " + e.getMessage(),
					e);
		}
	}

	public ClusteringDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableNamespace,
			final Connector accumuloConnector ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableNamespace = tempKMeansTableNamespace;
		this.accumuloConnector = accumuloConnector;
	}

	public void generatePolygonsForPoints() {
		log.info("Initiating clustering and polygon generation...");

		final int numPts = ClusteringUtils.getPointCount(
				accumuloConnector,
				dataTableNamespace,
				dataTypeId);
		log.info("Input point count: " + numPts);

		// no point (pun intended) to process < 3 pts since there wont be a
		// polygon
		if (numPts >= 3) {
			int maxNumClusters;
			if (numPts < 10) {
				maxNumClusters = numPts / 2;
			}
			else if (numPts < 1000) {
				maxNumClusters = numPts / 10;
			}
			else if (numPts < 100000) {
				maxNumClusters = numPts / 1000;
			}
			else {
				maxNumClusters = 100;
			}

			final JumpDriver jumpDriver = new JumpDriver(
					instanceName,
					zooservers,
					user,
					password,
					dataTableNamespace,
					tempKMeansTableNamespace,
					accumuloConnector);

			final String clusterAssignmentRowId = jumpDriver.runKMeansWithJumpMethod(maxNumClusters);

			final PolygonGenerationDriver polygonGenerationDriver = new PolygonGenerationDriver(
					instanceName,
					zooservers,
					user,
					password,
					tempKMeansTableNamespace,
					dataTableNamespace,
					accumuloConnector);
			final String outputRowId = polygonGenerationDriver.generatePolygons(clusterAssignmentRowId);
			log.info("Output row id: [" + outputRowId + "] in temp table");
			log.info("Output data type id: [MultiPolygon] in input table namespace: " + dataTableNamespace);
		}
		else {
			log.error("Not enough data for polygon generation");
		}
	}

	/**
	 * Main entry for running GeoSearch (clustering and polygon generation for
	 * point cloud data)
	 * 
	 * @param args
	 *            Array of input parameters: args[0] == GeoWave instance name
	 *            args[1] == CSV list of Zookeeper nodes args[2] == user name
	 *            for GeoWave instance args[3] == password for user args[4] ==
	 *            input table namespace in GeoWave, created to only store input
	 *            point data for this process args[5] == namespace in GeoWave to
	 *            store temporary data
	 */
	public static void main(
			final String[] args ) {
		final String instanceName = args[0];
		final String zooservers = args[1];
		final String user = args[2];
		final String password = args[3];
		final String dataTableNamespace = args[4];
		final String tempKMeansTableNamespace = args[5];

		final ClusteringDriver clusteringDriver = new ClusteringDriver(
				instanceName,
				zooservers,
				user,
				password,
				dataTableNamespace,
				tempKMeansTableNamespace);
		clusteringDriver.generatePolygonsForPoints();
	}
}
