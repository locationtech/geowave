package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

public class KMeansDriver
{
	// configuration for Accumulo
	private final String instanceName;
	private final String zooservers;
	private final String user;
	private final String password;
	private Instance zookeeperInstance;
	private Connector accumuloConnector;

	private final String dataTableNamespace;
	private final String tempKMeansTableName;
	private String runId;

	private final double convergenceTol = 10e-3;

	final Logger log = LoggerFactory.getLogger(KMeansDriver.class);

	/**
	 * @param instanceName
	 *            String Name of Accumulo instance
	 * @param zooservers
	 *            String CSV of zookeeper addresses
	 * @param user
	 *            String User for Accumulo instance
	 * @param password
	 *            String Password for access
	 * @param dataTableNamespace
	 *            String GeoWave namespace for input data table, only run data
	 *            shall be stored in this table
	 * @param tempKMeansTableName
	 *            String Table for storing intra-processing information
	 */
	public KMeansDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableName ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableName = tempKMeansTableName;

		connectToAccumulo();
	}

	public KMeansDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableName,
			final Connector conn ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableName = tempKMeansTableName;

		accumuloConnector = conn;
	}

	/**
	 * To connect to Accumulo, one must first contact Zookeeper because it is
	 * the service that holds all of the configurations and settings for
	 * Accumulo
	 */
	@SuppressWarnings("deprecation")
	protected void connectToAccumulo() {
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
					"KMeans driver: " + e.getMessage(),
					e);
		}
	}

	/**
	 * Method runs the K-Means clustering process until convergence or until
	 * maximum allowable iteration count has been reached
	 */
	public Integer runKMeans(
			final String runId,
			final String dataTypeId,
			final int numClusters )
			throws Exception {
		this.runId = runId;

		System.out.println("K-Means Driver Running...");

		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes());

		Integer iterCounter = 0;

		/*
		 * Initialize the centroids, writes centroid data to accumulo table
		 */
		initializeCentroids(
				dataTypeId,
				numClusters);
		iterCounter++;

		/*
		 * Set up job to run for iteration 2 - n, until convergence
		 */
		boolean converged = false;
		while (!converged && (iterCounter <= 50)) {
			final Job job = new Job(
					new Configuration(),
					"GeoSearch, K-Means, iteration: " + iterCounter);

			job.getConfiguration().set(
					"zookeeper.instanceName",
					instanceName);
			job.getConfiguration().set(
					"zookeeper.zooservers",
					zooservers);
			job.getConfiguration().set(
					"accumulo.userName",
					user);
			job.getConfiguration().set(
					"accumulo.password",
					password);
			job.getConfiguration().set(
					"data.table",
					dataTableNamespace);
			job.getConfiguration().set(
					"run.id",
					runId);
			job.getConfiguration().set(
					"kmeans.table",
					tempKMeansTableName);
			job.getConfiguration().set(
					"iteration.number",
					iterCounter.toString());
			job.getConfiguration().set(
					"dataTypeId",
					dataTypeId);

			job.setJarByClass(KMeansDriver.class);

			// the actual classes used in this job
			job.setMapperClass(KMeansMapper.Map.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(KMeansReducer.Reduce.class);
			job.setNumReduceTasks(numClusters);

			job.setInputFormatClass(GeoWaveInputFormat.class);
			job.setOutputFormatClass(AccumuloOutputFormat.class);

			// split ranges for polygon query into individual map tasks
			final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
			final SimpleFeatureType type = ClusteringUtils.createPointSimpleFeatureType(dataTypeId);
			final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
					type);
			GeoWaveInputFormat.addDataAdapter(
					job,
					adapter);
			GeoWaveInputFormat.addIndex(
					job,
					index);
			GeoWaveInputFormat.setAccumuloOperationsInfo(
					job,
					zooservers,
					instanceName,
					user,
					password,
					dataTableNamespace);
			GeoWaveInputFormat.setMinimumSplitCount(
					job,
					1);
			GeoWaveInputFormat.setMaximumSplitCount(
					job,
					20);
			GeoWaveInputFormat.setQuery(
					job,
					null);

			// set up AccumuloOutputFormat
			AccumuloOutputFormat.setConnectorInfo(
					job,
					user,
					authToken);
			AccumuloOutputFormat.setZooKeeperInstance(
					job,
					instanceName,
					zooservers);
			AccumuloOutputFormat.setDefaultTableName(
					job,
					tempKMeansTableName);
			AccumuloOutputFormat.setCreateTables(
					job,
					true);

			job.waitForCompletion(true);

			// check for convergence
			converged = checkForConvergence(iterCounter);

			iterCounter++;
		}

		return iterCounter - 1;
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}

	/**
	 * Centroid initialization via random data assignments
	 */
	private List<DataPoint> initializeCentroids(
			final String dataTypeId,
			final int numClusters )
			throws SchemaException {
		List<DataPoint> points = new ArrayList<DataPoint>();

		// spatial index definition and query stuff
		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		// stuff to build SimpleFeature objects
		final SimpleFeatureType type = ClusteringUtils.createPointSimpleFeatureType(dataTypeId);

		// stuff to interact with GeoWave
		final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
				type);
		final DataStore inputDataStore = new AccumuloDataStore(
				new BasicAccumuloOperations(
						accumuloConnector,
						dataTableNamespace));

		final int numPts = ClusteringUtils.getPointCount(
				accumuloConnector,
				dataTableNamespace,
				dataTypeId);

		final Random random = new Random();
		// pick out numPts of random numbers for picking centroid points
		final List<Integer> randInts = new ArrayList<Integer>();
		if (numPts < (numClusters * 10)) {

			// small sample size, just pick the first points for centroids
			for (int ii = 0; ii < numClusters; ii++) {
				randInts.add(ii);
			}
		}
		else {
			// large sample size, pick random points for centroids
			for (int ii = 0; ii < numClusters; ii++) {
				Integer randInt = random.nextInt(numPts);

				// make sure it's not a repeated number
				int breaker = 0;
				while (randInts.contains(randInt) && (breaker < 10)) {
					randInt = random.nextInt(numPts);
					breaker++;
				}
				randInts.add(randInt);
			}

			// sort the picks in ascending order
			Collections.sort(randInts);
		}

		// extract points from GeoWave
		points = ClusteringUtils.getSpecifiedPoints(
				inputDataStore,
				adapter,
				index,
				randInts);

		// write centroids to a custom temporary Accumulo table on the GeoWave
		// instance
		try {
			if (!accumuloConnector.tableOperations().exists(
					tempKMeansTableName)) {
				accumuloConnector.tableOperations().create(
						tempKMeansTableName);
			}

			final BatchWriter writer = accumuloConnector.createBatchWriter(
					tempKMeansTableName,
					new BatchWriterConfig());

			// write initial clusters to table
			Integer centroidCount = 0;
			for (final DataPoint centroid : points) {
				/*
				 * writes to the centroid keeper row
				 * 
				 * run_id | iter# | centroid_id | pt_Str (x,y)
				 */
				final Coordinate coord = new Coordinate(
						centroid.x,
						centroid.y);
				final Mutation mutation = new Mutation(
						runId);
				mutation.put(
						new Text(
								"0"),
						new Text(
								centroidCount.toString()),
						new Value(
								coord.toString().getBytes()));

				centroidCount++;

				writer.addMutation(mutation);
			}

			writer.close();
		}
		catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
			log.error(
					"KMeans driver: " + e.getMessage(),
					e);
		}

		return points;
	}

	/*
	 * convergence metric is the average centroid movement between the previous
	 * and current iteration
	 */
	private boolean checkForConvergence(
			final Integer iterCount ) {
		try {
			final Scanner scanner = accumuloConnector.createScanner(
					tempKMeansTableName,
					new Authorizations());

			// retrieve centroid information from previous run
			final Integer previousIter = iterCount - 1;
			scanner.addScanIterator(ClusteringUtils.createScanIterator(
					"GeoSearch filter",
					runId,
					previousIter.toString(),
					null,
					null,
					false));
			final HashMap<Integer, DataPoint> idToCentroidMap = new HashMap<Integer, DataPoint>();
			for (final Entry<Key, Value> entry : scanner) {
				// entries should be centroid locations for previous run
				String ptStr = entry.getValue().toString();
				ptStr = ptStr.substring(
						1,
						ptStr.length() - 1);
				final String[] splits = ptStr.split(",");

				final int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());

				final DataPoint dp = new DataPoint(
						id,
						Double.parseDouble(splits[0]),
						Double.parseDouble(splits[1]),
						id,
						true);
				idToCentroidMap.put(
						id,
						dp);
			}
			scanner.clearScanIterators();

			// retrieve latest centroid information
			scanner.addScanIterator(ClusteringUtils.createScanIterator(
					"GeoSearch filter",
					runId,
					iterCount.toString(),
					null,
					null,
					false));
			double diff = 0.0;
			for (final Entry<Key, Value> entry : scanner) {
				// entries should be centroid locations for latest run
				String ptStr = entry.getValue().toString();
				ptStr = ptStr.substring(
						1,
						ptStr.length() - 1);
				final String[] splits = ptStr.split(",");

				final int id = Integer.parseInt(entry.getKey().getColumnQualifier().toString());

				final DataPoint dp = new DataPoint(
						id,
						Double.parseDouble(splits[0]),
						Double.parseDouble(splits[1]),
						id,
						true);
				if (idToCentroidMap.get(id) != null) {
					diff += idToCentroidMap.get(
							id).calculateDistance(
							dp);
				}
			}
			scanner.clearScanIterators();

			scanner.close();
			diff /= idToCentroidMap.size();

			if (diff < convergenceTol) {
				return true;
			}
		}
		catch (final TableNotFoundException e) {
			log.error(
					"KMeans driver: " + e.getMessage(),
					e);
		}

		return false;
	}

}
