package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JumpDriver
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
	private String outputRowIdForJumpValues;

	private final Integer _numDimensions = 2;

	private final String dataTypeId = "Location";

	final Logger log = LoggerFactory.getLogger(JumpDriver.class);

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
	public JumpDriver(
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

	public JumpDriver(
			final String instanceName,
			final String zooservers,
			final String user,
			final String password,
			final String dataTableNamespace,
			final String tempKMeansTableName,
			final Connector accumuloConnector ) {
		this.instanceName = instanceName;
		this.zooservers = zooservers;
		this.user = user;
		this.password = password;
		this.dataTableNamespace = dataTableNamespace;
		this.tempKMeansTableName = tempKMeansTableName;

		this.accumuloConnector = accumuloConnector;
	}

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
					"Jump driver: " + e.getMessage(),
					e);
		}
	}

	private Double calculateDistortion(
			final Integer clusterCount,
			final String runId,
			final Integer iteration )
			throws Exception {
		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes(StringUtils.UTF8_CHAR_SET));

		// create a map/reduce job to do the calculations
		final Job job = new Job(
				new Configuration(),
				"GeoSearch, Jump, calculate distortion: " + clusterCount);

		job.getConfiguration().set(
				"cluster.count",
				clusterCount.toString());
		job.getConfiguration().set(
				"numDimensions",
				_numDimensions.toString());

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
				iteration.toString());
		job.getConfiguration().set(
				"dataTypeId",
				dataTypeId);

		job.getConfiguration().set(
				"jumpRowId",
				outputRowIdForJumpValues);

		// extends wait time to one hour (default: 600 seconds)
		final long milliSeconds = 1000 * 60 * 60;
		job.getConfiguration().setLong(
				"mapred.task.timeout",
				milliSeconds);

		job.setJarByClass(JumpDriver.class);

		// the actual classes used in this job
		job.setMapperClass(DistortionMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// only needs a single reducer to aggregate all stats and calculate
		// average
		job.setReducerClass(DistortionReducer.class);
		job.setNumReduceTasks(1);

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

		return null;
	}

	private void writeFinalAssignmentsToAccumulo(
			final String runId,
			final String iteration,
			final String outputRowId )
			throws Exception {
		final AuthenticationToken authToken = new PasswordToken(
				password.getBytes(StringUtils.UTF8_CHAR_SET));

		// create a map/reduce job to do the calculations
		final Job job = new Job(
				new Configuration(),
				"GeoSearch, Jump, output writing");

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
				"run.id",
				runId);
		job.getConfiguration().set(
				"iteration.number",
				iteration.toString());

		job.getConfiguration().set(
				"kmeans.table",
				tempKMeansTableName);

		job.getConfiguration().set(
				"data.table",
				dataTableNamespace);

		job.getConfiguration().set(
				"outputRowId",
				outputRowId);

		job.getConfiguration().set(
				"dataTypeId",
				dataTypeId);

		// extends wait time to one hour (default: 600 seconds)
		final long milliSeconds = 1000 * 60 * 60;
		job.getConfiguration().setLong(
				"mapred.task.timeout",
				milliSeconds);

		job.setJarByClass(JumpDriver.class);

		// the actual classes used in this job
		job.setMapperClass(OutputWriterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Mutation.class);

		job.setNumReduceTasks(0);

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
		// GeoWaveInputFormat.setQuery(
		// job,
		// new SpatialQuery(
		// ClusteringUtils.generateWorldPolygon()));

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
	}

	/**
	 * The supplied input table in GeoWave is required to only contain points
	 * for this process. This is meant to be a batch processing task for a point
	 * cloud that is the result of a pre-screening query.
	 * 
	 * @param maxNumClusters
	 *            int Maximum cluster count to serve as the upper bound for the
	 *            Jump method
	 * 
	 * @return String Row id in the temporary table that holds the
	 *         point-to-cluster assignments
	 */
	public String runKMeansWithJumpMethod(
			final int maxNumClusters ) {

		log.info("Running KMeans with Jump with max num clusters of: " + maxNumClusters);
		final KMeansDriver kmeans = new KMeansDriver(
				instanceName,
				zooservers,
				user,
				password,
				dataTableNamespace,
				tempKMeansTableName,
				accumuloConnector);

		outputRowIdForJumpValues = UUID.randomUUID() + "_JUMP";

		String outputRowIdForStats = null;

		final HashMap<Integer, KMeansStats> clusterCountToStatsMap = new HashMap<Integer, KMeansStats>();
		for (int kk = 1; kk <= maxNumClusters; kk++) {
			// generate run id for the kmeans run (on the temp table)
			final UUID uuid = UUID.randomUUID();

			try {
				final Integer iter = kmeans.runKMeans(
						uuid.toString(),
						dataTypeId,
						kk);

				log.info("converged iteration: " + iter);
				final KMeansStats stats = new KMeansStats(
						uuid.toString(),
						iter);
				clusterCountToStatsMap.put(
						kk,
						stats);

				// calculate the distortion for this run
				calculateDistortion(
						kk,
						uuid.toString(),
						iter);
			}
			catch (final Exception e) {
				log.error(
						"Jump driver: " + e.getMessage(),
						e);
			}
		}

		// retrieve the jump values
		try {
			final Scanner scanner = accumuloConnector.createScanner(
					tempKMeansTableName,
					new Authorizations());
			final IteratorSetting iter = ClusteringUtils.createScanIterator(
					"GeoSearch Iterator",
					outputRowIdForJumpValues,
					"DISTORTION",
					null,
					null,
					false);
			scanner.addScanIterator(iter);

			double maxJump = -1.0;
			Integer jumpIdx = -1;
			double oldD = 0.0;

			// colQual is cluster count
			final Map<Integer, Double> clusterCountToDistortionMap = new HashMap<Integer, Double>();
			final List<Integer> clusterCounts = new ArrayList<Integer>();
			for (final Entry<Key, Value> entry : scanner) {
				final Integer clusterCount = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
				final Double distortion = Double.parseDouble(entry.getValue().toString());

				clusterCountToDistortionMap.put(
						clusterCount,
						distortion);
				clusterCounts.add(clusterCount);
			}

			// make certain the ascending order of the cluster ids
			Collections.sort(clusterCounts);

			for (final int idx : clusterCounts) {
				final Double newD = clusterCountToDistortionMap.get(idx);
				final Double jump = newD - oldD;
				if (jump > maxJump) {
					maxJump = jump;
					jumpIdx = idx;
				}
				oldD = newD;
			}

			log.info("best estimate for number of clusters: " + jumpIdx);
			scanner.clearScanIterators();
			scanner.close();

			// write point assignment to temp table, associate with the final
			// kmeans run
			outputRowIdForStats = clusterCountToStatsMap.get(jumpIdx).runId + "_FINAL";
			writeFinalAssignmentsToAccumulo(
					clusterCountToStatsMap.get(jumpIdx).runId,
					clusterCountToStatsMap.get(jumpIdx).convergedIteration.toString(),
					outputRowIdForStats);

		}
		catch (final Exception e) {
			log.error(
					"Jump driver: " + e.getMessage(),
					e);
		}

		return outputRowIdForStats;
	}

	private class KMeansStats
	{
		public String runId;
		public Integer convergedIteration;

		public KMeansStats(
				final String runId,
				final Integer iter ) {
			this.runId = runId;
			convergedIteration = iter;
		}
	}
}
