package mil.nga.giat.geowave.analytic.mapreduce.dbscan;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.GeoWaveOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.GeoWaveInputLoadJobRunner;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters.Extract;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters.Partition;
import mil.nga.giat.geowave.analytic.partitioner.AbstractPartitioner;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.geotools.feature.type.BasicFeatureTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DBScan involves multiple iterations. The first iteration conceivably takes a
 * set of points and produces small clusters (nearest neighbors). Each
 * subsequent iteration merges clusters within a given distance from each other.
 * This process can continue no new clusters are created (merges do not occur).
 * 
 * The first iteration places a constraint on the minimum number of neighbors.
 * Subsequent iterations do not have a minimum, since each of the clusters is
 * already vetted out by the first iteration.
 */

public class DBScanIterationsJobRunner implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(DBScanIterationsJobRunner.class);
	DBScanJobRunner jobRunner = new DBScanJobRunner();
	GeoWaveInputLoadJobRunner inputLoadRunner = new GeoWaveInputLoadJobRunner();
	protected FormatConfiguration inputFormatConfiguration;
	protected int zoomLevel = 1;

	public DBScanIterationsJobRunner() {
		super();
		inputFormatConfiguration = new GeoWaveInputFormatConfiguration();
		jobRunner.setInputFormatConfiguration(inputFormatConfiguration);
		inputLoadRunner.setOutputFormatConfiguration(new GeoWaveOutputFormatConfiguration());
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		this.inputFormatConfiguration = inputFormatConfiguration;
	}

	public void setReducerCount(
			final int reducerCount ) {
		jobRunner.setReducerCount(reducerCount);
	}

	protected void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		runTimeProperties.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		final FileSystem fs = FileSystem.get(config);

		final String outputBaseDir = runTimeProperties.getPropertyAsString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");

		Path startPath = new Path(
				outputBaseDir + "/" + runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE) + "_level_0");
		if (fs.exists(startPath)) {
			fs.delete(
					startPath,
					true);
		}

		AbstractPartitioner.putDistances(
				runTimeProperties,
				new double[] {
					runTimeProperties.getPropertyAsDouble(
							Partition.PARTITION_DISTANCE,
							10)
				});

		jobRunner.setInputFormatConfiguration(inputFormatConfiguration);
		jobRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
				startPath));

		LOGGER.info(
				"Running with partition distance {}",
				runTimeProperties.getPropertyAsDouble(
						Partition.PARTITION_DISTANCE,
						10.0));
		final int initialStatus = jobRunner.run(
				config,
				runTimeProperties);

		if (initialStatus != 0) {
			return initialStatus;
		}

		int maxIterationCount = runTimeProperties.getPropertyAsInt(
				ClusteringParameters.Clustering.MAX_ITERATIONS,
				15);

		int iteration = 2;
		long lastRecordCount = 0;
		double precisionFactor = 0.9;
		while (maxIterationCount > 0) {

			// context does not mater in this case

			try {
				final Partitioner<?> partitioner = runTimeProperties.getClassInstance(
						PartitionParameters.Partition.PARTITIONER_CLASS,
						Partitioner.class,
						OrthodromicDistancePartitioner.class);

				partitioner.initialize(runTimeProperties);
			}
			catch (final IllegalArgumentException argEx) {
				// this occurs if the partitioner decides that the distance is
				// invalid (e.g. bigger than the map space).
				// In this case, we just exist out of the loop.
				// startPath has the final data
				break;
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			final PropertyManagement localScopeProperties = new PropertyManagement(
					runTimeProperties);

			localScopeProperties.store(
					Partition.PARTITION_PRECISION,
					precisionFactor);
			jobRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
					startPath));

			jobRunner.setFirstIteration(false);

			localScopeProperties.store(
					HullParameters.Hull.ZOOM_LEVEL,
					zoomLevel);

			localScopeProperties.store(
					HullParameters.Hull.ITERATION,
					iteration);

			localScopeProperties.storeIfEmpty(
					OutputParameters.Output.DATA_TYPE_ID,
					localScopeProperties.getPropertyAsString(
							HullParameters.Hull.DATA_TYPE_ID,
							"concave_hull"));

			// Set to zero to force each cluster to be moved into the next
			// iteration
			// even if no merge occurs
			localScopeProperties.store(
					ClusteringParameters.Clustering.MINIMUM_SIZE,
					0);

			final Path nextPath = new Path(
					outputBaseDir + "/" + runTimeProperties.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE) + "_level_" + iteration);

			if (fs.exists(nextPath)) {
				fs.delete(
						nextPath,
						true);
			}
			jobRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
					nextPath));

			final int status = jobRunner.run(
					config,
					localScopeProperties);

			if (status != 0) {
				return status;
			}

			final long currentOutputCount = jobRunner.getCounterValue(TaskCounter.REDUCE_OUTPUT_RECORDS);
			if (currentOutputCount == lastRecordCount) {
				maxIterationCount = 0;
			}
			lastRecordCount = currentOutputCount;
			startPath = nextPath;
			maxIterationCount--;
			precisionFactor -= 0.1;
			iteration++;
		}
		final PropertyManagement localScopeProperties = new PropertyManagement(
				runTimeProperties);

		localScopeProperties.storeIfEmpty(
				OutputParameters.Output.DATA_TYPE_ID,
				localScopeProperties.getPropertyAsString(
						HullParameters.Hull.DATA_TYPE_ID,
						"concave_hull"));
		localScopeProperties.storeIfEmpty(
				OutputParameters.Output.DATA_NAMESPACE_URI,
				localScopeProperties.getPropertyAsString(
						HullParameters.Hull.DATA_NAMESPACE_URI,
						BasicFeatureTypes.DEFAULT_NAMESPACE));
		localScopeProperties.storeIfEmpty(
				OutputParameters.Output.INDEX_ID,
				localScopeProperties.get(HullParameters.Hull.INDEX_ID));
		inputLoadRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				startPath));
		inputLoadRunner.run(
				config,
				runTimeProperties);

		return 0;
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		jobRunner.fillOptions(options);
		inputLoadRunner.fillOptions(options);
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				MapReduceJobController.getConfiguration(runTimeProperties),
				runTimeProperties);
	}

}
