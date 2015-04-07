package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytics.clustering.DistortionGroupManagement;
import mil.nga.giat.geowave.analytics.clustering.runners.ClusteringRunner;
import mil.nga.giat.geowave.analytics.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.ClusteringParameters;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.JumpParameters;
import mil.nga.giat.geowave.analytics.parameters.MapReduceParameters;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KMeans Jump algorithm
 * 
 * Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * @formatter:off Couple things to note:
 * 
 * 
 * @formatter:on
 * 
 */
public class KMeansJumpJobRunner extends
		MapReduceJobController implements
		ClusteringRunner
{
	final static Logger LOGGER = LoggerFactory.getLogger(KMeansJumpJobRunner.class);
	final KMeansDistortionJobRunner jumpRunner = new KMeansDistortionJobRunner();
	final KMeansParallelJobRunnerDelegate kmeansRunner = new KMeansParallelJobRunnerDelegate();

	private int currentZoomLevel = 1;
	private Path currentInputPath = null;

	public KMeansJumpJobRunner() {
		// defaults
		setZoomLevel(1);

		// child runners
		init(
				new MapReduceJobRunner[] {
					kmeansRunner,
					jumpRunner,
				},
				new PostOperationTask[] {
					DoNothingTask,
					DoNothingTask
				});
	}

	@Override
	public void setZoomLevel(
			final int zoomLevel ) {
		currentZoomLevel = zoomLevel;
	}

	@Override
	public void setInputHDFSPath(
			final Path inputHDFSPath ) {
		currentInputPath = inputHDFSPath;
	}

	@Override
	public int run(
			final Configuration configuration,
			final PropertyManagement propertyManagement )
			throws Exception {
		return runJob(
				configuration,
				propertyManagement);
	}

	@SuppressWarnings("unchecked")
	private int runJob(
			final Configuration config,
			final PropertyManagement propertyManagement )
			throws Exception {

		propertyManagement.store(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				currentZoomLevel);

		propertyManagement.storeIfEmpty(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());
		propertyManagement.store(
				CommonParameters.Common.HDFS_INPUT_PATH,
				currentInputPath);

		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);
		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);
		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);
		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);

		propertyManagement.copy(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				SampleParameters.Sample.DATA_TYPE_ID);

		propertyManagement.copy(
				CentroidParameters.Centroid.INDEX_ID,
				SampleParameters.Sample.INDEX_ID);

		jumpRunner.setInputHDFSPath(currentInputPath);

		kmeansRunner.setInputHDFSPath(currentInputPath);
		kmeansRunner.setZoomLevel(currentZoomLevel);

		ClusteringUtils.createAdapter(propertyManagement);
		ClusteringUtils.createIndex(propertyManagement);

		final boolean destroyDistorationTable = propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DISTORTION_TABLE_NAME) == null;

		final String currentBatchId = propertyManagement.getPropertyAsString(
				GlobalParameters.Global.BATCH_ID,
				UUID.randomUUID().toString());

		final String tableName = propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.DISTORTION_TABLE_NAME,
				"DIS_" + currentBatchId.replace(
						'-',
						'_')).toString();

		try {
			final BasicAccumuloOperations ops = ClusteringUtils.createOperations(propertyManagement);

			if (!ops.tableExists(tableName)) {
				ops.createTable(tableName);
			}

			final NumericRange rangeOfIterations = propertyManagement.getPropertyAsRange(
					JumpParameters.Jump.RANGE_OF_CENTROIDS,
					new NumericRange(
							2,
							200));
			propertyManagement.store(
					GlobalParameters.Global.PARENT_BATCH_ID,
					currentBatchId);

			for (int k = (int) Math.max(
					2,
					Math.round(rangeOfIterations.getMin())); k < Math.round(rangeOfIterations.getMax()); k++) {

				// regardless of the algorithm, the sample set is fixed in size
				propertyManagement.store(
						SampleParameters.Sample.MIN_SAMPLE_SIZE,
						k);
				propertyManagement.store(
						SampleParameters.Sample.MAX_SAMPLE_SIZE,
						k);
				propertyManagement.store(
						SampleParameters.Sample.SAMPLE_SIZE,
						k);
				jumpRunner.setCentroidsCount(k);

				final String iterationBatchId = currentBatchId + "_" + k;
				propertyManagement.store(
						GlobalParameters.Global.BATCH_ID,
						iterationBatchId);
				jumpRunner.setReducerCount(k);
				LOGGER.info("KMeans for k: " + k + " and batch " + currentBatchId);
				final int status = super.run(
						config,
						propertyManagement);
				if (status != 0) {
					return status;
				}
			}
			propertyManagement.store(
					GlobalParameters.Global.BATCH_ID,
					currentBatchId);

			@SuppressWarnings("rawtypes")
			final Class<AnalyticItemWrapperFactory> analyticItemWrapperFC = propertyManagement.getPropertyAsClass(
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					AnalyticItemWrapperFactory.class);

			/**
			 * Associate the batch id with the best set of groups so the caller
			 * can find the clusters for the given batch
			 */
			final int result = DistortionGroupManagement.retainBestGroups(
					ClusteringUtils.createOperations(propertyManagement),
					(AnalyticItemWrapperFactory<SimpleFeature>) analyticItemWrapperFC.newInstance(),
					propertyManagement.getPropertyAsString(CentroidParameters.Centroid.DATA_TYPE_ID),
					propertyManagement.getPropertyAsString(CentroidParameters.Centroid.INDEX_ID),
					tableName,
					currentBatchId,
					currentZoomLevel);

			if (destroyDistorationTable) {
				ops.deleteTable(tableName);
			}

			return result;
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot create table for distortions",
					ex);
			return 1;
		}

	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		kmeansRunner.singleSamplekmeansJobRunner.fillOptions(options);
		kmeansRunner.parallelJobRunner.fillOptions(options);
		JumpParameters.fillOptions(
				options,
				new JumpParameters.Jump[] {
					JumpParameters.Jump.RANGE_OF_CENTROIDS,
					JumpParameters.Jump.KPLUSPLUS_MIN
				});
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.MAX_REDUCER_COUNT
				});
		CentroidParameters.fillOptions(
				options,
				new CentroidParameters.Centroid[] {
					CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
					CentroidParameters.Centroid.INDEX_ID,
					CentroidParameters.Centroid.DATA_TYPE_ID,
					CentroidParameters.Centroid.DATA_NAMESPACE_URI,
					CentroidParameters.Centroid.EXTRACTOR_CLASS,
				});
		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
					CommonParameters.Common.DIMENSION_EXTRACT_CLASS
				});
		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ZOOKEEKER,
					GlobalParameters.Global.ACCUMULO_INSTANCE,
					GlobalParameters.Global.ACCUMULO_PASSWORD,
					GlobalParameters.Global.ACCUMULO_USER,
					GlobalParameters.Global.ACCUMULO_NAMESPACE,
					GlobalParameters.Global.BATCH_ID
				});
		MapReduceParameters.fillOptions(options);
		// override
		PropertyManagement.removeOption(
				options,
				CommonParameters.Common.HDFS_INPUT_PATH);
		PropertyManagement.removeOption(
				options,
				CentroidParameters.Centroid.ZOOM_LEVEL);
		PropertyManagement.removeOption(
				options,
				SampleParameters.Sample.DATA_TYPE_ID);
		PropertyManagement.removeOption(
				options,
				SampleParameters.Sample.INDEX_ID);
	}

	private static class KMeansParallelJobRunnerDelegate implements
			MapReduceJobRunner
	{
		final KMeansSingleSampleJobRunner<SimpleFeature> singleSamplekmeansJobRunner = new KMeansSingleSampleJobRunner<SimpleFeature>();
		final KMeansParallelJobRunner parallelJobRunner = new KMeansParallelJobRunner();

		@Override
		public int run(
				final Configuration config,
				final PropertyManagement runTimeProperties )
				throws Exception {
			final int k = runTimeProperties.getPropertyAsInt(
					SampleParameters.Sample.SAMPLE_SIZE,
					1);
			final int minkplusplus = runTimeProperties.getPropertyAsInt(
					JumpParameters.Jump.KPLUSPLUS_MIN,
					3);
			if (k >= minkplusplus) {
				return parallelJobRunner.run(
						config,
						runTimeProperties);
			}
			else {
				return singleSamplekmeansJobRunner.run(
						config,
						runTimeProperties);
			}
		}

		public void setZoomLevel(
				final int zoomLevel ) {
			parallelJobRunner.setZoomLevel(zoomLevel);
			singleSamplekmeansJobRunner.setZoomLevel(zoomLevel);
		}

		public void setInputHDFSPath(
				final Path inputHDFSPath ) {
			parallelJobRunner.setInputHDFSPath(inputHDFSPath);
			singleSamplekmeansJobRunner.setInputHDFSPath(inputHDFSPath);
		}

	}
}