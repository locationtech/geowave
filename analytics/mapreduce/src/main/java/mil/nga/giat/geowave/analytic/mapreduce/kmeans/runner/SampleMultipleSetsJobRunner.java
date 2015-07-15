package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Loop and sample multiple sets of K centers.
 * 
 * Fulfills steps 3 through 5 in the Kmeans Parellel initialize Algorithm 2,as documented in section 3.3 in
 * 
 * Bahmani, Kumar, Moseley, Vassilvitskii and Vattani. Scalable K-means++. VLDB
 * Endowment Vol. 5, No. 7. 2012.
 * 
 * The number of iterations is assumed to be log(psi), according the paper.
 * 
 * As an added bonus, remove those centers that did not have sufficient number of matches, leaving the top sampleSize/iterations.
 * 
 */
public class SampleMultipleSetsJobRunner<T> extends
		MapReduceJobController implements
		MapReduceJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(SampleMultipleSetsJobRunner.class);

	private final KSamplerJobRunner initialSampleRunner = new KSamplerJobRunner();
	private final UpdateCentroidCostJobRunner updateCostRunner = new UpdateCentroidCostJobRunner();
	private final RankSamplerJobRunner jobGrowSampleRunner = new RankSamplerJobRunner();
	private final StripWeakCentroidsRunner<T> stripWeakCentroidsRunner = new StripWeakCentroidsRunner<T>();
	private final IterationCountCalculateRunner<T> iterationCountCalculateRunner = new IterationCountCalculateRunner<T>();
	private int iterations = 1;
	private int zoomLevel = 1;

	public SampleMultipleSetsJobRunner() {
		stage1Setup();
	}

	private void stage1Setup() {
		init(
				new MapReduceJobRunner[] {
					initialSampleRunner,
					updateCostRunner,
					iterationCountCalculateRunner
				},
				new PostOperationTask[] {
					DoNothingTask,
					DoNothingTask,
					DoNothingTask
				});
	}

	public int getCurrentCentroidCount() {
		return stripWeakCentroidsRunner.getCurrentCentroidCount();
	}

	private void stage2Setup(
			final PropertyManagement runTimeProperties ) {
		setIterations(iterationCountCalculateRunner.getIterationsCount());
		init(
				new MapReduceJobRunner[] {
					jobGrowSampleRunner,
					updateCostRunner,
					stripWeakCentroidsRunner
				},
				new PostOperationTask[] {
					DoNothingTask,
					DoNothingTask,
					new PostOperationTask() {
						@Override
						public void runTask(
								final Configuration config,
								final MapReduceJobRunner runner ) {
							updateCostRunner.setReducerCount(Math.min(
									stripWeakCentroidsRunner.getCurrentCentroidCount(),
									runTimeProperties.getPropertyAsInt(
											ClusteringParameters.Clustering.MAX_REDUCER_COUNT,
											32)));
						}
					}
				});
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		// run stage 1
		updateCostRunner.setReducerCount(1);

		this.stripWeakCentroidsRunner.setRange(
				runTimeProperties.getPropertyAsInt(
						SampleParameters.Sample.MIN_SAMPLE_SIZE,
						2),
				runTimeProperties.getPropertyAsInt(
						SampleParameters.Sample.MAX_SAMPLE_SIZE,
						1000));

		runTimeProperties.store(
				SampleParameters.Sample.SAMPLE_SIZE,
				runTimeProperties.getPropertyAsInt(
						SampleParameters.Sample.MAX_SAMPLE_SIZE,
						1000));

		setIterations(runTimeProperties.getPropertyAsInt(
				SampleParameters.Sample.SAMPLE_ITERATIONS,
				1));

		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
				SimpleFeatureItemWrapperFactory.class);

		runTimeProperties.storeIfEmpty(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				FeatureCentroidDistanceFn.class);

		runTimeProperties.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);

		runTimeProperties.storeIfEmpty(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);

		runTimeProperties.copy(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				SampleParameters.Sample.DATA_TYPE_ID);

		runTimeProperties.copy(
				CentroidParameters.Centroid.INDEX_ID,
				SampleParameters.Sample.INDEX_ID);

		NestedGroupCentroidAssignment.setZoomLevel(
				config,
				zoomLevel);

		stage1Setup();
		final int status1 = super.run(
				config,
				runTimeProperties);
		if (status1 != 0) {
			return status1;
		}
		stage2Setup(runTimeProperties);

		for (int i = 0; i < iterations; i++) {
			final int status2 = super.run(
					config,
					runTimeProperties);
			if (status2 != 0) {
				return status2;
			}
		}
		return 0;
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		SampleParameters.fillOptions(
				options,
				new SampleParameters.Sample[] {
					SampleParameters.Sample.MAX_SAMPLE_SIZE,
					SampleParameters.Sample.SAMPLE_ITERATIONS,
					SampleParameters.Sample.MIN_SAMPLE_SIZE
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

		NestedGroupCentroidAssignment.fillOptions(options);
		CentroidManagerGeoWave.fillOptions(options);
		initialSampleRunner.fillOptions(options);
	}

	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		initialSampleRunner.setInputFormatConfiguration(inputFormatConfiguration);
		updateCostRunner.setInputFormatConfiguration(inputFormatConfiguration);
		jobGrowSampleRunner.setInputFormatConfiguration(inputFormatConfiguration);
	}

	private void setIterations(
			final int iterations ) {
		this.iterations = Math.max(
				this.iterations,
				iterations);
	}

	public void setZoomLevel(
			final int zoomLevel ) {
		this.zoomLevel = zoomLevel;
		initialSampleRunner.setZoomLevel(zoomLevel);
		jobGrowSampleRunner.setZoomLevel(zoomLevel);
	}
}