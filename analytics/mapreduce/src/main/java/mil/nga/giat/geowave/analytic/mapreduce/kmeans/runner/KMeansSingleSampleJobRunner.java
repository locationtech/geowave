package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import java.util.Set;
import java.util.UUID;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytic.clustering.ClusteringUtils;
import mil.nga.giat.geowave.analytic.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.ClusteringRunner;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.FormatConfiguration;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * 
 */
public class KMeansSingleSampleJobRunner<T> extends
		MapReduceJobController implements
		ClusteringRunner
{
	final KSamplerJobRunner sampleSetsRunner = new KSamplerJobRunner();
	final KMeansIterationsJobRunner<T> kmeansJobRunner = new KMeansIterationsJobRunner<T>();

	private int currentZoomLevel = 1;

	public KMeansSingleSampleJobRunner() {
		// defaults
		setZoomLevel(1);

		// sets of child runners
		init(
				new MapReduceJobRunner[] {
					sampleSetsRunner,
					kmeansJobRunner
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
		sampleSetsRunner.setZoomLevel(zoomLevel);
	}

	@Override
	public void setInputFormatConfiguration(
			final FormatConfiguration inputFormatConfiguration ) {
		sampleSetsRunner.setInputFormatConfiguration(inputFormatConfiguration);
		kmeansJobRunner.setInputFormatConfiguration(inputFormatConfiguration);
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

		ClusteringUtils.createAdapter(propertyManagement);
		ClusteringUtils.createIndex(propertyManagement);

		return super.run(
				config,
				propertyManagement);
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		kmeansJobRunner.fillOptions(options);
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.MAX_REDUCER_COUNT
				});
		SampleParameters.fillOptions(
				options,
				new SampleParameters.Sample[] {
					SampleParameters.Sample.SAMPLE_SIZE,
					SampleParameters.Sample.SAMPLE_RANK_FUNCTION
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
		ClusteringParameters.fillOptions(
				options,
				new ClusteringParameters.Clustering[] {
					ClusteringParameters.Clustering.MAX_REDUCER_COUNT
				});

		NestedGroupCentroidAssignment.fillOptions(options);

		// override
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

}