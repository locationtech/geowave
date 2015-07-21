package mil.nga.giat.geowave.analytic.mapreduce.clustering.runner;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobController;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileInputFormatConfiguration;
import mil.nga.giat.geowave.analytic.mapreduce.SequenceFileOutputFormatConfiguration;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters.Clustering;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters.Global;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.core.geotime.IndexType;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.feature.type.BasicFeatureTypes;

/**
 * Runs a clustering at multiple levels. Lower levels cluster within each
 * cluster of the higher level.
 * 
 * Steps:
 * 
 * @formatter: off
 * 
 *             (1) Extract and deduplicate items from geowave.
 * 
 *             (2) Cluster item within their assigned groups. Initially, items
 *             are all part of the same group.
 * 
 *             (3) Assign to each point the cluster (group id).
 * 
 *             (4) Repeat steps 2 to 3 for each lower level.
 * 
 * @formatter: on
 * 
 */
public abstract class MultiLevelClusteringJobRunner extends
		MapReduceJobController implements
		MapReduceJobRunner
{

	final GroupAssigmentJobRunner groupAssignmentRunner = new GroupAssigmentJobRunner();
	final GeoWaveAnalyticExtractJobRunner jobExtractRunner = new GeoWaveAnalyticExtractJobRunner();
	final ConvexHullJobRunner hullRunner = new ConvexHullJobRunner();

	public MultiLevelClusteringJobRunner() {
		init(
				new MapReduceJobRunner[] {},
				new PostOperationTask[] {});
	}

	protected abstract ClusteringRunner getClusteringRunner();

	@Override
	public void fillOptions(
			final Set<Option> options ) {

		jobExtractRunner.fillOptions(options);
		hullRunner.fillOptions(options);
		getClusteringRunner().fillOptions(
				options);
		ClusteringParameters.fillOptions(
				options,
				new Clustering[] {
					Clustering.ZOOM_LEVELS
				});
		GlobalParameters.fillOptions(
				options,
				new Global[] {
					Global.BATCH_ID,
					Global.ACCUMULO_NAMESPACE
				});
		MapReduceParameters.fillOptions(options);
		// the output data type is used for centroid management
		PropertyManagement.removeOption(
				options,
				CentroidParameters.Centroid.DATA_TYPE_ID);

		PropertyManagement.removeOption(
				options,
				CentroidParameters.Centroid.DATA_NAMESPACE_URI);
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

		final ClusteringRunner clusteringRunner = getClusteringRunner();
		final Integer zoomLevels = propertyManagement.getPropertyAsInt(
				Clustering.ZOOM_LEVELS,
				1);

		jobExtractRunner.setConf(config);

		final String dataTypeId = propertyManagement.getPropertyAsString(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				"centroid");

		final String namespaceURI = propertyManagement.getPropertyAsString(
				ExtractParameters.Extract.DATA_NAMESPACE_URI,
				BasicFeatureTypes.DEFAULT_NAMESPACE);

		propertyManagement.storeIfEmpty(
				ExtractParameters.Extract.DATA_NAMESPACE_URI,
				namespaceURI);

		propertyManagement.storeIfEmpty(
				ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID,
				dataTypeId);

		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.EXTRACTOR_CLASS,
				SimpleFeatureCentroidExtractor.class);

		propertyManagement.storeIfEmpty(
				CommonParameters.Common.DIMENSION_EXTRACT_CLASS,
				SimpleFeatureGeometryExtractor.class);

		propertyManagement.store(
				CentroidParameters.Centroid.DATA_TYPE_ID,
				dataTypeId);

		propertyManagement.store(
				CentroidParameters.Centroid.DATA_NAMESPACE_URI,
				namespaceURI);

		// TODO: set out index type for extracts?
		propertyManagement.storeIfEmpty(
				CentroidParameters.Centroid.INDEX_ID,
				IndexType.SPATIAL_VECTOR.getDefaultId());

		propertyManagement.storeIfEmpty(
				HullParameters.Hull.INDEX_ID,
				IndexType.SPATIAL_VECTOR.getDefaultId());

		final FileSystem fs = FileSystem.get(config);

		final Path extractPath = GeoWaveAnalyticExtractJobRunner.getHdfsOutputPath(propertyManagement);

		if (fs.exists(extractPath)) {
			fs.delete(
					extractPath,
					true);
		}

		// first. extract data
		int status = jobExtractRunner.run(
				config,
				propertyManagement);

		groupAssignmentRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));
		clusteringRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));
		hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
				extractPath));

		final boolean retainGroupAssigments = propertyManagement.getPropertyAsBoolean(
				Clustering.RETAIN_GROUP_ASSIGNMENTS,
				false);

		// run clustering for each level
		final String outputBaseDir = propertyManagement.getPropertyAsString(
				MapReduceParameters.MRConfig.HDFS_BASE_DIR,
				"/tmp");
		for (int i = 0; (status == 0) && (i < zoomLevels); i++) {
			final int zoomLevel = i + 1;
			clusteringRunner.setZoomLevel(zoomLevel);
			hullRunner.setZoomLevel(zoomLevel);
			// need to get this removed at some point.
			propertyManagement.store(
					CentroidParameters.Centroid.ZOOM_LEVEL,
					zoomLevel);
			status = clusteringRunner.run(
					config,
					propertyManagement);
			if (status == 0) {
				final Path nextPath = new Path(
						outputBaseDir + "/" + propertyManagement.getPropertyAsString(GlobalParameters.Global.ACCUMULO_NAMESPACE) + "_level_" + zoomLevel);
				if (fs.exists(nextPath)) {
					fs.delete(
							nextPath,
							true);
				}

				groupAssignmentRunner.setOutputFormatConfiguration(new SequenceFileOutputFormatConfiguration(
						nextPath));
				groupAssignmentRunner.setZoomLevel(zoomLevel);

				status = retainGroupAssigments ? groupAssignmentRunner.run(
						config,
						propertyManagement) : 0;

				if (status == 0) {
					status = hullRunner.run(
							config,
							propertyManagement);
				}
				if (retainGroupAssigments) {
					clusteringRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
							nextPath));
					hullRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
							nextPath));
					groupAssignmentRunner.setInputFormatConfiguration(new SequenceFileInputFormatConfiguration(
							nextPath));
				}

			}
		}
		return status;
	}
}