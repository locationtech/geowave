package mil.nga.giat.geowave.analytic.mapreduce;

import mil.nga.giat.geowave.analytic.AnalyticCLIOperationDriver;
import mil.nga.giat.geowave.analytic.AnalyticOperationCategory;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.clustering.runner.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.dbscan.DBScanIterationsJobRunner;
import mil.nga.giat.geowave.analytic.mapreduce.nn.GeoWaveExtractNNJobRunner;
import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

public class MapReduceAnalyticOperationCLIProvider implements
		CLIOperationProviderSpi
{
	/**
	 * This identifies the set of operations supported and which driver to
	 * execute based on the operation selected.
	 */
	private static final CLIOperation[] MR_ANALYTIC_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"kmeansparallel",
				"KMeans Parallel Clustering",
				new AnalyticCLIOperationDriver(
						new MultiLevelKMeansClusteringJobRunner())),
		new CLIOperation(
				"nn",
				"Nearest Neighbors",
				new AnalyticCLIOperationDriver(
						new GeoWaveExtractNNJobRunner())),
		new CLIOperation(
				"dbscan",
				"Density Based Scanner",
				new AnalyticCLIOperationDriver(
						new DBScanIterationsJobRunner())),
		new CLIOperation(
				"kmeansjump",
				"KMeans Clustering using Jump Method",
				new AnalyticCLIOperationDriver(
						new MultiLevelJumpKMeansClusteringJobRunner()))
	};

	private static final CLIOperationCategory CATEGORY = new AnalyticOperationCategory();

	@Override
	public CLIOperation[] getOperations() {
		return MR_ANALYTIC_OPERATIONS;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return CATEGORY;
	}

}
