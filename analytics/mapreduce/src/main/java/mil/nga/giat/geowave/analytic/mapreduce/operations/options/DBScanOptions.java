package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.annotations.ClusteringParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.GlobalParameter;
import mil.nga.giat.geowave.analytic.param.annotations.HullParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.PartitionParameter;

public class DBScanOptions
{

	@ClusteringParameter(ClusteringParameters.Clustering.DISTANCE_THRESHOLDS)
	@Parameter(names = {
		"-dt",
		"--clusteringDistanceThresholds"
	}, description = "Comma separated list of distance thresholds, per dimension")
	private String clusteringDistanceThresholds;

	@ClusteringParameter(ClusteringParameters.Clustering.MAX_ITERATIONS)
	@Parameter(names = {
		"-cmi",
		"--clusteringMaxIterations"
	}, description = "Maximum number of iterations when finding optimal clusters")
	private String clusteringMaxIterations;

	@ClusteringParameter(ClusteringParameters.Clustering.MINIMUM_SIZE)
	@Parameter(names = {
		"-cms",
		"--clusteringMinimumSize"
	}, description = "Minimum Cluster Size")
	private String clusteringMinimumSize;

	@ExtractParameter(ExtractParameters.Extract.QUERY_OPTIONS)
	@Parameter(names = {
		"-eqf",
		"--extractQueryOptions"
	}, description = "Restricted extracted field list (comma-separated list of field ids)")
	private String extractQueryOptions;

	@GlobalParameter(GlobalParameters.Global.BATCH_ID)
	@Parameter(names = {
		"-b",
		"--globalBatchId"
	}, description = "Batch ID")
	private String globalBatchId;

	@HullParameter(HullParameters.Hull.DATA_NAMESPACE_URI)
	@Parameter(names = {
		"-hns",
		"--hullDataNamespaceUri"
	}, description = "Data Type Namespace for a centroid item")
	private String hullDataNamespaceUri;

	@HullParameter(HullParameters.Hull.DATA_TYPE_ID)
	@Parameter(names = {
		"-hdt",
		"--hullDataTypeId"
	}, description = "Data Type ID for a centroid item")
	private String hullDataTypeId;

	@HullParameter(HullParameters.Hull.INDEX_ID)
	@Parameter(names = {
		"-hid",
		"--hullIndexId"
	}, description = "Index Identifier for Centroids")
	private String hullIndexId;

	@HullParameter(HullParameters.Hull.ITERATION)
	@Parameter(names = {
		"-hi",
		"--hullIteration"
	}, description = "The iteration of the hull calculation")
	private String hullIteration;

	@HullParameter(HullParameters.Hull.PROJECTION_CLASS)
	@Parameter(names = {
		"-hpe",
		"--hullProjectionClass"
	}, description = "Class to project on to 2D space. Implements mil.nga.giat.geowave.analytics.tools.Projection")
	private String hullProjectionClass;

	@HullParameter(HullParameters.Hull.ZOOM_LEVEL)
	@Parameter(names = {
		"-hzl",
		"--hullZoomLevel"
	}, description = "Zoom Level Number")
	private String hullZoomLevel;

	@InputParameter(InputParameters.Input.HDFS_INPUT_PATH)
	@Parameter(names = {
		"-iip",
		"--inputHdfsInputPath"
	}, description = "Input HDFS File Path")
	private String inputHdfsInputPath;

	@OutputParameter(OutputParameters.Output.DATA_TYPE_ID)
	@Parameter(names = {
		"-odt",
		"--outputDataTypeId"
	}, description = "Output Data ID assigned to objects that will be written to GeoWave")
	private String outputDataTypeId;

	@OutputParameter(OutputParameters.Output.INDEX_ID)
	@Parameter(names = {
		"-oid",
		"--outputIndexId"
	}, description = "Output Index ID for objects that will be written to GeoWave")
	private String outputIndexId;

	@PartitionParameter(PartitionParameters.Partition.MAX_MEMBER_SELECTION)
	@Parameter(names = {
		"-pms",
		"--partitionMaxMemberSelection"
	}, description = "Maximum number of members selected from a partition")
	private String partitionMaxMemberSelection;

	@PartitionParameter(PartitionParameters.Partition.PARTITIONER_CLASS)
	@Parameter(names = {
		"-pc",
		"--partitionPartitionerClass"
	}, description = "Index Identifier for Centroids")
	private String partitionPartitionerClass;

	@PartitionParameter(PartitionParameters.Partition.PARTITION_DECREASE_RATE)
	@Parameter(names = {
		"-pdr",
		"--partitionPartitionDecreaseRate"
	}, description = "Rate of decrease for precision(within (0,1])")
	private String partitionPartitionDecreaseRate;

	@PartitionParameter(PartitionParameters.Partition.PARTITION_DISTANCE)
	@Parameter(names = {
		"-pd",
		"--partitionPartitionDistance"
	}, description = "Partition Distance")
	private String partitionPartitionDistance;

	@PartitionParameter(PartitionParameters.Partition.PARTITION_PRECISION)
	@Parameter(names = {
		"-pp",
		"--partitionPartitionPrecision"
	}, description = "Partition Precision")
	private String partitionPartitionPrecision;

	@PartitionParameter(PartitionParameters.Partition.SECONDARY_PARTITIONER_CLASS)
	@Parameter(names = {
		"-psp",
		"--partitionSecondaryPartitionerClass"
	}, description = "Perform secondary partitioning with the provided class")
	private String partitionSecondaryPartitionerClass;

	public String getClusteringDistanceThresholds() {
		return clusteringDistanceThresholds;
	}

	public void setClusteringDistanceThresholds(
			String clusteringDistanceThresholds ) {
		this.clusteringDistanceThresholds = clusteringDistanceThresholds;
	}

	public String getClusteringMaxIterations() {
		return clusteringMaxIterations;
	}

	public void setClusteringMaxIterations(
			String clusteringMaxIterations ) {
		this.clusteringMaxIterations = clusteringMaxIterations;
	}

	public String getClusteringMinimumSize() {
		return clusteringMinimumSize;
	}

	public void setClusteringMinimumSize(
			String clusteringMinimumSize ) {
		this.clusteringMinimumSize = clusteringMinimumSize;
	}

	public String getExtractQueryOptions() {
		return extractQueryOptions;
	}

	public void setExtractQueryOptions(
			String extractQueryOptions ) {
		this.extractQueryOptions = extractQueryOptions;
	}

	public String getGlobalBatchId() {
		return globalBatchId;
	}

	public void setGlobalBatchId(
			String globalBatchId ) {
		this.globalBatchId = globalBatchId;
	}

	public String getHullDataNamespaceUri() {
		return hullDataNamespaceUri;
	}

	public void setHullDataNamespaceUri(
			String hullDataNamespaceUri ) {
		this.hullDataNamespaceUri = hullDataNamespaceUri;
	}

	public String getHullDataTypeId() {
		return hullDataTypeId;
	}

	public void setHullDataTypeId(
			String hullDataTypeId ) {
		this.hullDataTypeId = hullDataTypeId;
	}

	public String getHullIndexId() {
		return hullIndexId;
	}

	public void setHullIndexId(
			String hullIndexId ) {
		this.hullIndexId = hullIndexId;
	}

	public String getHullIteration() {
		return hullIteration;
	}

	public void setHullIteration(
			String hullIteration ) {
		this.hullIteration = hullIteration;
	}

	public String getHullProjectionClass() {
		return hullProjectionClass;
	}

	public void setHullProjectionClass(
			String hullProjectionClass ) {
		this.hullProjectionClass = hullProjectionClass;
	}

	public String getHullZoomLevel() {
		return hullZoomLevel;
	}

	public void setHullZoomLevel(
			String hullZoomLevel ) {
		this.hullZoomLevel = hullZoomLevel;
	}

	public String getInputHdfsInputPath() {
		return inputHdfsInputPath;
	}

	public void setInputHdfsInputPath(
			String inputHdfsInputPath ) {
		this.inputHdfsInputPath = inputHdfsInputPath;
	}

	public String getOutputDataTypeId() {
		return outputDataTypeId;
	}

	public void setOutputDataTypeId(
			String outputDataTypeId ) {
		this.outputDataTypeId = outputDataTypeId;
	}

	public String getOutputIndexId() {
		return outputIndexId;
	}

	public void setOutputIndexId(
			String outputIndexId ) {
		this.outputIndexId = outputIndexId;
	}

	public String getPartitionMaxMemberSelection() {
		return partitionMaxMemberSelection;
	}

	public void setPartitionMaxMemberSelection(
			String partitionMaxMemberSelection ) {
		this.partitionMaxMemberSelection = partitionMaxMemberSelection;
	}

	public String getPartitionPartitionerClass() {
		return partitionPartitionerClass;
	}

	public void setPartitionPartitionerClass(
			String partitionPartitionerClass ) {
		this.partitionPartitionerClass = partitionPartitionerClass;
	}

	public String getPartitionPartitionDecreaseRate() {
		return partitionPartitionDecreaseRate;
	}

	public void setPartitionPartitionDecreaseRate(
			String partitionPartitionDecreaseRate ) {
		this.partitionPartitionDecreaseRate = partitionPartitionDecreaseRate;
	}

	public String getPartitionPartitionDistance() {
		return partitionPartitionDistance;
	}

	public void setPartitionPartitionDistance(
			String partitionPartitionDistance ) {
		this.partitionPartitionDistance = partitionPartitionDistance;
	}

	public String getPartitionPartitionPrecision() {
		return partitionPartitionPrecision;
	}

	public void setPartitionPartitionPrecision(
			String partitionPartitionPrecision ) {
		this.partitionPartitionPrecision = partitionPartitionPrecision;
	}

	public String getPartitionSecondaryPartitionerClass() {
		return partitionSecondaryPartitionerClass;
	}

	public void setPartitionSecondaryPartitionerClass(
			String partitionSecondaryPartitionerClass ) {
		this.partitionSecondaryPartitionerClass = partitionSecondaryPartitionerClass;
	}
}
