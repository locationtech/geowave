package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.annotations.ClusteringParameter;
import mil.nga.giat.geowave.analytic.param.annotations.GlobalParameter;
import mil.nga.giat.geowave.analytic.param.annotations.HullParameter;
import mil.nga.giat.geowave.analytic.param.annotations.MapReduceParameter;
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

	@ClusteringParameter(ClusteringParameters.Clustering.GEOMETRIC_DISTANCE_UNIT)
	@Parameter(names = {
		"-du",
		"--clusteringGeometricDistanceUnit"
	}, description = "Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)")
	private String clusteringGeometricDistanceUnit;

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

	@GlobalParameter(GlobalParameters.Global.BATCH_ID)
	@Parameter(names = {
		"-b",
		"--globalBatchId"
	}, description = "Batch ID")
	private String globalBatchId;

	@HullParameter(HullParameters.Hull.DATA_TYPE_ID)
	@Parameter(names = {
		"-hdt",
		"--hullDataTypeId"
	}, description = "Data Type ID for a centroid item")
	private String hullDataTypeId;

	@HullParameter(HullParameters.Hull.HULL_BUILDER)
	@Parameter(names = {
		"-hhb",
		"--hullHullBuilder"
	}, description = "Hull Builder")
	private String hullHullBuilder;

	@HullParameter(HullParameters.Hull.PROJECTION_CLASS)
	@Parameter(names = {
		"-hpe",
		"--hullProjectionClass"
	}, description = "Class to project on to 2D space. Implements mil.nga.giat.geowave.analytics.tools.Projection")
	private String hullProjectionClass;

	@MapReduceParameter(MapReduceParameters.MRConfig.CONFIG_FILE)
	@Parameter(names = {
		"-conf",
		"--mapReduceConfigFile"
	}, description = "MapReduce Configuration")
	private String mapReduceConfigFile;

	@MapReduceParameter(MapReduceParameters.MRConfig.HDFS_BASE_DIR)
	@Parameter(names = {
		"-hdfsbase",
		"--mapReduceHdfsBaseDir"
	}, description = "Fully qualified path to the base directory in hdfs")
	private String mapReduceHdfsBaseDir;

	@MapReduceParameter(MapReduceParameters.MRConfig.HDFS_HOST_PORT)
	@Parameter(names = {
		"-hdfs",
		"--mapReduceHdfsHostPort"
	}, description = "HDFS hostname and port in the format hostname:port")
	private String mapReduceHdfsHostPort;

	@MapReduceParameter(MapReduceParameters.MRConfig.JOBTRACKER_HOST_PORT)
	@Parameter(names = {
		"-jobtracker",
		"--mapReduceJobtrackerHostPort"
	}, description = "Hadoop job tracker hostname and port in the format hostname:port")
	private String mapReduceJobtrackerHostPort;

	@MapReduceParameter(MapReduceParameters.MRConfig.YARN_RESOURCE_MANAGER)
	@Parameter(names = {
		"-resourceman",
		"--mapReduceYarnResourceManager"
	}, description = "Yarn resource manager hostname and port in the format hostname:port")
	private String mapReduceYarnResourceManager;

	@OutputParameter(OutputParameters.Output.DATA_NAMESPACE_URI)
	@Parameter(names = {
		"-ons",
		"--outputDataNamespaceUri"
	}, description = "Output namespace for objects that will be written to GeoWave")
	private String outputDataNamespaceUri;

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

	public String getClusteringGeometricDistanceUnit() {
		return clusteringGeometricDistanceUnit;
	}

	public void setClusteringGeometricDistanceUnit(
			String clusteringGeometricDistanceUnit ) {
		this.clusteringGeometricDistanceUnit = clusteringGeometricDistanceUnit;
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

	public String getGlobalBatchId() {
		return globalBatchId;
	}

	public void setGlobalBatchId(
			String globalBatchId ) {
		this.globalBatchId = globalBatchId;
	}

	public String getHullDataTypeId() {
		return hullDataTypeId;
	}

	public void setHullDataTypeId(
			String hullDataTypeId ) {
		this.hullDataTypeId = hullDataTypeId;
	}

	public String getHullHullBuilder() {
		return hullHullBuilder;
	}

	public void setHullHullBuilder(
			String hullHullBuilder ) {
		this.hullHullBuilder = hullHullBuilder;
	}

	public String getHullProjectionClass() {
		return hullProjectionClass;
	}

	public void setHullProjectionClass(
			String hullProjectionClass ) {
		this.hullProjectionClass = hullProjectionClass;
	}

	public String getMapReduceConfigFile() {
		return mapReduceConfigFile;
	}

	public void setMapReduceConfigFile(
			String mapReduceConfigFile ) {
		this.mapReduceConfigFile = mapReduceConfigFile;
	}

	public String getMapReduceHdfsBaseDir() {
		return mapReduceHdfsBaseDir;
	}

	public void setMapReduceHdfsBaseDir(
			String mapReduceHdfsBaseDir ) {
		this.mapReduceHdfsBaseDir = mapReduceHdfsBaseDir;
	}

	public String getMapReduceHdfsHostPort() {
		return mapReduceHdfsHostPort;
	}

	public void setMapReduceHdfsHostPort(
			String mapReduceHdfsHostPort ) {
		this.mapReduceHdfsHostPort = mapReduceHdfsHostPort;
	}

	public String getMapReduceJobtrackerHostPort() {
		return mapReduceJobtrackerHostPort;
	}

	public void setMapReduceJobtrackerHostPort(
			String mapReduceJobtrackerHostPort ) {
		this.mapReduceJobtrackerHostPort = mapReduceJobtrackerHostPort;
	}

	public String getMapReduceYarnResourceManager() {
		return mapReduceYarnResourceManager;
	}

	public void setMapReduceYarnResourceManager(
			String mapReduceYarnResourceManager ) {
		this.mapReduceYarnResourceManager = mapReduceYarnResourceManager;
	}

	public String getOutputDataNamespaceUri() {
		return outputDataNamespaceUri;
	}

	public void setOutputDataNamespaceUri(
			String outputDataNamespaceUri ) {
		this.outputDataNamespaceUri = outputDataNamespaceUri;
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
