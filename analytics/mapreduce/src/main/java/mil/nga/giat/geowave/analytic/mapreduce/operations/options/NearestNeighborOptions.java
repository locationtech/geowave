package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.annotations.MapReduceParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.PartitionParameter;

public class NearestNeighborOptions
{

	@OutputParameter(OutputParameters.Output.HDFS_OUTPUT_PATH)
	@Parameter(names = {
		"-oop",
		"--outputHdfsOutputPath"
	}, description = "Output HDFS File Path")
	private String outputHdfsOutputPath;

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

	@PartitionParameter(PartitionParameters.Partition.MAX_DISTANCE)
	@Parameter(names = {
		"-pmd",
		"--partitionMaxDistance"
	}, description = "Maximum Partition Distance")
	private String partitionPartitionDistance;

	@PartitionParameter(PartitionParameters.Partition.PARTITION_PRECISION)
	@Parameter(names = {
		"-pp",
		"--partitionPartitionPrecision"
	}, description = "Partition Precision")
	private String partitionPartitionPrecision;

	@PartitionParameter(PartitionParameters.Partition.DISTANCE_THRESHOLDS)
	@Parameter(names = {
		"-pdt",
		"--partitionDistanceThresholds"
	}, description = "Comma separated list of distance thresholds, per dimension")
	private String partitioningDistanceThresholds;

	@PartitionParameter(PartitionParameters.Partition.GEOMETRIC_DISTANCE_UNIT)
	@Parameter(names = {
		"-pdu",
		"--partitionGeometricDistanceUnit"
	}, description = "Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)")
	private String partitioningGeometricDistanceUnit;

	@PartitionParameter(PartitionParameters.Partition.SECONDARY_PARTITIONER_CLASS)
	@Parameter(names = {
		"-psp",
		"--partitionSecondaryPartitionerClass"
	}, description = "Perform secondary partitioning with the provided class")
	private String partitionSecondaryPartitionerClass;

	public String getOutputHdfsOutputPath() {
		return outputHdfsOutputPath;
	}

	public void setOutputHdfsOutputPath(
			String outputHdfsOutputPath ) {
		this.outputHdfsOutputPath = outputHdfsOutputPath;
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

	public String getPartitionPartitionDistance() {
		return partitionPartitionDistance;
	}

	public void setPartitionPartitionDistance(
			String partitionPartitionDistance ) {
		this.partitionPartitionDistance = partitionPartitionDistance;
	}

	public String getPartitionSecondaryPartitionerClass() {
		return partitionSecondaryPartitionerClass;
	}

	public void setPartitionSecondaryPartitionerClass(
			String partitionSecondaryPartitionerClass ) {
		this.partitionSecondaryPartitionerClass = partitionSecondaryPartitionerClass;
	}

	public String getPartitionPartitionPrecision() {
		return partitionPartitionPrecision;
	}

	public void setPartitionPartitionPrecision(
			String partitionPartitionPrecision ) {
		this.partitionPartitionPrecision = partitionPartitionPrecision;
	}

	public String getPartitioningDistanceThresholds() {
		return partitioningDistanceThresholds;
	}

	public void setPartitioningDistanceThresholds(
			String partitioningDistanceThresholds ) {
		this.partitioningDistanceThresholds = partitioningDistanceThresholds;
	}

	public String getPartitioningGeometricDistanceUnit() {
		return partitioningGeometricDistanceUnit;
	}

	public void setPartitioningGeometricDistanceUnit(
			String partitioningGeometricDistanceUnit ) {
		this.partitioningGeometricDistanceUnit = partitioningGeometricDistanceUnit;
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

}
