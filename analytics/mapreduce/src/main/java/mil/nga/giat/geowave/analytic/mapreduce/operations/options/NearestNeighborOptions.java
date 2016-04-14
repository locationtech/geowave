package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.PartitionParameter;

public class NearestNeighborOptions
{

	@ExtractParameter(ExtractParameters.Extract.QUERY_OPTIONS)
	@Parameter(names = {
		"-eqf",
		"--extractQueryOptions"
	}, description = "Restricted extracted field list (comma-separated list of field ids)")
	private String extractQueryOptions;

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

	public String getExtractQueryOptions() {
		return extractQueryOptions;
	}

	public void setExtractQueryOptions(
			String extractQueryOptions ) {
		this.extractQueryOptions = extractQueryOptions;
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
