/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.PartitionParameter;

public class NearestNeighborOptions
{

	@OutputParameter(OutputParameters.Output.HDFS_OUTPUT_PATH)
	@Parameter(names = {
		"-oop",
		"--outputHdfsOutputPath"
	}, required = true, description = "Output HDFS File Path")
	private String outputHdfsOutputPath;

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
	}, required = true, description = "Maximum Partition Distance")
	private String partitionMaxDistance;

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

	public String getPartitionMaxDistance() {
		return partitionMaxDistance;
	}

	public void setPartitionMaxDistance(
			String partitionMaxDistance ) {
		this.partitionMaxDistance = partitionMaxDistance;
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
}
