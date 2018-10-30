/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import org.locationtech.geowave.analytic.param.ClusteringParameters;
import org.locationtech.geowave.analytic.param.GlobalParameters;
import org.locationtech.geowave.analytic.param.HullParameters;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.param.annotations.ClusteringParameter;
import org.locationtech.geowave.analytic.param.annotations.GlobalParameter;
import org.locationtech.geowave.analytic.param.annotations.HullParameter;
import org.locationtech.geowave.analytic.param.annotations.OutputParameter;
import org.locationtech.geowave.analytic.param.annotations.PartitionParameter;

import com.beust.jcommander.Parameter;

public class DBScanOptions
{
	@OutputParameter(OutputParameters.Output.HDFS_OUTPUT_PATH)
	@Parameter(names = {
		"-oop",
		"--outputHdfsOutputPath"
	}, description = "Output HDFS File Path")
	private String outputHdfsOutputPath;

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

	@ClusteringParameter(ClusteringParameters.Clustering.MAX_ITERATIONS)
	@Parameter(names = {
		"-cmi",
		"--clusteringMaxIterations"
	}, required = true, description = "Maximum number of iterations when finding optimal clusters")
	private String clusteringMaxIterations;

	@ClusteringParameter(ClusteringParameters.Clustering.MINIMUM_SIZE)
	@Parameter(names = {
		"-cms",
		"--clusteringMinimumSize"
	}, required = true, description = "Minimum Cluster Size")
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

	@HullParameter(HullParameters.Hull.PROJECTION_CLASS)
	@Parameter(names = {
		"-hpe",
		"--hullProjectionClass"
	}, description = "Class to project on to 2D space. Implements org.locationtech.geowave.analytics.tools.Projection")
	private String hullProjectionClass;

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

	@PartitionParameter(PartitionParameters.Partition.SECONDARY_PARTITIONER_CLASS)
	@Parameter(names = {
		"-psp",
		"--partitionSecondaryPartitionerClass"
	}, description = "Perform secondary partitioning with the provided class")
	private String partitionSecondaryPartitionerClass;

	public String getPartitioningDistanceThresholds() {
		return partitioningDistanceThresholds;
	}

	public void setPartitioningDistanceThresholds(
			String clusteringDistanceThresholds ) {
		this.partitioningDistanceThresholds = clusteringDistanceThresholds;
	}

	public String getPartitioningGeometricDistanceUnit() {
		return partitioningGeometricDistanceUnit;
	}

	public void setPartitioningGeometricDistanceUnit(
			String clusteringGeometricDistanceUnit ) {
		this.partitioningGeometricDistanceUnit = clusteringGeometricDistanceUnit;
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

	public String getHullProjectionClass() {
		return hullProjectionClass;
	}

	public void setHullProjectionClass(
			String hullProjectionClass ) {
		this.hullProjectionClass = hullProjectionClass;
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

	public String getPartitionMaxDistance() {
		return partitionMaxDistance;
	}

	public void setPartitionMaxDistance(
			String partitionMaxDistance ) {
		this.partitionMaxDistance = partitionMaxDistance;
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

	public String getOutputHdfsOutputPath() {
		return outputHdfsOutputPath;
	}

	public void setOutputHdfsOutputPath(
			String outputHdfsOutputPath ) {
		this.outputHdfsOutputPath = outputHdfsOutputPath;
	}
}
