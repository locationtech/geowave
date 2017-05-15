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

import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CentroidParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ClusteringParameter;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.GlobalParameter;
import mil.nga.giat.geowave.analytic.param.annotations.HullParameter;
import mil.nga.giat.geowave.analytic.param.annotations.SampleParameter;

public class KMeansCommonOptions
{
	@CentroidParameter(CentroidParameters.Centroid.EXTRACTOR_CLASS)
	@Parameter(names = {
		"-cce",
		"--centroidExtractorClass"
	}, description = "Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor")
	private String centroidExtractorClass;

	@CentroidParameter(CentroidParameters.Centroid.INDEX_ID)
	@Parameter(names = {
		"-cid",
		"--centroidIndexId"
	}, description = "Index Identifier for Centroids")
	private String centroidIndexId;

	@CentroidParameter(CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS)
	@Parameter(names = {
		"-cfc",
		"--centroidWrapperFactoryClass"
	}, description = "A factory class that implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory")
	private String centroidWrapperFactoryClass;

	@CentroidParameter(CentroidParameters.Centroid.ZOOM_LEVEL)
	@Parameter(names = {
		"-czl",
		"--centroidZoomLevel"
	}, description = "Zoom Level Number")
	private String centroidZoomLevel;

	@ClusteringParameter(ClusteringParameters.Clustering.CONVERGANCE_TOLERANCE)
	@Parameter(names = {
		"-cct",
		"--clusteringConverganceTolerance"
	}, description = "Convergence Tolerance")
	private String clusteringConverganceTolerance;

	@ClusteringParameter(ClusteringParameters.Clustering.MAX_ITERATIONS)
	@Parameter(names = {
		"-cmi",
		"--clusteringMaxIterations"
	}, required = true, description = "Maximum number of iterations when finding optimal clusters")
	private String clusteringMaxIterations;

	@ClusteringParameter(ClusteringParameters.Clustering.MAX_REDUCER_COUNT)
	@Parameter(names = {
		"-crc",
		"--clusteringMaxReducerCount"
	}, description = "Maximum Clustering Reducer Count")
	private String clusteringMaxReducerCount;

	@ClusteringParameter(ClusteringParameters.Clustering.ZOOM_LEVELS)
	@Parameter(names = {
		"-zl",
		"--clusteringZoomLevels"
	}, required = true, description = "Number of Zoom Levels to Process")
	private String clusteringZoomLevels;

	@CommonParameter(CommonParameters.Common.DIMENSION_EXTRACT_CLASS)
	@Parameter(names = {
		"-dde",
		"--commonDimensionExtractClass"
	}, description = "Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor")
	private String commonDimensionExtractClass;

	@ExtractParameter(ExtractParameters.Extract.DATA_NAMESPACE_URI)
	@Parameter(names = {
		"-ens",
		"--extractDataNamespaceUri"
	}, description = "Output Data Namespace URI")
	private String extractDataNamespaceUri;

	@ExtractParameter(ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS)
	@Parameter(names = {
		"-ede",
		"--extractDimensionExtractClass"
	}, description = "Class to extract dimensions into a simple feature output")
	private String extractDimensionExtractClass;

	@ExtractParameter(ExtractParameters.Extract.OUTPUT_DATA_TYPE_ID)
	@Parameter(names = {
		"-eot",
		"--extractOutputDataTypeId"
	}, description = "Output Data Type ID")
	private String extractOutputDataTypeId;

	@ExtractParameter(ExtractParameters.Extract.REDUCER_COUNT)
	@Parameter(names = {
		"-erc",
		"--extractReducerCount"
	}, description = "Number of Reducers For initial data extraction and de-duplication")
	private String extractReducerCount;

	@GlobalParameter(GlobalParameters.Global.BATCH_ID)
	@Parameter(names = {
		"-b",
		"--globalBatchId"
	}, description = "Batch ID")
	private String globalBatchId;

	@GlobalParameter(GlobalParameters.Global.PARENT_BATCH_ID)
	@Parameter(names = {
		"-pb",
		"--globalParentBatchId"
	}, description = "Batch ID")
	private String globalParentBatchId;

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

	@HullParameter(HullParameters.Hull.PROJECTION_CLASS)
	@Parameter(names = {
		"-hpe",
		"--hullProjectionClass"
	}, description = "Class to project on to 2D space. Implements mil.nga.giat.geowave.analytics.tools.Projection")
	private String hullProjectionClass;

	@HullParameter(HullParameters.Hull.REDUCER_COUNT)
	@Parameter(names = {
		"-hrc",
		"--hullReducerCount"
	}, description = "Centroid Reducer Count")
	private String hullReducerCount;

	@HullParameter(HullParameters.Hull.WRAPPER_FACTORY_CLASS)
	@Parameter(names = {
		"-hfc",
		"--hullWrapperFactoryClass"
	}, description = "Class to create analytic item to capture hulls. Implements mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory")
	private String hullWrapperFactoryClass;

	public String getCentroidExtractorClass() {
		return centroidExtractorClass;
	}

	public void setCentroidExtractorClass(
			String centroidExtractorClass ) {
		this.centroidExtractorClass = centroidExtractorClass;
	}

	public String getCentroidIndexId() {
		return centroidIndexId;
	}

	public void setCentroidIndexId(
			String centroidIndexId ) {
		this.centroidIndexId = centroidIndexId;
	}

	public String getCentroidWrapperFactoryClass() {
		return centroidWrapperFactoryClass;
	}

	public void setCentroidWrapperFactoryClass(
			String centroidWrapperFactoryClass ) {
		this.centroidWrapperFactoryClass = centroidWrapperFactoryClass;
	}

	public String getCentroidZoomLevel() {
		return centroidZoomLevel;
	}

	public void setCentroidZoomLevel(
			String centroidZoomLevel ) {
		this.centroidZoomLevel = centroidZoomLevel;
	}

	public String getClusteringConverganceTolerance() {
		return clusteringConverganceTolerance;
	}

	public void setClusteringConverganceTolerance(
			String clusteringConverganceTolerance ) {
		this.clusteringConverganceTolerance = clusteringConverganceTolerance;
	}

	public String getClusteringMaxIterations() {
		return clusteringMaxIterations;
	}

	public void setClusteringMaxIterations(
			String clusteringMaxIterations ) {
		this.clusteringMaxIterations = clusteringMaxIterations;
	}

	public String getClusteringMaxReducerCount() {
		return clusteringMaxReducerCount;
	}

	public void setClusteringMaxReducerCount(
			String clusteringMaxReducerCount ) {
		this.clusteringMaxReducerCount = clusteringMaxReducerCount;
	}

	public String getClusteringZoomLevels() {
		return clusteringZoomLevels;
	}

	public void setClusteringZoomLevels(
			String clusteringZoomLevels ) {
		this.clusteringZoomLevels = clusteringZoomLevels;
	}

	public String getCommonDimensionExtractClass() {
		return commonDimensionExtractClass;
	}

	public void setCommonDimensionExtractClass(
			String commonDimensionExtractClass ) {
		this.commonDimensionExtractClass = commonDimensionExtractClass;
	}

	public String getExtractDataNamespaceUri() {
		return extractDataNamespaceUri;
	}

	public void setExtractDataNamespaceUri(
			String extractDataNamespaceUri ) {
		this.extractDataNamespaceUri = extractDataNamespaceUri;
	}

	public String getExtractDimensionExtractClass() {
		return extractDimensionExtractClass;
	}

	public void setExtractDimensionExtractClass(
			String extractDimensionExtractClass ) {
		this.extractDimensionExtractClass = extractDimensionExtractClass;
	}

	public String getExtractOutputDataTypeId() {
		return extractOutputDataTypeId;
	}

	public void setExtractOutputDataTypeId(
			String extractOutputDataTypeId ) {
		this.extractOutputDataTypeId = extractOutputDataTypeId;
	}

	public String getExtractReducerCount() {
		return extractReducerCount;
	}

	public void setExtractReducerCount(
			String extractReducerCount ) {
		this.extractReducerCount = extractReducerCount;
	}

	public String getGlobalBatchId() {
		return globalBatchId;
	}

	public void setGlobalBatchId(
			String globalBatchId ) {
		this.globalBatchId = globalBatchId;
	}

	public String getGlobalParentBatchId() {
		return globalParentBatchId;
	}

	public void setGlobalParentBatchId(
			String globalParentBatchId ) {
		this.globalParentBatchId = globalParentBatchId;
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

	public String getHullProjectionClass() {
		return hullProjectionClass;
	}

	public void setHullProjectionClass(
			String hullProjectionClass ) {
		this.hullProjectionClass = hullProjectionClass;
	}

	public String getHullReducerCount() {
		return hullReducerCount;
	}

	public void setHullReducerCount(
			String hullReducerCount ) {
		this.hullReducerCount = hullReducerCount;
	}

	public String getHullWrapperFactoryClass() {
		return hullWrapperFactoryClass;
	}

	public void setHullWrapperFactoryClass(
			String hullWrapperFactoryClass ) {
		this.hullWrapperFactoryClass = hullWrapperFactoryClass;
	}

}
