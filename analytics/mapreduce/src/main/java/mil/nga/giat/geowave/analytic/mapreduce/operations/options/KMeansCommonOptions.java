package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.ClusteringParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.HullParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.SampleParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CentroidParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ClusteringParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.GlobalParameter;
import mil.nga.giat.geowave.analytic.param.annotations.HullParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.SampleParameter;

public class KMeansCommonOptions
{
	@CentroidParameter(CentroidParameters.Centroid.EXTRACTOR_CLASS)
	@Parameter(names = {
		"-cce",
		"--centroidExtractorClass"
	}, description = "Centroid Exractor Class implements mil.nga.giat.geowave.analytics.extract.CentroidExtractor")
	private String centroidExtractorClass;

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
	}, description = "Maximum number of iterations when finding optimal clusters")
	private String clusteringMaxIterations;

	@ClusteringParameter(ClusteringParameters.Clustering.MAX_REDUCER_COUNT)
	@Parameter(names = {
		"-crc",
		"--clusteringMaxReducerCount"
	}, description = "Maximum Clustering Reducer Count")
	private String clusteringMaxReducerCount;

	@ClusteringParameter(ClusteringParameters.Clustering.RETAIN_GROUP_ASSIGNMENTS)
	@Parameter(names = {
		"-ga",
		"--clusteringRetainGroupAssignments"
	}, description = "Retain Group assignments during execution")
	private String clusteringRetainGroupAssignments;

	@ClusteringParameter(ClusteringParameters.Clustering.ZOOM_LEVELS)
	@Parameter(names = {
		"-zl",
		"--clusteringZoomLevels"
	}, description = "Number of Zoom Levels to Process")
	private String clusteringZoomLevels;

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

	@ExtractParameter(ExtractParameters.Extract.GROUP_ID)
	@Parameter(names = {
		"-eg",
		"--extractGroupId"
	}, description = "Group ID assigned to extracted data")
	private String extractGroupId;

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

	@InputParameter(InputParameters.Input.HDFS_INPUT_PATH)
	@Parameter(names = {
		"-iip",
		"--inputHdfsInputPath"
	}, description = "Input HDFS File Path")
	private String inputHdfsInputPath;

	@SampleParameter(SampleParameters.Sample.DATA_TYPE_ID)
	@Parameter(names = {
		"-sdt",
		"--sampleDataTypeId"
	}, description = "Sample Data Type Id")
	private String sampleDataTypeId;

	@SampleParameter(SampleParameters.Sample.INDEX_ID)
	@Parameter(names = {
		"-sdt",
		"--sampleIndexId"
	}, description = "Sample Index Type Id")
	private String sampleIndexId;

	@SampleParameter(SampleParameters.Sample.MAX_SAMPLE_SIZE)
	@Parameter(names = {
		"-sxs",
		"--sampleMaxSampleSize"
	}, description = "Max Sample Size")
	private String sampleMaxSampleSize;

	@SampleParameter(SampleParameters.Sample.MIN_SAMPLE_SIZE)
	@Parameter(names = {
		"-sms",
		"--sampleMinSampleSize"
	}, description = "Minimum Sample Size")
	private String sampleMinSampleSize;

	@SampleParameter(SampleParameters.Sample.PROBABILITY_FUNCTION)
	@Parameter(names = {
		"-spf",
		"--sampleProbabilityFunction"
	}, description = "The PDF determines the probability for samping an item. Used by specific sample rank functions, such as CentroidDistanceBasedSamplingRankFunction.")
	private String sampleProbabilityFunction;

	@SampleParameter(SampleParameters.Sample.SAMPLE_ITERATIONS)
	@Parameter(names = {
		"-ssi",
		"--sampleSampleIterations"
	}, description = "Minimum number of sample iterations")
	private String sampleSampleIterations;

	@SampleParameter(SampleParameters.Sample.SAMPLE_RANK_FUNCTION)
	@Parameter(names = {
		"-srf",
		"--sampleSampleRankFunction"
	}, description = "The rank function used when sampling the first N highest rank items.")
	private String sampleSampleRankFunction;

	@SampleParameter(SampleParameters.Sample.SAMPLE_SIZE)
	@Parameter(names = {
		"-sss",
		"--sampleSampleSize"
	}, description = "Sample Size")
	private String sampleSampleSize;

	public String getCentroidExtractorClass() {
		return centroidExtractorClass;
	}

	public void setCentroidExtractorClass(
			String centroidExtractorClass ) {
		this.centroidExtractorClass = centroidExtractorClass;
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

	public String getClusteringRetainGroupAssignments() {
		return clusteringRetainGroupAssignments;
	}

	public void setClusteringRetainGroupAssignments(
			String clusteringRetainGroupAssignments ) {
		this.clusteringRetainGroupAssignments = clusteringRetainGroupAssignments;
	}

	public String getClusteringZoomLevels() {
		return clusteringZoomLevels;
	}

	public void setClusteringZoomLevels(
			String clusteringZoomLevels ) {
		this.clusteringZoomLevels = clusteringZoomLevels;
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

	public String getExtractGroupId() {
		return extractGroupId;
	}

	public void setExtractGroupId(
			String extractGroupId ) {
		this.extractGroupId = extractGroupId;
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

	public String getInputHdfsInputPath() {
		return inputHdfsInputPath;
	}

	public void setInputHdfsInputPath(
			String inputHdfsInputPath ) {
		this.inputHdfsInputPath = inputHdfsInputPath;
	}

	public String getSampleDataTypeId() {
		return sampleDataTypeId;
	}

	public void setSampleDataTypeId(
			String sampleDataTypeId ) {
		this.sampleDataTypeId = sampleDataTypeId;
	}

	public String getSampleIndexId() {
		return sampleIndexId;
	}

	public void setSampleIndexId(
			String sampleIndexId ) {
		this.sampleIndexId = sampleIndexId;
	}

	public String getSampleMaxSampleSize() {
		return sampleMaxSampleSize;
	}

	public void setSampleMaxSampleSize(
			String sampleMaxSampleSize ) {
		this.sampleMaxSampleSize = sampleMaxSampleSize;
	}

	public String getSampleMinSampleSize() {
		return sampleMinSampleSize;
	}

	public void setSampleMinSampleSize(
			String sampleMinSampleSize ) {
		this.sampleMinSampleSize = sampleMinSampleSize;
	}

	public String getSampleProbabilityFunction() {
		return sampleProbabilityFunction;
	}

	public void setSampleProbabilityFunction(
			String sampleProbabilityFunction ) {
		this.sampleProbabilityFunction = sampleProbabilityFunction;
	}

	public String getSampleSampleIterations() {
		return sampleSampleIterations;
	}

	public void setSampleSampleIterations(
			String sampleSampleIterations ) {
		this.sampleSampleIterations = sampleSampleIterations;
	}

	public String getSampleSampleRankFunction() {
		return sampleSampleRankFunction;
	}

	public void setSampleSampleRankFunction(
			String sampleSampleRankFunction ) {
		this.sampleSampleRankFunction = sampleSampleRankFunction;
	}

	public String getSampleSampleSize() {
		return sampleSampleSize;
	}

	public void setSampleSampleSize(
			String sampleSampleSize ) {
		this.sampleSampleSize = sampleSampleSize;
	}
}
