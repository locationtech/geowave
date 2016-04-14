package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CentroidParameter;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.MapReduceParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;

public class CommonOptions
{

	@CentroidParameter(CentroidParameters.Centroid.DATA_NAMESPACE_URI)
	@Parameter(names = {
		"-cns",
		"--centroidDataNamespaceUri"
	}, description = "Data Type Namespace for centroid item")
	private String centroidDataNamespaceUri;

	@CentroidParameter(CentroidParameters.Centroid.DATA_TYPE_ID)
	@Parameter(names = {
		"-cdt",
		"--centroidDataTypeId"
	}, description = "Data Type ID for a centroid item")
	private String centroidDataTypeId;

	@CentroidParameter(CentroidParameters.Centroid.INDEX_ID)
	@Parameter(names = {
		"-cid",
		"--centroidIndexId"
	}, description = "Index Identifier for Centroids")
	private String centroidIndexId;

	@CommonParameter(CommonParameters.Common.DIMENSION_EXTRACT_CLASS)
	@Parameter(names = {
		"-dde",
		"--commonDimensionExtractClass"
	}, description = "Dimension Extractor Class implements mil.nga.giat.geowave.analytics.extract.DimensionExtractor")
	private String commonDimensionExtractClass;

	@CommonParameter(CommonParameters.Common.DISTANCE_FUNCTION_CLASS)
	@Parameter(names = {
		"-cdf",
		"--commonDistanceFunctionClass"
	}, description = "Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn")
	private String commonDistanceFunctionClass;

	@ExtractParameter(ExtractParameters.Extract.ADAPTER_ID)
	@Parameter(names = {
		"-eit",
		"--extractAdapterId"
	}, description = "Input Data Type ID")
	private String extractAdapterId;

	@ExtractParameter(ExtractParameters.Extract.INDEX_ID)
	@Parameter(names = {
		"-ei",
		"--extractIndexId"
	}, description = "Extract from a specific index")
	private String extractIndexId;

	@ExtractParameter(ExtractParameters.Extract.MAX_INPUT_SPLIT)
	@Parameter(names = {
		"-emx",
		"--extractMaxInputSplit"
	}, description = "Maximum input split size")
	private String extractMaxInputSplit;

	@ExtractParameter(ExtractParameters.Extract.MIN_INPUT_SPLIT)
	@Parameter(names = {
		"-emn",
		"--extractMinInputSplit"
	}, description = "Minimum input split size")
	private String extractMinInputSplit;

	@ExtractParameter(ExtractParameters.Extract.QUERY)
	@Parameter(names = {
		"-eq",
		"--extractQuery"
	}, description = "Query")
	private String extractQuery;

	@InputParameter(InputParameters.Input.INPUT_FORMAT)
	@Parameter(names = {
		"-ifc",
		"--inputInputFormat"
	}, description = "Input Format Class")
	private String inputInputFormat;

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

	@OutputParameter(OutputParameters.Output.HDFS_OUTPUT_PATH)
	@Parameter(names = {
		"-oop",
		"--outputHdfsOutputPath"
	}, description = "Output HDFS File Path")
	private String outputHdfsOutputPath;

	@OutputParameter(OutputParameters.Output.OUTPUT_FORMAT)
	@Parameter(names = {
		"-ofc",
		"--outputOutputFormat"
	}, description = "Output Format Class")
	private String outputOutputFormat;

	@OutputParameter(OutputParameters.Output.REDUCER_COUNT)
	@Parameter(names = {
		"-orc",
		"--outputReducerCount"
	}, description = "Number of Reducers For Output")
	private String outputReducerCount;

	public String getCentroidDataNamespaceUri() {
		return centroidDataNamespaceUri;
	}

	public void setCentroidDataNamespaceUri(
			String centroidDataNamespaceUri ) {
		this.centroidDataNamespaceUri = centroidDataNamespaceUri;
	}

	public String getCentroidDataTypeId() {
		return centroidDataTypeId;
	}

	public void setCentroidDataTypeId(
			String centroidDataTypeId ) {
		this.centroidDataTypeId = centroidDataTypeId;
	}

	public String getCentroidIndexId() {
		return centroidIndexId;
	}

	public void setCentroidIndexId(
			String centroidIndexId ) {
		this.centroidIndexId = centroidIndexId;
	}

	public String getCommonDimensionExtractClass() {
		return commonDimensionExtractClass;
	}

	public void setCommonDimensionExtractClass(
			String commonDimensionExtractClass ) {
		this.commonDimensionExtractClass = commonDimensionExtractClass;
	}

	public String getCommonDistanceFunctionClass() {
		return commonDistanceFunctionClass;
	}

	public void setCommonDistanceFunctionClass(
			String commonDistanceFunctionClass ) {
		this.commonDistanceFunctionClass = commonDistanceFunctionClass;
	}

	public String getExtractAdapterId() {
		return extractAdapterId;
	}

	public void setExtractAdapterId(
			String extractAdapterId ) {
		this.extractAdapterId = extractAdapterId;
	}

	public String getExtractIndexId() {
		return extractIndexId;
	}

	public void setExtractIndexId(
			String extractIndexId ) {
		this.extractIndexId = extractIndexId;
	}

	public String getExtractMaxInputSplit() {
		return extractMaxInputSplit;
	}

	public void setExtractMaxInputSplit(
			String extractMaxInputSplit ) {
		this.extractMaxInputSplit = extractMaxInputSplit;
	}

	public String getExtractMinInputSplit() {
		return extractMinInputSplit;
	}

	public void setExtractMinInputSplit(
			String extractMinInputSplit ) {
		this.extractMinInputSplit = extractMinInputSplit;
	}

	public String getExtractQuery() {
		return extractQuery;
	}

	public void setExtractQuery(
			String extractQuery ) {
		this.extractQuery = extractQuery;
	}

	public String getInputInputFormat() {
		return inputInputFormat;
	}

	public void setInputInputFormat(
			String inputInputFormat ) {
		this.inputInputFormat = inputInputFormat;
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

	public String getOutputHdfsOutputPath() {
		return outputHdfsOutputPath;
	}

	public void setOutputHdfsOutputPath(
			String outputHdfsOutputPath ) {
		this.outputHdfsOutputPath = outputHdfsOutputPath;
	}

	public String getOutputOutputFormat() {
		return outputOutputFormat;
	}

	public void setOutputOutputFormat(
			String outputOutputFormat ) {
		this.outputOutputFormat = outputOutputFormat;
	}

	public String getOutputReducerCount() {
		return outputReducerCount;
	}

	public void setOutputReducerCount(
			String outputReducerCount ) {
		this.outputReducerCount = outputReducerCount;
	}
}
