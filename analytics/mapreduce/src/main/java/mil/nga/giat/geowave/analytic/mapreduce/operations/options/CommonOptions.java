package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;

public class CommonOptions
{

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
