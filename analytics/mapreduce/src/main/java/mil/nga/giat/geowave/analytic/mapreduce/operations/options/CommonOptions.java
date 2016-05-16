package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.analytic.param.InputParameters;
import mil.nga.giat.geowave.analytic.param.annotations.CommonParameter;
import mil.nga.giat.geowave.analytic.param.annotations.ExtractParameter;
import mil.nga.giat.geowave.analytic.param.annotations.InputParameter;
import mil.nga.giat.geowave.analytic.param.annotations.OutputParameter;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class CommonOptions
{

	@CommonParameter(CommonParameters.Common.DISTANCE_FUNCTION_CLASS)
	@Parameter(names = {
		"-cdf",
		"--commonDistanceFunctionClass"
	}, description = "Distance Function Class implements mil.nga.giat.geowave.analytics.distance.DistanceFn")
	private String commonDistanceFunctionClass;

	@ParametersDelegate
	@PrefixParameter(prefix = "query")
	private QueryOptionsCommand queryOptions = new QueryOptionsCommand();

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

	@InputParameter(InputParameters.Input.INPUT_FORMAT)
	@Parameter(names = {
		"-ifc",
		"--inputFormatClass"
	}, description = "Input Format Class")
	private String inputFormatClass;

	@InputParameter(InputParameters.Input.HDFS_INPUT_PATH)
	@Parameter(names = {
		"-iip",
		"--inputHdfsPath"
	}, description = "Input Path")
	private String inputHdfsPath;

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

	public QueryOptionsCommand getQueryOptions() {
		return queryOptions;
	}

	public void setQueryOptions(
			QueryOptionsCommand extractQueryOptions ) {
		this.queryOptions = extractQueryOptions;
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

	public String getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(
			String inputFormatClass ) {
		this.inputFormatClass = inputFormatClass;
	}

	public String getInputHdfsPath() {
		return inputHdfsPath;
	}

	public void setInputHdfsPath(
			String inputHdfsPath ) {
		this.inputHdfsPath = inputHdfsPath;
	}

	/**
	 * Build the query options from the command line arguments.
	 * 
	 * @return
	 */
	public QueryOptions buildQueryOptions() {
		final QueryOptions options = new QueryOptions();
		if (queryOptions.getAdapterIds() != null && queryOptions.getAdapterIds().size() > 0) options.setAdapter(Lists.transform(
				queryOptions.getAdapterIds(),
				new Function<String, ByteArrayId>() {
					@Override
					public ByteArrayId apply(
							String input ) {
						return new ByteArrayId(
								input);
					}
				}));
		if (queryOptions.getAuthorizations() != null) {
			options.setAuthorizations(this.queryOptions.getAuthorizations().toArray(
					new String[this.queryOptions.getAuthorizations().size()]));
		}
		if (queryOptions.getIndexId() != null) {
			options.setIndexId(new ByteArrayId(
					queryOptions.getIndexId()));
		}
		return options;
	}

}
