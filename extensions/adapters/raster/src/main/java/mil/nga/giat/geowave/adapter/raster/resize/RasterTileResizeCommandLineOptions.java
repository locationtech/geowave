package mil.nga.giat.geowave.adapter.raster.resize;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class RasterTileResizeCommandLineOptions
{
	public static final String INPUT_COVERAGE_NAME_KEY = "inputCoverageName";
	public static final String OUTPUT_COVERAGE_NAME_KEY = "outputCoverageName";
	public static final String MIN_SPLITS_KEY = "minSplits";
	public static final String MAX_SPLITS_KEY = "maxSplits";
	public static final String HDFS_HOST_PORT_KEY = "hdfsHostPort";
	public static final String JOB_TRACKER_HOST_PORT_KEY = "jobSubmissionHostPort";
	public static final String INDEX_ID_KEY = "indexId";
	public static final String TILE_SIZE_KEY = "outputTileSize";
	private final String inputCoverageName;
	private final String outputCoverageName;
	private final Integer minSplits;
	private final Integer maxSplits;
	private final String hdfsHostPort;
	private final String jobTrackerOrResourceManHostPort;
	private final Integer outputTileSize;
	private final String indexId;

	public RasterTileResizeCommandLineOptions(
			final String inputCoverageName,
			final String outputCoverageName,
			final Integer minSplits,
			final Integer maxSplits,
			final String hdfsHostPort,
			final String jobTrackerOrResourceManHostPort,
			final Integer outputTileSize,
			final String indexId ) {
		this.inputCoverageName = inputCoverageName;
		this.outputCoverageName = outputCoverageName;
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.hdfsHostPort = hdfsHostPort;
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
		this.outputTileSize = outputTileSize;
		this.indexId = indexId;
	}

	public String getInputCoverageName() {
		return inputCoverageName;
	}

	public String getOutputCoverageName() {
		return outputCoverageName;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getJobTrackerOrResourceManHostPort() {
		return jobTrackerOrResourceManHostPort;
	}

	public Integer getOutputTileSize() {
		return outputTileSize;
	}

	public String getIndexId() {
		return indexId;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option inputCoverageOption = new Option(
				INPUT_COVERAGE_NAME_KEY,
				true,
				"The name of the feature type to run a KDE on");
		inputCoverageOption.setRequired(true);
		allOptions.addOption(inputCoverageOption);

		final Option outputCoverageOption = new Option(
				OUTPUT_COVERAGE_NAME_KEY,
				true,
				"The min level to run a KDE at");
		outputCoverageOption.setRequired(true);
		allOptions.addOption(outputCoverageOption);
		final Option indexIdOption = new Option(
				INDEX_ID_KEY,
				true,
				"The max level to run a KDE at");
		indexIdOption.setRequired(false);
		allOptions.addOption(indexIdOption);
		final Option minSplitsOption = new Option(
				MIN_SPLITS_KEY,
				true,
				"The min partitions for the input data");
		allOptions.addOption(minSplitsOption);
		final Option maxSplitsOption = new Option(
				MAX_SPLITS_KEY,
				true,
				"The max partitions for the input data");
		allOptions.addOption(maxSplitsOption);
		final Option hdfsHostPortOption = new Option(
				HDFS_HOST_PORT_KEY,
				true,
				"The max partitions for the input data");
		allOptions.addOption(hdfsHostPortOption);
		final Option jobTrackerOption = new Option(
				JOB_TRACKER_HOST_PORT_KEY,
				true,
				"The max partitions for the input data");
		allOptions.addOption(jobTrackerOption);
		final Option tileSizeOption = new Option(
				TILE_SIZE_KEY,
				true,
				"The max partitions for the input data");
		allOptions.addOption(tileSizeOption);
	}

	public static RasterTileResizeCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String inputCoverageName = commandLine.getOptionValue(INPUT_COVERAGE_NAME_KEY);
		final String outputCoverageName = commandLine.getOptionValue(OUTPUT_COVERAGE_NAME_KEY);
		final int minSplits = Integer.parseInt(commandLine.getOptionValue(MIN_SPLITS_KEY));
		final int maxSplits = Integer.parseInt(commandLine.getOptionValue(MAX_SPLITS_KEY));
		String hdfsHostPort = commandLine.getOptionValue(HDFS_HOST_PORT_KEY);

		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		final String jobTrackerOrResourceManHostPort = commandLine.getOptionValue(JOB_TRACKER_HOST_PORT_KEY);
		final int tileSize = Integer.parseInt(commandLine.getOptionValue(TILE_SIZE_KEY));
		String indexId = null;
		if (commandLine.hasOption(INDEX_ID_KEY)) {
			indexId = commandLine.getOptionValue(INDEX_ID_KEY);
		}
		return new RasterTileResizeCommandLineOptions(
				inputCoverageName,
				outputCoverageName,
				minSplits,
				maxSplits,
				hdfsHostPort,
				jobTrackerOrResourceManHostPort,
				tileSize,
				indexId);
	}
}
