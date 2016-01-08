package mil.nga.giat.geowave.analytic.mapreduce.kde;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class KDECommandLineOptions
{
	public static final String FEATURE_TYPE_KEY = "featureType";
	public static final String MIN_LEVEL_KEY = "minLevel";
	public static final String MAX_LEVEL_KEY = "maxLevel";
	public static final String MIN_SPLITS_KEY = "minSplits";
	public static final String MAX_SPLITS_KEY = "maxSplits";
	public static final String COVERAGE_NAME_KEY = "coverageName";
	public static final String HDFS_HOST_PORT_KEY = "hdfsHostPort";
	public static final String JOB_TRACKER_HOST_PORT_KEY = "jobSubmissionHostPort";
	public static final String TILE_SIZE_KEY = "tileSize";
	public static final String CQL_FILTER_KEY = "cqlFilter";

	private final String featureType;
	private final Integer minLevel;
	private final Integer maxLevel;
	private final Integer minSplits;
	private final Integer maxSplits;
	private final String coverageName;
	private final String hdfsHostPort;
	private final String jobTrackerOrResourceManHostPort;
	private final Integer tileSize;
	private final String cqlFilter;

	public KDECommandLineOptions(
			final String featureType,
			final Integer minLevel,
			final Integer maxLevel,
			final Integer minSplits,
			final Integer maxSplits,
			final String coverageName,
			final String hdfsHostPort,
			final String jobTrackerOrResourceManHostPort,
			final Integer tileSize,
			final String cqlFilter ) {
		this.featureType = featureType;
		this.minLevel = minLevel;
		this.maxLevel = maxLevel;
		this.minSplits = minSplits;
		this.maxSplits = maxSplits;
		this.coverageName = coverageName;
		this.hdfsHostPort = hdfsHostPort;
		this.jobTrackerOrResourceManHostPort = jobTrackerOrResourceManHostPort;
		this.tileSize = tileSize;
		this.cqlFilter = cqlFilter;
	}

	public String getFeatureType() {
		return featureType;
	}

	public Integer getMinLevel() {
		return minLevel;
	}

	public Integer getMaxLevel() {
		return maxLevel;
	}

	public Integer getMinSplits() {
		return minSplits;
	}

	public Integer getMaxSplits() {
		return maxSplits;
	}

	public String getCoverageName() {
		return coverageName;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getJobTrackerOrResourceManHostPort() {
		return jobTrackerOrResourceManHostPort;
	}

	public Integer getTileSize() {
		return tileSize;
	}

	public String getCqlFilter() {
		return cqlFilter;
	}

	public static KDECommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		final String featureType = commandLine.getOptionValue(FEATURE_TYPE_KEY);
		final int minLevel = Integer.parseInt(commandLine.getOptionValue(MIN_LEVEL_KEY));
		final int maxLevel = Integer.parseInt(commandLine.getOptionValue(MAX_LEVEL_KEY));
		final int minSplits = Integer.parseInt(commandLine.getOptionValue(MIN_SPLITS_KEY));
		final int maxSplits = Integer.parseInt(commandLine.getOptionValue(MAX_SPLITS_KEY));
		final String coverageName = commandLine.getOptionValue(COVERAGE_NAME_KEY);
		String hdfsHostPort = commandLine.getOptionValue(HDFS_HOST_PORT_KEY);

		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		final String jobTrackerOrResourceManHostPort = commandLine.getOptionValue(JOB_TRACKER_HOST_PORT_KEY);
		final int tileSize = Integer.parseInt(commandLine.getOptionValue(TILE_SIZE_KEY));
		String cqlFilter = null;
		if (commandLine.hasOption(CQL_FILTER_KEY)) {
			cqlFilter = commandLine.getOptionValue(CQL_FILTER_KEY);
		}
		return new KDECommandLineOptions(
				featureType,
				minLevel,
				maxLevel,
				minSplits,
				maxSplits,
				coverageName,
				hdfsHostPort,
				jobTrackerOrResourceManHostPort,
				tileSize,
				cqlFilter);
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option featureTypeOption = new Option(
				FEATURE_TYPE_KEY,
				true,
				"The name of the feature type to run a KDE on");
		featureTypeOption.setRequired(true);
		allOptions.addOption(featureTypeOption);

		final Option minLevelOption = new Option(
				MIN_LEVEL_KEY,
				true,
				"The min level to run a KDE at");
		minLevelOption.setRequired(true);
		allOptions.addOption(minLevelOption);
		final Option maxLevelOption = new Option(
				MAX_LEVEL_KEY,
				true,
				"The max level to run a KDE at");
		maxLevelOption.setRequired(true);
		allOptions.addOption(maxLevelOption);
		final Option minSplitsOption = new Option(
				MIN_SPLITS_KEY,
				true,
				"The min partitions for the input data");
		minSplitsOption.setRequired(true);
		allOptions.addOption(minSplitsOption);
		final Option maxSplitsOption = new Option(
				MAX_SPLITS_KEY,
				true,
				"The max partitions for the input data");
		maxSplitsOption.setRequired(true);
		allOptions.addOption(maxSplitsOption);
		final Option coverageNameOption = new Option(
				COVERAGE_NAME_KEY,
				true,
				"The max partitions for the input data");
		coverageNameOption.setRequired(true);
		allOptions.addOption(coverageNameOption);
		final Option hdfsHostPortOption = new Option(
				HDFS_HOST_PORT_KEY,
				true,
				"The max partitions for the input data");
		hdfsHostPortOption.setRequired(true);
		allOptions.addOption(hdfsHostPortOption);
		final Option jobTrackerOption = new Option(
				JOB_TRACKER_HOST_PORT_KEY,
				true,
				"The max partitions for the input data");
		jobTrackerOption.setRequired(true);
		allOptions.addOption(jobTrackerOption);
		final Option tileSizeOption = new Option(
				TILE_SIZE_KEY,
				true,
				"The max partitions for the input data");
		tileSizeOption.setRequired(true);
		allOptions.addOption(tileSizeOption);
		final Option cqlFilterOption = new Option(
				CQL_FILTER_KEY,
				true,
				"An optional CQL filter applied to the input data");
		cqlFilterOption.setRequired(false);
		allOptions.addOption(cqlFilterOption);
	}
}
