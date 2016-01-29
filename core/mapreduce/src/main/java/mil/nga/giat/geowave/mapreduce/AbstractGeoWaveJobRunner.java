package mil.nga.giat.geowave.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * This class can run a basic job to query GeoWave. It manages Accumulo user,
 * Accumulo password, Accumulo instance name, zookeeper URLs, Accumulo
 * namespace, adapters, indices, query, min splits and max splits.
 */
public abstract class AbstractGeoWaveJobRunner extends
		Configured implements
		Tool,
		CLIOperationDriver
{

	protected static final Logger LOGGER = Logger.getLogger(AbstractGeoWaveJobRunner.class);

	protected Map<String, Object> configOptions;
	protected String namespace;
	protected DataStoreFactorySpi dataStoreFactory;
	protected List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>();
	protected List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();
	protected DistributableQuery query = null;
	protected Integer minInputSplits = null;
	protected Integer maxInputSplits = null;

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	@SuppressWarnings("deprecation")
	public int runJob()
			throws Exception {
		final Job job = new Job(
				super.getConf());
		// must use the assembled job configuration
		final Configuration conf = job.getConfiguration();

		GeoWaveInputFormat.setDataStoreName(
				conf,
				dataStoreFactory.getName());
		GeoWaveInputFormat.setStoreConfigOptions(
				conf,
				ConfigUtils.valuesToStrings(
						configOptions,
						dataStoreFactory.getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				conf,
				namespace);

		GeoWaveOutputFormat.setDataStoreName(
				conf,
				dataStoreFactory.getName());
		GeoWaveOutputFormat.setStoreConfigOptions(
				conf,
				ConfigUtils.valuesToStrings(
						configOptions,
						dataStoreFactory.getOptions()));
		GeoWaveOutputFormat.setGeoWaveNamespace(
				conf,
				namespace);

		job.setJarByClass(this.getClass());

		configure(job);

		if ((adapters != null) && (adapters.size() > 0)) {
			for (final DataAdapter<?> adapter : adapters) {
				GeoWaveInputFormat.addDataAdapter(
						conf,
						adapter);
			}
		}
		if ((indices != null) && (indices.size() > 0)) {
			for (final PrimaryIndex index : indices) {
				GeoWaveInputFormat.addIndex(
						conf,
						index);
			}
		}
		if (query != null) {
			GeoWaveInputFormat.setQuery(
					conf,
					query);
		}
		if (minInputSplits != null) {
			GeoWaveInputFormat.setMinimumSplitCount(
					conf,
					minInputSplits);
		}
		if (maxInputSplits != null) {
			GeoWaveInputFormat.setMaximumSplitCount(
					conf,
					maxInputSplits);
		}

		final boolean jobSuccess = job.waitForCompletion(true);

		return (jobSuccess) ? 0 : 1;
	}

	protected abstract void configure(
			Job job )
			throws Exception;

	public void setMaxInputSplits(
			final int maxInputSplits ) {
		this.maxInputSplits = maxInputSplits;
	}

	public void setMinInputSplits(
			final int minInputSplits ) {
		this.minInputSplits = minInputSplits;
	}

	public void addDataAdapter(
			final DataAdapter<?> adapter ) {
		adapters.add(adapter);
	}

	public void addIndex(
			final PrimaryIndex index ) {
		indices.add(index);
	}

	public void setQuery(
			final DistributableQuery query ) {
		this.query = query;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		return runOperation(args) ? 0 : -1;
	}

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		allOptions.addOptionGroup(baseOptionGroup);
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				allOptions,
				args,
				true);
		final CommandLineResult<DataStoreCommandLineOptions> dataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
				allOptions,
				commandLine);
		final DataStoreCommandLineOptions dataStoreOptions = dataStoreOptionsResult.getResult();
		dataStoreFactory = (DataStoreFactorySpi) dataStoreOptions.getFactory();
		configOptions = dataStoreOptions.getConfigOptions();
		namespace = dataStoreOptions.getNamespace();
		if (commandLine.hasOption("h")) {
			printHelp(allOptions);
			return true;
		}
		else {

			try {
				return runJob() == 0 ? true : false;
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to run job",
						e);
				throw new ParseException(
						e.getMessage());
			}
		}
	}

	private static void printHelp(
			final Options options ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"GeoWave MapReduce",
				"\nOptions:",
				options,
				"");
	}
}
