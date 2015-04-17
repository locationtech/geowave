package mil.nga.giat.geowave.analytics.tools;

import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytics.clustering.runners.MultiLevelJumpKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytics.clustering.runners.MultiLevelKMeansClusteringJobRunner;
import mil.nga.giat.geowave.analytics.mapreduce.nn.NNJobRunner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

/**
 * 
 * A tool, similar to ingest, to run all the analytics.
 * 
 */
public class AnalyticsMain
{
	public static enum AnalyticRunner {
		KMeansParallel(
				"kmeans-parallel",
				"KMeans Parallel Clustering",
				new MultiLevelKMeansClusteringJobRunner()),
		NN(
				"nn",
				"Nearest Neighbors",
				new NNJobRunner()),
		KMeansJump(
				"kmeans-jump",
				"KMeans Clustering using Jump Method",
				new MultiLevelJumpKMeansClusteringJobRunner());

		private AnalyticRunner(
				final String commandlineOptionValue,
				final String description,
				final IndependentJobRunner runner ) {
			this.commandlineOptionValue = commandlineOptionValue;
			this.description = description;
			this.runner = runner;
		}

		private final String commandlineOptionValue;
		private final String description;
		private final IndependentJobRunner runner;

		public String getCommandlineOptionValue() {
			return commandlineOptionValue;
		}

		public String getDescription() {
			return description;
		}

		public IndependentJobRunner getRunner() {
			return runner;
		}
	}

	public static AnalyticRunner getRunner(
			final String argValue ) {
		for (final AnalyticRunner o : AnalyticRunner.values()) {
			if (argValue.equalsIgnoreCase(o.commandlineOptionValue)) {
				return o;
			}
		}
		try {
			return (AnalyticRunner) Class.forName(
					argValue).newInstance();
		}
		catch (InstantiationException e) {
			System.err.println("Cannot not instantiate " + argValue);
		}
		catch (IllegalAccessException e) {
			System.err.println("Cannot not access " + argValue);
		}
		catch (ClassNotFoundException | ClassCastException e) {
			System.err.println(argValue + " is neither a known service or viable AnalyticRunner class");
		}
		return null;
	}

	private static void printHelp(
			final String args[] ) {
		System.err.println("Invalid operation: '" + args[0] + "'");
		System.err.println("Arguments must be in the following order: <operation> <options>");
		System.err.println("<operation> is one of the following.");
		for (final AnalyticRunner o : AnalyticRunner.values()) {
			System.err.println("\t" + o.getCommandlineOptionValue());
		}
	}

	public static void main(
			final String args[] )
			throws Exception {
		final AnalyticRunner runner = getRunner(args[0]);
		if (runner == null) {
			printHelp(args);
		}
		else {
			final String[] strippedArgs = new String[args.length - 1];
			System.arraycopy(
					args,
					1,
					strippedArgs,
					0,
					strippedArgs.length);
			runJob(
					runner.getRunner(),
					strippedArgs);
		}

	}

	private static void runJob(
			final IndependentJobRunner jobRunner,
			final String args[] )
			throws Exception {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		options.addOptionGroup(baseOptionGroup);

		final Set<Option> optionSet = new HashSet<Option>();
		jobRunner.fillOptions(optionSet);
		for (final Option option : optionSet) {
			options.addOption(option);
		}

		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				args);
		if (commandLine.hasOption("h")) {
			printHelp(options);
			System.exit(0);
		}
		else {
			final PropertyManagement pm = new PropertyManagement();
			pm.buildFromOptions(commandLine);
			jobRunner.run(pm);
		}
	}

	private static void printHelp(
			final Options options ) {
		final HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(
				"Analytics",
				"\nOptions:",
				options,
				"");
	}
}
