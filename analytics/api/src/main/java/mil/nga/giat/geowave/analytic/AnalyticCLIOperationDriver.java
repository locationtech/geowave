package mil.nga.giat.geowave.analytic;

import java.util.Collection;

import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticCLIOperationDriver implements
		CLIOperationDriver
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticCLIOperationDriver.class);
	private final IndependentJobRunner jobRunner;

	public AnalyticCLIOperationDriver(
			final IndependentJobRunner jobRunner ) {
		super();
		this.jobRunner = jobRunner;
	}

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Options options = new Options();
		final OptionGroup baseOptionGroup = new OptionGroup();
		baseOptionGroup.setRequired(false);
		baseOptionGroup.addOption(new Option(
				"h",
				"help",
				false,
				"Display help"));
		options.addOptionGroup(baseOptionGroup);

		final Collection<ParameterEnum<?>> params = jobRunner.getParameters();

		for (final ParameterEnum<?> param : params) {
			final Option[] paramOptions = param.getHelper().getOptions();
			for (final Option o : paramOptions) {
				options.addOption(o);
			}
		}

		final BasicParser parser = new BasicParser();

		Exception exception = null;
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(
					options,
					args,
					true);
		}
		catch (final Exception e) {
			exception = e;
		}
		try {
			final PropertyManagement properties = new PropertyManagement();
			// if the command-line changes on the first parameter, we do not
			// need to reparse
			boolean first = true;
			boolean newCommandLine = false;
			do {
				if (commandLine != null && commandLine.hasOption("h")) {
					printHelp(options);
					return true;
				}
				if (!params.isEmpty()) {
					newCommandLine = false;
					exception = null;
					first = true;
					for (final ParameterEnum<?> param : params) {
						CommandLineResult value = null;
						try {
							value = param.getHelper().getValue(
									options,
									commandLine);
						}
						catch (final Exception e) {
							exception = e;
						}
						if ((value != null) && value.isCommandLineChange()) {
							commandLine = value.getCommandLine();
							if (!first) {
								newCommandLine = true;
								break;
							}
						}
						first = false;
						if (value != null) {
							((ParameterEnum<Object>) param).getHelper().setValue(
									properties,
									value.getResult());
						}
					}
				}
			}
			while (newCommandLine);
			if (exception != null) {
				throw exception;
			}
			return jobRunner.run(properties) >= 0;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to run analytic job",
					e);
			return false;
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
