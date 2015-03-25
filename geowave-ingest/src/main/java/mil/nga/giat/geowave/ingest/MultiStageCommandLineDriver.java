package mil.nga.giat.geowave.ingest;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * This command-line driver wraps a list of ordered stages as drivers and
 * executes them in order. For example, it is used by the HDFS ingest process to
 * first stage intermediate data to HDFS and then to ingest it.
 */
public class MultiStageCommandLineDriver extends
AbstractCommandLineDriver
{
	private final AbstractCommandLineDriver[] orderedStages;

	public MultiStageCommandLineDriver(
			final String operation,
			final AbstractCommandLineDriver[] orderedStages ) {
		super(
				operation);
		this.orderedStages = orderedStages;
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {
		for (final AbstractCommandLineDriver stage : orderedStages) {
			stage.runInternal(
					args,
					pluginProviders);
		}
	}

	@Override
	public void parseOptionsInternal(
			final CommandLine commandLine )
					throws ParseException {
		for (final AbstractCommandLineDriver stage : orderedStages) {
			stage.parseOptionsInternal(commandLine);
		}
	}

	@Override
	public void applyOptionsInternal(
			final Options allOptions ) {
		for (final AbstractCommandLineDriver stage : orderedStages) {
			stage.applyOptionsInternal(allOptions);
		}
	}

}
