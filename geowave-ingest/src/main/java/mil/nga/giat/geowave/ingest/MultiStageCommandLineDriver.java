package mil.nga.giat.geowave.ingest;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
	public void parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		for (final AbstractCommandLineDriver stage : orderedStages) {
			stage.parseOptions(commandLine);
		}
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		for (final AbstractCommandLineDriver stage : orderedStages) {
			stage.applyOptions(allOptions);
		}
	}

}
