package mil.nga.giat.geowave.core.ingest;

import java.util.List;

import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This simply executes an operation to clear a given namespace. It will delete
 * all tables prefixed by the given namespace.
 */
public class ClearNamespaceDriver extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(ClearNamespaceDriver.class);
	protected DataStoreCommandLineOptions dataStoreOptions;
	protected IngestCommandLineOptions ingest;

	public ClearNamespaceDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptionsInternal(
			final Options options,
			CommandLine commandLine )
			throws ParseException {
		final CommandLineResult<DataStoreCommandLineOptions> dataStoreOptionsResult = DataStoreCommandLineOptions.parseOptions(
				options,
				commandLine);
		dataStoreOptions = dataStoreOptionsResult.getResult();
		if (dataStoreOptionsResult.isCommandLineChange()) {
			commandLine = dataStoreOptionsResult.getCommandLine();
		}
		ingest = IngestCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptionsInternal(
			final Options allOptions ) {
		DataStoreCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
	}

	@Override
	protected boolean runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		// just check if the flag to clear namespaces is set, and even if it is
		// not, clear it, but only if a namespace is provided
		if (!ingest.isClearNamespace()) {
			clearNamespace();
		}
		return true;
	}

	protected void clearNamespace() {
		// don't delete all tables in the case that no namespace is given
		if ((dataStoreOptions.getNamespace() != null) && !dataStoreOptions.getNamespace().isEmpty()) {
			LOGGER.info("deleting everything in namespace '" + dataStoreOptions.getNamespace() + "'");
			dataStoreOptions.createStore().delete(
					new QueryOptions(),
					new EverythingQuery());
		}
		else {
			LOGGER.error("cannot clear a namespace if no namespace is provided");
		}
	}
}
