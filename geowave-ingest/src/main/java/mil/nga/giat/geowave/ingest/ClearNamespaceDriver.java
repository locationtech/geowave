package mil.nga.giat.geowave.ingest;

import java.util.List;

import mil.nga.giat.geowave.store.IndexWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * This simply executes an operation to clear a given namespace. It will delete
 * all tables prefixed by the given namespace.
 */
public class ClearNamespaceDriver extends
		AbstractCommandLineDriver
{
	protected AccumuloCommandLineOptions accumulo;
	protected IndexWriter indexWriter;

	public ClearNamespaceDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		accumulo = AccumuloCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestTypePluginProviderSpi<?, ?>> pluginProviders ) {
		// just check if the flag to clear namespaces is set, and even if it is
		// not, clear it, but only if a namespace is provided
		if (!accumulo.isClearNamespace()) {
			accumulo.clearNamespace();
		}
	}
}
