package mil.nga.giat.geowave.core.ingest;

import java.util.List;

import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloCommandLineOptions;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
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
	protected AccumuloCommandLineOptions accumulo;
	protected IngestCommandLineOptions ingest;
	protected IndexWriter indexWriter;

	public ClearNamespaceDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		accumulo = AccumuloCommandLineOptions.parseOptions(commandLine);
		ingest = IngestCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptionsInternal(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		IngestCommandLineOptions.applyOptions(allOptions);
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		// just check if the flag to clear namespaces is set, and even if it is
		// not, clear it, but only if a namespace is provided
		if (!ingest.isClearNamespace()) {
			try {
				clearNamespace();
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.fatal(
						"Unable to connect to Accumulo with the specified options",
						e);
			}
		}
	}

	protected void clearNamespace()
			throws AccumuloException,
			AccumuloSecurityException {
		// don't delete all tables in the case that no namespace is given
		if ((accumulo.getNamespace() != null) && !accumulo.getNamespace().isEmpty()) {
			LOGGER.info("deleting all tables prefixed by '" + accumulo.getNamespace() + "'");
			try {
				accumulo.getAccumuloOperations().deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
				LOGGER.error("Unable to clear accumulo namespace");
			}

		}
		else {
			LOGGER.error("cannot clear a namespace if no namespace is provided");
		}
	}
}
