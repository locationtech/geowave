/**
 * 
 */
package mil.nga.giat.geowave.core.ingest;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.datastore.hbase.HBaseCommandLineOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> ClearNamespaceDriver </code>
 */
public class ClearNamespaceHBaseDriver extends
		AbstractIngestHBaseCommandLineDriver
{

	private final static Logger LOGGER = Logger.getLogger(ClearNamespaceHBaseDriver.class);
	protected HBaseCommandLineOptions hbase;
	protected IngestCommandLineOptions ingest;
	protected IndexWriter indexWriter;

	public ClearNamespaceHBaseDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	public void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		hbase = HBaseCommandLineOptions.parseOptions(commandLine);
		ingest = IngestCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void applyOptionsInternal(
			final Options allOptions ) {
		HBaseCommandLineOptions.applyOptions(allOptions);
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
			catch (IOException e) {
				LOGGER.fatal(
						"Unable to connect to Accumulo with the specified options",
						e);
			}
		}
	}

	protected void clearNamespace()
			throws IOException {
		// don't delete all tables in the case that no namespace is given
		if ((hbase.getNamespace() != null) && !hbase.getNamespace().isEmpty()) {
			LOGGER.info("deleting all tables prefixed by '" + hbase.getNamespace() + "'");
			try {
				hbase.getOperations().deleteAll();
			}
			catch (IOException e) {
				LOGGER.error("Unable to clear hbase namespace");
			}

		}
		else {
			LOGGER.error("cannot clear a namespace if no namespace is provided");
		}
	}

}
