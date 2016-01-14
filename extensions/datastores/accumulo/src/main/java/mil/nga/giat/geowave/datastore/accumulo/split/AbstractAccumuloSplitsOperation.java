package mil.nga.giat.geowave.datastore.accumulo.split;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractAccumuloSplitsOperation implements
		CLIOperationDriver
{
	private static Logger LOGGER = LoggerFactory.getLogger(AbstractAccumuloSplitsOperation.class);

	@Override
	public boolean runOperation(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(allOptions);
		SplitCommandLineOptions.applyOptions(allOptions);
		final CommandLine commandLine = new BasicParser().parse(
				allOptions,
				args);
		final SplitCommandLineOptions splitOptions = SplitCommandLineOptions.parseOptions(commandLine);
		final AccumuloCommandLineOptions accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		try {
			final IndexStore indexStore = new AccumuloIndexStore(
					accumuloOptions.getAccumuloOperations());
			final Connector connector = accumuloOptions.getAccumuloOperations().getConnector();
			final String namespace = accumuloOptions.getNamespace();
			final long number = splitOptions.getNumber();
			if (splitOptions.getIndexId() == null) {
				boolean retVal = false;
				try (CloseableIterator<Index<?, ?>> indices = indexStore.getIndices()) {
					if (indices.hasNext()) {
						retVal = true;
					}
					while (indices.hasNext()) {
						final Index index = indices.next();
						if (index instanceof PrimaryIndex) {
							if (!setSplits(
									connector,
									(PrimaryIndex) index,
									namespace,
									number)) {
								retVal = false;
							}
						}
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"unable to close index store",
							e);
					return false;
				}
				if (!retVal) {
					LOGGER.error("no indices were successfully split, try providing an indexId");
				}
				return retVal;
			}
			else if (isPreSplit()) {
				setSplits(
						connector,
						new NullIndex(
								splitOptions.getIndexId()),
						namespace,
						number);
			}
			else {
				final Index index = indexStore.getIndex(new ByteArrayId(
						splitOptions.getIndexId()));
				if (index == null) {
					LOGGER.error("index '" + splitOptions.getIndexId() + "' does not exist; unable to create splits");
				}
				if (!(index instanceof PrimaryIndex)) {
					LOGGER.error("index '" + splitOptions.getIndexId() + "' is not a primary index; unable to create splits");
				}
				return setSplits(
						connector,
						(PrimaryIndex) index,
						namespace,
						number);
			}
		}
		catch (final AccumuloSecurityException | AccumuloException e) {
			LOGGER.error(
					"unable to create index store",
					e);
			return false;
		}
		return true;
	}

	protected boolean isPreSplit() {
		return false;
	};

	abstract protected boolean setSplits(
			Connector connector,
			PrimaryIndex index,
			String namespace,
			long number );
}
