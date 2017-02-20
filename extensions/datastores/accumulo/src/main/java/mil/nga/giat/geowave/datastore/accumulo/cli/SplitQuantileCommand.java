package mil.nga.giat.geowave.datastore.accumulo.cli;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.split.AbstractAccumuloSplitsOperation;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

@GeowaveOperation(name = "splitquantile", parentOperation = AccumuloSection.class)
@Parameters(commandDescription = "Set Accumulo splits by providing the number of partitions based on a quantile distribution strategy")
public class SplitQuantileCommand extends
		AbstractSplitsCommand implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SplitQuantileCommand.class);

	@Override
	public void doSplit()
			throws Exception {

		new AbstractAccumuloSplitsOperation(
				inputStoreOptions,
				splitOptions) {

			@Override
			protected boolean setSplits(
					final Connector connector,
					final PrimaryIndex index,
					final String namespace,
					final long number ) {
				try {
					AccumuloUtils.setSplitsByQuantile(
							(AccumuloDataStore) inputStoreOptions.createDataStore(),
							connector,
							namespace,
							index,
							(int) number);
				}
				catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
					LOGGER.error(
							"Error setting quantile splits",
							e);
					return false;
				}
				return true;
			}
		}.runOperation();
	}
}
