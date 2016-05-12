package mil.nga.giat.geowave.datastore.accumulo.operations;

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
import mil.nga.giat.geowave.datastore.accumulo.split.AbstractAccumuloSplitsOperation;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

@GeowaveOperation(name = "splitequalinterval", parentOperation = AccumuloSection.class)
@Parameters(commandDescription = "Set Accumulo splits by providing the number of partitions based on an equal interval strategy")
public class SplitEqualIntervalCommand extends
		AbstractSplitsCommand implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SplitEqualIntervalCommand.class);

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
					AccumuloUtils.setSplitsByNumSplits(
							connector,
							namespace,
							index,
							(int) number);
				}
				catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
					LOGGER.error(
							"Error setting equal interval splits",
							e);
					return false;
				}
				return true;
			}
		}.runOperation();
	}
}
