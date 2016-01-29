package mil.nga.giat.geowave.datastore.accumulo.split;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.cli.CustomOperationCategory;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class AccumuloSplitsOperationCLIProvider implements
		CLIOperationProviderSpi
{
	private static Logger LOGGER = LoggerFactory.getLogger(AccumuloSplitsOperationCLIProvider.class);

	@Override
	public CLIOperation[] getOperations() {
		return new CLIOperation[] {
			new CLIOperation(
					"splitquantile",
					"Set Accumulo splits by providing the number of partitions based on a quantile distribution strategy",
					new AbstractAccumuloSplitsOperation() {

						@Override
						protected boolean setSplits(
								final Connector connector,
								final PrimaryIndex index,
								final String namespace,
								final long number ) {
							try {
								AccumuloUtils.setSplitsByQuantile(
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
					}),
			new CLIOperation(
					"splitequalinterval",
					"Set Accumulo splits by providing the number of partitions based on an equal interval strategy",
					new AbstractAccumuloSplitsOperation() {

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
					}),
			new CLIOperation(
					"presplitpartitionid",
					"Pre-split Accumulo table by providing the number of partition IDs",
					new AbstractAccumuloSplitsOperation() {

						@Override
						protected boolean setSplits(
								final Connector connector,
								final PrimaryIndex index,
								final String namespace,
								final long number ) {
							try {
								AccumuloUtils.setSplitsByRandomPartitions(
										connector,
										namespace,
										index,
										(int) number);
							}
							catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
								LOGGER.error(
										"Error pre-splitting",
										e);
								return false;
							}
							return true;
						}

						@Override
						protected boolean isPreSplit() {
							return true;
						}

					}),
			new CLIOperation(
					"splitnumrecords",
					"Set Accumulo splits by providing the number of entries per split",
					new AbstractAccumuloSplitsOperation() {

						@Override
						protected boolean setSplits(
								final Connector connector,
								final PrimaryIndex index,
								final String namespace,
								final long number ) {
							try {
								AccumuloUtils.setSplitsByNumRows(
										connector,
										namespace,
										index,
										number);
							}
							catch (AccumuloException | AccumuloSecurityException | IOException | TableNotFoundException e) {
								LOGGER.error(
										"Error setting number of entry splits",
										e);
								return false;
							}
							return true;
						}
					})
		};
	}

	@Override
	public CLIOperationCategory getCategory() {
		return new CustomOperationCategory(
				"accsplits",
				"accumulo splits",
				"Utility operations to set accumulo splits");
	}

}
