package mil.nga.giat.geowave.adapter.vector.transaction;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class TransactionAllocationCLIOperationProvider implements
		CLIOperationProviderSpi
{

	@Override
	public CLIOperation[] getOperations() {
		return new CLIOperation[] {
			new CLIOperation(
					"zkTx",
					"Pre-allocate a set of transaction IDs in zookeeper to be used for WFS-T transactions",
					new TransactionAllocationCLIOperation())
		};
	}

	@Override
	public CLIOperationCategory getCategory() {
		return new CustomOperationCategory(
				"zk-transactions",
				"Zookeeper Transaction Allocation",
				"Pre-allocate a set of transaction IDs in zookeeper to be used for WFS-T transactions");
	}

}
