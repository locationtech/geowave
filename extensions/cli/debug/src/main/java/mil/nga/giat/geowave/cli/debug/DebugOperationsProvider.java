package mil.nga.giat.geowave.cli.debug;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;
import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class DebugOperationsProvider implements
		CLIOperationProviderSpi
{

	@Override
	public CLIOperation[] getOperations() {
		return new CLIOperation[] {
			new CLIOperation(
					"bbox",
					"bbox query",
					new BBOXQuery()),
			new CLIOperation(
					"clientCql",
					"cql client-side, primarily useful for consistency checking",
					new ClientSideCQLQuery()),
			new CLIOperation(
					"serverCql",
					"cql server-side",
					new CQLQuery()),
			new CLIOperation(
					"fullscan",
					"fulltable scan",
					new FullTableScan()),
			new CLIOperation(
					"fullscanMinimal",
					"full table scan without any iterators or deserialization",
					new MinimalFullTable()),
		};
	}

	@Override
	public CLIOperationCategory getCategory() {
		return new CustomOperationCategory(
				"scratch",
				"Random scratch operations",
				"scratchpad for geowave ops");
	}

}
