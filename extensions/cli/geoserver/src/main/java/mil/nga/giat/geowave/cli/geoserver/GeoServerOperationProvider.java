package mil.nga.giat.geowave.cli.geoserver;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class GeoServerOperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		GeoServerSection.class,
		ConfigGeoServerCommand.class,
		GeoServerAddLayerCommand.class,
		GeoServerListWorkspacesCommand.class,
		GeoServerAddWorkspaceCommand.class,
		GeoServerRemoveWorkspaceCommand.class,
		GeoServerListDatastoresCommand.class,
		GeoServerGetDatastoreCommand.class,
		GeoServerAddDatastoreCommand.class,
		GeoServerRemoveDatastoreCommand.class,
		GeoServerListFeatureLayersCommand.class,
		GeoServerGetFeatureLayerCommand.class,
		GeoServerAddFeatureLayerCommand.class,
		GeoServerRemoveFeatureLayerCommand.class,
		GeoServerListCoverageStoresCommand.class,
		GeoServerGetCoverageStoreCommand.class,
		GeoServerAddCoverageStoreCommand.class,
		GeoServerRemoveCoverageStoreCommand.class,
		GeoServerListCoveragesCommand.class,
		GeoServerGetCoverageCommand.class,
		GeoServerAddCoverageCommand.class,
		GeoServerRemoveCoverageCommand.class,
		GeoServerGetStoreAdapterCommand.class,
		GeoServerAddStyleCommand.class,
		GeoServerGetStyleCommand.class,
		GeoServerListStylesCommand.class,
		GeoServerRemoveStyleCommand.class,
		GeoServerSetLayerStyleCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}
