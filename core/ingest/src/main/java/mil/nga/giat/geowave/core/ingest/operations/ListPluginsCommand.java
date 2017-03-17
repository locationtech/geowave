package mil.nga.giat.geowave.core.ingest.operations;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginRegistry;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeRegistry;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listplugins", parentOperation = IngestSection.class)
@Parameters(commandDescription = "List supported data store types, index types, and ingest formats")
public class ListPluginsCommand extends
		DefaultOperation implements
		Command
{

	@Override
	public void execute(
			OperationParams params ) {
		final PrintWriter pw = new PrintWriter(
				new OutputStreamWriter(
						System.out,
						StringUtils.GEOWAVE_CHAR_SET));
		pw.println("Available index types currently registered as plugins:\n");
		for (final Entry<String, DimensionalityTypeProviderSpi> pluginProviderEntry : DimensionalityTypeRegistry
				.getRegisteredDimensionalityTypes()
				.entrySet()) {
			final DimensionalityTypeProviderSpi pluginProvider = pluginProviderEntry.getValue();
			final String desc = pluginProvider.getDimensionalityTypeDescription() == null ? "no description"
					: pluginProvider.getDimensionalityTypeDescription();
			final String text = "  " + pluginProviderEntry.getKey() + ":\n    " + desc;
			pw.println(text);
			pw.println();
		}

		pw.println("Available ingest formats currently registered as plugins:\n");
		for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderEntry : IngestFormatPluginRegistry
				.getPluginProviderRegistry()
				.entrySet()) {
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
			final String desc = pluginProvider.getIngestFormatDescription() == null ? "no description" : pluginProvider
					.getIngestFormatDescription();
			final String text = "  " + pluginProviderEntry.getKey() + ":\n    " + desc;
			pw.println(text);
			pw.println();
		}
		pw.println("Available datastores currently registered:\n");
		final Map<String, StoreFactoryFamilySpi> dataStoreFactories = GeoWaveStoreFinder
				.getRegisteredStoreFactoryFamilies();
		for (final Entry<String, StoreFactoryFamilySpi> dataStoreFactoryEntry : dataStoreFactories.entrySet()) {
			final StoreFactoryFamilySpi dataStoreFactory = dataStoreFactoryEntry.getValue();
			final String desc = dataStoreFactory.getDescription() == null ? "no description" : dataStoreFactory
					.getDescription();
			final String text = "  " + dataStoreFactory.getType() + ":\n    " + desc;
			pw.println(text);
			pw.println();
		}
		pw.flush();
	}

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}

}