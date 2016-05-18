package mil.nga.giat.geowave.core.store.operations.remote.options;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

/**
 * This is a convenience class to load the desired indexes from either
 * IndexPluginOptions or IndexGroupPluginOptions based on the name of the index
 * (not the type). It will load from config file.
 */
public class IndexLoader
{

	private final String indexName;

	private List<IndexPluginOptions> loadedIndexes;

	/**
	 * Constructor
	 */
	public IndexLoader(
			final String indexName ) {
		this.indexName = indexName;
	}

	/**
	 * Attempt to find an index group or index name in the config file with the
	 * given name.
	 * 
	 * @param configFile
	 * @return
	 */
	public boolean loadFromConfig(
			File configFile ) {

		loadedIndexes = new ArrayList<IndexPluginOptions>();

		// Properties (load them all)
		Properties props = ConfigOptions.loadProperties(
				configFile,
				null);

		// Is there a comma?
		String[] indexes = indexName.split(",");
		for (String index : indexes) {

			// Attempt to load as an index group first.
			IndexGroupPluginOptions indexGroupOptions = loadIndexGroupPluginOptions(
					props,
					index);

			// Attempt to load as an index next.
			IndexPluginOptions indexOptions = loadIndexPluginOptions(
					props,
					index);

			if (indexGroupOptions != null && indexOptions != null) {
				throw new ParameterException(
						"Aborting because there is both an index group " + "and index with the name: " + indexName);
			}
			else if (indexOptions != null) {
				loadedIndexes.add(indexOptions);
			}
			else if (indexGroupOptions != null) {
				loadedIndexes.addAll(indexGroupOptions.getDimensionalityPlugins().values());
			}
		}

		return loadedIndexes.size() != 0;
	}

	private IndexGroupPluginOptions loadIndexGroupPluginOptions(
			Properties props,
			String name ) {
		IndexGroupPluginOptions indexGroupPlugin = new IndexGroupPluginOptions();
		String indexGroupNamespace = IndexGroupPluginOptions.getIndexGroupNamespace(indexName);
		if (!indexGroupPlugin.load(
				props,
				indexGroupNamespace)) {
			return null;
		}
		return indexGroupPlugin;
	}

	private IndexPluginOptions loadIndexPluginOptions(
			Properties props,
			String name ) {
		IndexPluginOptions indexPlugin = new IndexPluginOptions();
		String indexNamespace = IndexPluginOptions.getIndexNamespace(indexName);
		if (!indexPlugin.load(
				props,
				indexNamespace)) {
			return null;
		}
		return indexPlugin;
	}

	public List<IndexPluginOptions> getLoadedIndexes() {
		return loadedIndexes;
	}

	public void addIndex(
			IndexPluginOptions option ) {
		if (this.loadedIndexes == null) {
			this.loadedIndexes = new ArrayList<IndexPluginOptions>();
		}
		this.loadedIndexes.add(option);
	}

	public void setLoadedIndexes(
			List<IndexPluginOptions> loadedIndexes ) {
		this.loadedIndexes = loadedIndexes;
	}

	public String getIndexName() {
		return indexName;
	}

}
