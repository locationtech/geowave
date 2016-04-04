package mil.nga.giat.geowave.adapter.vector.export;

import java.io.File;
import java.util.List;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class VectorLocalExportOptions
{
	// TODO annotate appropriately when new commandline tools is merged
	private VectorExportOptions generalExportOptions = new VectorExportOptions();
	private File outputFile = new File(
			"");

	public String getCqlFilter() {
		return generalExportOptions.getCqlFilter();
	}

	public List<String> getAdapterIds() {
		return generalExportOptions.getAdapterIds();
	}

	public String getIndexId() {
		return generalExportOptions.getIndexId();
	}

	public DataStore getDataStore() {
		return generalExportOptions.getDataStore();
	}

	public AdapterStore getAdapterStore() {
		return generalExportOptions.getAdapterStore();
	}

	public IndexStore getIndexStore() {
		return generalExportOptions.getIndexStore();
	}

	public int getBatchSize() {
		return generalExportOptions.getBatchSize();
	}

	public File getOutputFile() {
		return outputFile;
	}

}
