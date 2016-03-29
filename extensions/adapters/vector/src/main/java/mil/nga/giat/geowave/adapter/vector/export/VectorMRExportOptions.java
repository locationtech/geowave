package mil.nga.giat.geowave.adapter.vector.export;

import java.util.List;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class VectorMRExportOptions
{
	// TODO annotate appropriately when new commandline tools is merged
	private String hdfsHostPort;
	private String resourceManagerHostPort;
	private int minSplits;
	private int maxSplits;
	private VectorExportOptions generalExportOptions;
	private String hdfsOutputFile;

	public int getMinSplits() {
		return minSplits;
	}

	public int getMaxSplits() {
		return maxSplits;
	}

	public String getHdfsOutputFile() {
		return hdfsOutputFile;
	}

	public String getHdfsHostPort() {
		return hdfsHostPort;
	}

	public String getResourceManagerHostPort() {
		return resourceManagerHostPort;
	}

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

}
