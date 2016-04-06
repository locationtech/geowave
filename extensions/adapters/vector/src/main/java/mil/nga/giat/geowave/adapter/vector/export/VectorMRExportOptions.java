package mil.nga.giat.geowave.adapter.vector.export;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private final VectorExportOptions generalExportOptions = new VectorExportOptions();
	private String hdfsOutputDirectory;

	// TODO set data store appropriately for input format
	// these options should go away, they are temporary place holders until
	// this gets merged with the new commandline tools
	public String dataStoreName = "";
	public Map<String, String> configOptions = new HashMap<String, String>();
	public String gwNamespace = "";

	public int getMinSplits() {
		return minSplits;
	}

	public int getMaxSplits() {
		return maxSplits;
	}

	public String getHdfsOutputDirectory() {
		return hdfsOutputDirectory;
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

	public void setHdfsHostPort(
			final String hdfsHostPort ) {
		this.hdfsHostPort = hdfsHostPort;
	}

	public void setResourceManagerHostPort(
			final String resourceManagerHostPort ) {
		this.resourceManagerHostPort = resourceManagerHostPort;
	}

	public void setMinSplits(
			final int minSplits ) {
		this.minSplits = minSplits;
	}

	public void setMaxSplits(
			final int maxSplits ) {
		this.maxSplits = maxSplits;
	}

	public void setHdfsOutputDirectory(
			final String hdfsOutputDirectory ) {
		this.hdfsOutputDirectory = hdfsOutputDirectory;
	}

	public void setCqlFilter(
			final String cqlFilter ) {
		generalExportOptions.setCqlFilter(cqlFilter);
	}

	public void setAdapterIds(
			final List<String> adapterIds ) {
		generalExportOptions.setAdapterIds(adapterIds);
	}

	public void setIndexId(
			final String indexId ) {
		generalExportOptions.setIndexId(indexId);
	}

	public void setDataStore(
			final DataStore dataStore ) {
		generalExportOptions.setDataStore(dataStore);
	}

	public void setAdapterStore(
			final AdapterStore adapterStore ) {
		generalExportOptions.setAdapterStore(adapterStore);
	}

	public void setIndexStore(
			final IndexStore indexStore ) {
		generalExportOptions.setIndexStore(indexStore);
	}

	public void setBatchSize(
			final int batchSize ) {
		generalExportOptions.setBatchSize(batchSize);
	}

}
