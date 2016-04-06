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

	public void setCqlFilter(
			String cqlFilter ) {
		generalExportOptions.setCqlFilter(cqlFilter);
	}

	public void setAdapterIds(
			List<String> adapterIds ) {
		generalExportOptions.setAdapterIds(adapterIds);
	}

	public void setIndexId(
			String indexId ) {
		generalExportOptions.setIndexId(indexId);
	}

	public void setDataStore(
			DataStore dataStore ) {
		generalExportOptions.setDataStore(dataStore);
	}

	public void setAdapterStore(
			AdapterStore adapterStore ) {
		generalExportOptions.setAdapterStore(adapterStore);
	}

	public void setIndexStore(
			IndexStore indexStore ) {
		generalExportOptions.setIndexStore(indexStore);
	}

	public void setBatchSize(
			int batchSize ) {
		generalExportOptions.setBatchSize(batchSize);
	}

	public void setOutputFile(
			File outputFile ) {
		this.outputFile = outputFile;
	}

}
