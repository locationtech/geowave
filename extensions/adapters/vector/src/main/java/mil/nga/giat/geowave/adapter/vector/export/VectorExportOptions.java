package mil.nga.giat.geowave.adapter.vector.export;

import java.util.List;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class VectorExportOptions
{
	// TODO annotate appropriately when new commandline tools is merged
	protected static final int DEFAULT_BATCH_SIZE = 10000;
	private String cqlFilter;
	private List<String> adapterIds;
	private String indexId;
	private int batchSize;

	public String getCqlFilter() {
		return cqlFilter;
	}

	public List<String> getAdapterIds() {
		return adapterIds;
	}

	public String getIndexId() {
		return indexId;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setCqlFilter(
			String cqlFilter ) {
		this.cqlFilter = cqlFilter;
	}

	public void setAdapterIds(
			List<String> adapterIds ) {
		this.adapterIds = adapterIds;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public void setBatchSize(
			int batchSize ) {
		this.batchSize = batchSize;
	}
}
