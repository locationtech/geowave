package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

import org.apache.commons.lang3.tuple.Pair;

abstract public class HBaseQuery
{
	protected List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected Pair<List<String>, DataAdapter<?>> fieldIds;
	protected HBaseOptions options = null;

	protected final String[] authorizations;

	public HBaseQuery(
			final PrimaryIndex index,
			final String... authorizations ) {
		this(
				null,
				index,
				null,
				authorizations);
	}

	public HBaseQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final String... authorizations ) {
		this.adapterIds = adapterIds;
		this.index = index;
		this.fieldIds = fieldIds;
		this.authorizations = authorizations;
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}

	abstract protected List<ByteArrayRange> getRanges();

	public void setOptions(
			HBaseOptions options ) {
		this.options = options;
	}
}
