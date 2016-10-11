package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

abstract public class HBaseQuery
{

	private final static Logger LOGGER = Logger.getLogger(HBaseQuery.class);
	protected List<ByteArrayId> adapterIds;
	protected final PrimaryIndex index;
	protected Pair<List<String>, DataAdapter<?>> fieldIds;

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

}
