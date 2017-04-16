package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreQuery;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

abstract public class HBaseQuery extends
		DataStoreQuery
{
	protected HBaseOptions options = null;

	public HBaseQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dataStore,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public HBaseQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				dataStore,
				adapterIds,
				index,
				fieldIds,
				visibilityCounts,
				authorizations);
	}

	public void setOptions(
			HBaseOptions options ) {
		this.options = options;
	}
}
