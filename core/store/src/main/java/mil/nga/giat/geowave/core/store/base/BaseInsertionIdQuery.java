package mil.nga.giat.geowave.core.store.base;

import java.util.Collections;

import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;

/**
 * Represents a query operation for a specific set of row IDs.
 *
 */
class BaseInsertionIdQuery<T> extends
		BaseConstraintsQuery
{
	private final QueryRanges ranges;

	public BaseInsertionIdQuery(
			final InternalDataAdapter<?> adapter,
			final PrimaryIndex index,
			final InsertionIdQuery query,
			final ScanCallback<T, ?> scanCallback,
			final DedupeFilter dedupeFilter,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				Collections.<Short> singletonList(adapter.getInternalAdapterId()),
				index,
				query,
				dedupeFilter,
				scanCallback,
				null,
				null,
				null,
				null,
				visibilityCounts,
				authorizations);
		this.ranges = new InsertionIds(
				query.getPartitionKey(),
				Lists.newArrayList(query.getSortKey())).asQueryRanges();
	}

	@Override
	protected QueryRanges getRanges() {
		return ranges;
	}
}