package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;

public class SecondaryIndexQueryFilterIterator extends
		RowFilter
{
	public static final String ITERATOR_NAME = "GEOWAVE_2ND_IDX_QUERY_FILTER";
	public static final int ITERATOR_PRIORITY = 50;
	public static final String FILTERS = "filters";
	public static final String PRIMARY_INDEX_ID = "primaryIndexId";
	private DistributableFilterList filters;
	private String primaryIndexId;

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		if ((options == null) || (!options.containsKey(FILTERS)) || (!options.containsKey(PRIMARY_INDEX_ID))) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + SecondaryIndexQueryFilterIterator.class.getName());
		}
		final String filterStr = options.get(FILTERS);
		final byte[] filterBytes = ByteArrayUtils.byteArrayFromString(filterStr);
		filters = PersistenceUtils.fromBinary(
				filterBytes,
				DistributableFilterList.class);
		primaryIndexId = options.get(PRIMARY_INDEX_ID);
	}

	@Override
	public boolean acceptRow(
			final SortedKeyValueIterator<Key, Value> rowIterator )
			throws IOException {
		if (filters != null) {
			while (rowIterator.hasTop()) {
				final Key key = rowIterator.getTopKey();
				final Value value = rowIterator.getTopValue();
				final String cq = StringUtils.stringFromBinary(key.getColumnQualifierData().getBackingArray());
				if (!cq.equals(primaryIndexId)) {
					final IndexedPersistenceEncoding<ByteArrayId> persistenceEncoding = new IndexedPersistenceEncoding<ByteArrayId>(
							null, // not needed
							null, // not needed
							null, // not needed
							0, // not needed
							new PersistentDataset<ByteArrayId>(
									new PersistentValue<ByteArrayId>(
											new ByteArrayId(
													key.getColumnQualifierData().getBackingArray()),
											new ByteArrayId(
													value.get()))),
							null);
					if (filters.accept(
							null,
							persistenceEncoding)) return true;
				}
				rowIterator.next();
			}
			return false;
		}
		// should not happen but if the filter is not sent to this iterator, it
		// will accept everything
		return true;
	}

}