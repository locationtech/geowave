package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloUnreadDataList;

/**
 * This iterator wraps a DistributableQueryFilter which is deserialized from a
 * byte array passed as an option with a "filter" key. Also, the model is needed
 * to deserialize the row into a set of fields that can be used by the filter.
 * The model is deserialized from a byte array stored as an option with the key
 * "model". If either one of these serialized options are not successfully
 * found, this iterator will accept everything.
 */
public class WholeRowQueryFilterIterator extends
		WholeRowIterator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WholeRowQueryFilterIterator.class);
	protected QueryFilterIterator queryFilterIterator;

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((queryFilterIterator != null) && queryFilterIterator.isSet()) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final List<FlattenedUnreadData> unreadData = new ArrayList<>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final Value value = values.get(i);
				queryFilterIterator.aggregateFieldData(
						key,
						value,
						commonData);
			}
			return queryFilterIterator.applyRowFilter(
					currentRow,
					commonData,
					unreadData.isEmpty() ? null : new AccumuloUnreadDataList(
							unreadData));
		}
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		queryFilterIterator = new QueryFilterIterator();
		queryFilterIterator.setOptions(options);
		super.init(
				source,
				options,
				env);
	}

}
