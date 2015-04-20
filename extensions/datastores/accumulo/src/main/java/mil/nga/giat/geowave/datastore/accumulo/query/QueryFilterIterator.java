package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

/**
 * This iterator wraps a DistributableQueryFilter which is deserialized from a
 * byte array passed as an option with a "filter" key. Also, the model is needed
 * to deserialize the row into a set of fields that can be used by the filter.
 * The model is deserialized from a byte array stored as an option with the key
 * "model". If either one of these serialized options are not successfully
 * found, this iterator will accept everything.
 */
public class QueryFilterIterator extends
		WholeRowIterator
{
	protected static final String QUERY_ITERATOR_NAME = "GEOWAVE_QUERY_FILTER";
	public static final String WHOLE_ROW_ITERATOR_NAME = "GEOWAVE_WHOLE_ROW_ITERATOR";
	protected static final int QUERY_ITERATOR_PRIORITY = 10;
	public static final int WHOLE_ROW_ITERATOR_PRIORITY = 10;
	protected static final String FILTER = "filter";
	protected static final String MODEL = "model";
	private DistributableQueryFilter filter;
	private CommonIndexModel model;

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((filter != null) && (model != null)) {
			final AccumuloRowId rowId = new AccumuloRowId(
					currentRow.getBytes());
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final ByteArrayId fieldId = new ByteArrayId(
						key.getColumnQualifierData().getBackingArray());
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(fieldId);
				if (reader == null) {
					continue;
				}
				final CommonIndexValue fieldValue = reader.readField(values.get(
						i).get());
				fieldValue.setVisibility(key.getColumnVisibilityData().getBackingArray());
				commonData.addValue(new PersistentValue<CommonIndexValue>(
						fieldId,
						fieldValue));
			}
			final IndexedPersistenceEncoding encoding = new IndexedPersistenceEncoding(
					new ByteArrayId(
							rowId.getAdapterId()),
					new ByteArrayId(
							rowId.getDataId()),
					new ByteArrayId(
							rowId.getInsertionId()),
					rowId.getNumberOfDuplicates(),
					commonData);
			return filter.accept(encoding);
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
		super.init(
				source,
				options,
				env);

		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + QueryFilterIterator.class.getName());
		}
		try {

			final String filterStr = options.get(FILTER);
			final byte[] filterBytes = ByteArrayUtils.byteArrayFromString(filterStr);
			filter = PersistenceUtils.fromBinary(
					filterBytes,
					DistributableQueryFilter.class);

			final String modelStr = options.get(MODEL);
			final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
			model = PersistenceUtils.fromBinary(
					modelBytes,
					CommonIndexModel.class);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

}
