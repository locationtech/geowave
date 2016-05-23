package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloCommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloFieldInfo;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

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
	private final static Logger LOGGER = Logger.getLogger(QueryFilterIterator.class);
	protected static final String QUERY_ITERATOR_NAME = "GEOWAVE_QUERY_FILTER";
	public static final String WHOLE_ROW_ITERATOR_NAME = "GEOWAVE_WHOLE_ROW_ITERATOR";
	protected static final int QUERY_ITERATOR_PRIORITY = 10;
	public static final int WHOLE_ROW_ITERATOR_PRIORITY = 10;
	protected static final String FILTER = "filter";
	protected static final String MODEL = "model";
	private DistributableQueryFilter filter;
	private CommonIndexModel model;
	private final List<ByteArrayId> commonIndexFieldIds = new ArrayList<>();

	static {
		initialize();
	}

	private static void initialize() {
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		}
		catch (final Error factoryError) {
			String type = "";
			Field f = null;
			try {
				f = URL.class.getDeclaredField("factory");
			}
			catch (final NoSuchFieldException e) {
				LOGGER
						.error(
								"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
								e);
				throw (factoryError);
			}
			f.setAccessible(true);
			Object o;
			try {
				o = f.get(null);
			}
			catch (final IllegalAccessException e) {
				LOGGER
						.error(
								"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
								e);
				throw (factoryError);
			}
			if (o instanceof FsUrlStreamHandlerFactory) {
				LOGGER
						.info("setURLStreamHandlerFactory already set on this JVM to FsUrlStreamHandlerFactory.  Nothing to do");
				return;
			}
			else {
				type = o.getClass().getCanonicalName();
			}
			LOGGER
					.error("URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to: "
							+ type);
			throw (factoryError);
		}
	}

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((filter != null) && (model != null)) {
			final AccumuloRowId rowId = new AccumuloRowId(
					currentRow.getBytes());
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final List<AccumuloFieldInfo> unknownData = new ArrayList<AccumuloFieldInfo>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final ByteArrayId colQual = new ByteArrayId(
						key.getColumnQualifierData().getBackingArray());
				final byte[] valueBytes = values.get(
						i).get();
				final List<AccumuloFieldInfo> fieldInfos = AccumuloUtils.decomposeFlattenedFields(
						colQual.getBytes(),
						valueBytes,
						key.getColumnVisibilityData().getBackingArray());
				for (final AccumuloFieldInfo fieldInfo : fieldInfos) {
					final int ordinal = fieldInfo.getFieldPosition();
					if (ordinal < model.getDimensions().length) {
						final ByteArrayId commonIndexFieldId = commonIndexFieldIds.get(ordinal);
						final FieldReader<? extends CommonIndexValue> reader = model.getReader(commonIndexFieldId);
						if (reader != null) {
							final CommonIndexValue fieldValue = reader.readField(fieldInfo.getValue());
							fieldValue.setVisibility(key.getColumnVisibility().getBytes());
							commonData.addValue(new PersistentValue<CommonIndexValue>(
									commonIndexFieldId,
									fieldValue));
						}
						else {
							LOGGER.error("Could not find reader for common index field: "
									+ commonIndexFieldId.getString());
						}
					}
					else {
						unknownData.add(fieldInfo);
					}
				}
			}
			final CommonIndexedPersistenceEncoding encoding = new AccumuloCommonIndexedPersistenceEncoding(
					new ByteArrayId(
							rowId.getAdapterId()),
					new ByteArrayId(
							rowId.getDataId()),
					new ByteArrayId(
							rowId.getInsertionId()),
					rowId.getNumberOfDuplicates(),
					commonData,
					unknownData);
			return accept(
					currentRow,
					keys,
					values,
					model,
					encoding);
		}
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	protected boolean accept(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values,
			final CommonIndexModel model,
			final CommonIndexedPersistenceEncoding encoding ) {
		return filter.accept(
				model,
				encoding);
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
			for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
				commonIndexFieldIds.add(numericDimension.getFieldId());
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

}
