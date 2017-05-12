package mil.nga.giat.geowave.datastore.accumulo.query;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloCommonIndexedPersistenceEncoding;

public class QueryFilterIterator extends
		Filter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(QueryFilterIterator.class);
	protected static final String QUERY_ITERATOR_NAME = "GEOWAVE_QUERY_FILTER";
	protected static final int QUERY_ITERATOR_PRIORITY = 25;
	protected static final String FILTER = "filter";
	protected static final String MODEL = "model";
	private DistributableQueryFilter filter;
	protected CommonIndexModel model;
	protected Text currentRow = new Text();
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

			// HP Fortify "Access Specifier Manipulation"
			// This object is being modified by trusted code,
			// in a way that is not influenced by user input
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
	protected void findTop() {
		// it seems like the key can be cached and turns out to improve
		// performance a bit
		findTopEnhanced(
				getSource(),
				this);
	}

	protected static void findTopEnhanced(
			final SortedKeyValueIterator<Key, Value> source,
			final Filter filter ) {
		Key key;
		if (source.hasTop()) {
			key = source.getTopKey();
		}
		else {
			return;
		}
		while (!key.isDeleted() && !filter.accept(
				key,
				source.getTopValue())) {
			try {
				source.next();
				if (source.hasTop()) {
					key = source.getTopKey();
				}
				else {
					return;
				}
			}
			catch (final IOException e) {
				throw new RuntimeException(
						e);
			}
		}
	}

	@Override
	public boolean accept(
			final Key key,
			final Value value ) {
		if (isSet()) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

			final FlattenedUnreadData unreadData = aggregateFieldData(
					key,
					value,
					commonData);
			return applyRowFilter(
					key.getRow(currentRow),
					commonData,
					unreadData);
		}
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(
			final IteratorEnvironment env ) {
		final QueryFilterIterator iterator = new QueryFilterIterator();
		iterator.setSource(getSource().deepCopy(
				env));
		iterator.filter = filter;
		iterator.commonIndexFieldIds.addAll(commonIndexFieldIds);
		iterator.model = model;
		return iterator;
	}

	protected boolean applyRowFilter(
			final Text currentRow,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		return applyRowFilter(getEncoding(
				currentRow,
				commonData,
				unreadData));
	}

	protected static CommonIndexedPersistenceEncoding getEncoding(
			final Text currentRow,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		final GeowaveRowId rowId = new GeowaveRowId(
				currentRow.getBytes(),
				currentRow.getLength());
		return new AccumuloCommonIndexedPersistenceEncoding(
				new ByteArrayId(
						rowId.getAdapterId()),
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				commonData,
				unreadData);
	}

	protected boolean applyRowFilter(
			final CommonIndexedPersistenceEncoding encoding ) {
		return filter.accept(
				model,
				encoding);
	}

	protected FlattenedUnreadData aggregateFieldData(
			final Key key,
			final Value value,
			final PersistentDataset<CommonIndexValue> commonData ) {
		final ByteArrayId colQual = new ByteArrayId(
				key.getColumnQualifierData().getBackingArray());
		final byte[] valueBytes = value.get();
		final FlattenedDataSet dataSet = DataStoreUtils.decomposeFlattenedFields(
				colQual.getBytes(),
				valueBytes,
				key.getColumnVisibilityData().getBackingArray(),
				model.getDimensions().length - 1);
		final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
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
					LOGGER.error("Could not find reader for common index field: " + commonIndexFieldId.getString());
				}
			}
		}
		return dataSet.getFieldsDeferred();
	}

	public boolean isSet() {
		return (filter != null) && (model != null);
	}

	@Override
	public void init(
			final SortedKeyValueIterator<Key, Value> source,
			final Map<String, String> options,
			final IteratorEnvironment env )
			throws IOException {
		setOptions(options);
		super.init(
				source,
				options,
				env);
	}

	public void setOptions(
			final Map<String, String> options ) {
		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + QueryFilterIterator.class.getName());
		}
		try {
			if (options.containsKey(FILTER)) {
				final String filterStr = options.get(FILTER);
				final byte[] filterBytes = ByteArrayUtils.byteArrayFromString(filterStr);
				filter = PersistenceUtils.fromBinary(
						filterBytes,
						DistributableQueryFilter.class);
			}
			if (options.containsKey(MODEL)) {
				final String modelStr = options.get(MODEL);
				final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
				model = PersistenceUtils.fromBinary(
						modelBytes,
						CommonIndexModel.class);
				for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
					commonIndexFieldIds.add(numericDimension.getFieldId());
				}
			}
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
