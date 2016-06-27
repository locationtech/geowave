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
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloDataSet;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloFieldInfo;
import mil.nga.giat.geowave.datastore.accumulo.encoding.AccumuloUnreadData;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

public class QueryFilterIterator extends
		Filter
{
	private final static Logger LOGGER = Logger.getLogger(QueryFilterIterator.class);
	protected static final String QUERY_ITERATOR_NAME = "GEOWAVE_QUERY_FILTER";
	protected static final int QUERY_ITERATOR_PRIORITY = 10;
	protected static final String FILTER = "filter";
	protected static final String MODEL = "model";
	private DistributableQueryFilter filter;
	protected CommonIndexModel model;
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
	public boolean accept(
			final Key key,
			final Value value ) {
		if (isSet()) {
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

			final AccumuloUnreadData unreadData = aggregateFieldData(
					key,
					value,
					commonData);
			return applyRowFilter(
					key.getRow(),
					commonData,
					unreadData);
		}
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	protected boolean applyRowFilter(
			final Text currentRow,
			final PersistentDataset<CommonIndexValue> commonData,
			final AccumuloUnreadData unreadData ) {
		return applyRowFilter(getEncoding(
				currentRow,
				commonData,
				unreadData));
	}

	protected static CommonIndexedPersistenceEncoding getEncoding(
			final Text currentRow,
			final PersistentDataset<CommonIndexValue> commonData,
			final AccumuloUnreadData unreadData ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				currentRow.getBytes());
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

	protected AccumuloUnreadData aggregateFieldData(
			final Key key,
			final Value value,
			final PersistentDataset<CommonIndexValue> commonData ) {
		final ByteArrayId colQual = new ByteArrayId(
				key.getColumnQualifierData().getBackingArray());
		final byte[] valueBytes = value.get();
		final AccumuloDataSet dataSet = AccumuloUtils.decomposeFlattenedFields(
				colQual.getBytes(),
				valueBytes,
				key.getColumnVisibilityData().getBackingArray(),
				model.getDimensions().length - 1);
		final List<AccumuloFieldInfo> fieldInfos = dataSet.getFieldsRead();
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
