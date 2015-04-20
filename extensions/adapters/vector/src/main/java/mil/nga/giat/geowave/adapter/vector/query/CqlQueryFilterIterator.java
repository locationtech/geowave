package mil.nga.giat.geowave.adapter.vector.query;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.*;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.geotools.factory.GeoTools;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

/**
 * This class extends from the WholeRowIterator (encapsulating an entire row of
 * key/value pairs into a single key/value pair) but also provides CQL filtering
 * on the tablet servers. It requires a FeatureDataAdapter to interpret whole
 * rows as SimpleFeatures and it will decode a CQL filter string into a query
 * filter to check acceptance with each SimpleFeature within an AccumuloIterator
 * and skip the row if it is not accepted.
 * 
 */
public class CqlQueryFilterIterator extends
		WholeRowIterator
{
	private static final Logger LOGGER = Logger.getLogger(CqlQueryFilterIterator.class);
	private static final Object MUTEX = new Object();
	private static boolean classLoaderInitialized = false;
	public static final String CQL_QUERY_ITERATOR_NAME = "GEOWAVE_CQL_QUERY_FILTER";
	public static final int CQL_QUERY_ITERATOR_PRIORITY = 10;
	public static final String CQL_FILTER = "cql_filter";
	public static final String GEOWAVE_FILTER = "geowave_filter";
	public static final String DATA_ADAPTER = "data_adapter";
	public static final String MODEL = "model";
	private CommonIndexModel model;
	private FeatureDataAdapter dataAdapter;
	private DistributableQueryFilter geowaveFilter;
	private Filter gtFilter;

	static {
		initialize();
	}

	private static void initialize() {
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		}
		catch (Error factoryError) {
			String type = "";
			Field f = null;
			try {
				f = URL.class.getDeclaredField("factory");
			}
			catch (NoSuchFieldException e) {
				LOGGER.error(
						"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
						e);
				throw (factoryError);
			}
			f.setAccessible(true);
			Object o;
			try {
				o = f.get(null);
			}
			catch (IllegalAccessException e) {
				LOGGER.error(
						"URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to;  additionally unable to discover type of Factory",
						e);
				throw (factoryError);
			}
			if (o instanceof FsUrlStreamHandlerFactory) {
				LOGGER.info("setURLStreamHandlerFactory already set on this JVM to FsUrlStreamHandlerFactory.  Nothing to do");
				return;
			}
			else {
				type = o.getClass().getCanonicalName();
			}
			LOGGER.error("URL.setURLStreamHandlerFactory() can only be called once per JVM instance, and currently something has set it to: " + type);
			throw (factoryError);
		}
	}

	@Override
	protected boolean filter(
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		if ((gtFilter != null) && (model != null) && (dataAdapter != null)) {
			final AccumuloRowId rowId = new AccumuloRowId(
					currentRow.copyBytes());
			final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();
			final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
			for (int i = 0; (i < keys.size()) && (i < values.size()); i++) {
				final Key key = keys.get(i);
				final ByteArrayId fieldId = new ByteArrayId(
						key.getColumnQualifierData().getBackingArray());
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(fieldId);
				if (reader == null) {
					// try extended data
					final FieldReader<Object> extReader = dataAdapter.getReader(fieldId);
					if (extReader == null) {
						continue;
					}
					final Object fieldValue = extReader.readField(values.get(
							i).get());
					extendedData.addValue(new PersistentValue<Object>(
							fieldId,
							fieldValue));
				}
				else {
					final CommonIndexValue fieldValue = reader.readField(values.get(
							i).get());
					fieldValue.setVisibility(key.getColumnVisibilityData().getBackingArray());
					commonData.addValue(new PersistentValue<CommonIndexValue>(
							fieldId,
							fieldValue));
				}
			}
			final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
					new ByteArrayId(
							rowId.getAdapterId()),
					new ByteArrayId(
							rowId.getDataId()),
					new ByteArrayId(
							rowId.getInsertionId()),
					rowId.getNumberOfDuplicates(),
					commonData,
					extendedData);
			if (geowaveFilter != null) {
				if (!geowaveFilter.accept(encoding)) {
					return false;
				}
			}
			final SimpleFeature feature = dataAdapter.decode(
					encoding,
					new Index(
							null, // because we know the feature data adapter
									// doesn't use the numeric index strategy
									// and only the common index model to decode
									// the simple feature, we pass along a null
									// strategy to eliminate the necessity to
									// send a serialization of the strategy in
									// the options of this iterator
							model));
			if (feature == null) {
				return false;
			}
			return evaluateFeature(
					gtFilter,
					feature,
					currentRow,
					keys,
					values);
		}
		return defaultFilterResult();
	}

	protected void setSource(
			final SortedKeyValueIterator<Key, Value> source ) {
		try {
			super.init(
					source,
					null,
					null);
		}
		catch (final IOException e) {
			throw new IllegalArgumentException(
					e);
		}
	}

	protected boolean defaultFilterResult() {
		// if the query filter or index model did not get sent to this iterator,
		// it'll just have to accept everything
		return true;
	}

	protected boolean evaluateFeature(
			final Filter filter,
			final SimpleFeature feature,
			final Text currentRow,
			final List<Key> keys,
			final List<Value> values ) {
		return filter.evaluate(feature);
	}

	public static void initClassLoader(
			@SuppressWarnings("rawtypes")
			final Class cls )
			throws MalformedURLException {
		synchronized (MUTEX) {
			if (classLoaderInitialized) {
				return;
			}
			LOGGER.info("Generating patched classloader");
			if (cls.getClassLoader() instanceof VFSClassLoader) {
				final VFSClassLoader cl = (VFSClassLoader) cls.getClassLoader();
				final FileObject[] fileObjs = cl.getFileObjects();
				final URL[] fileUrls = new URL[fileObjs.length];
				for (int i = 0; i < fileObjs.length; i++) {
					fileUrls[i] = new URL(
							fileObjs[i].toString());
				}
				GeoTools.addClassLoader(java.security.AccessController.doPrivileged(new java.security.PrivilegedAction<URLClassLoader>() {
					public URLClassLoader run() {
						final URLClassLoader ucl = new URLClassLoader(
								fileUrls,
								cl);
						return ucl;
					}
				}));

			}
			classLoaderInitialized = true;
		}

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
		initClassLoader(getClass());

		if (options == null) {
			throw new IllegalArgumentException(
					"Arguments must be set for " + CqlQueryFilterIterator.class.getName());
		}
		try {
			final String gtFilterStr = options.get(CQL_FILTER);
			gtFilter = ECQL.toFilter(gtFilterStr);
			final String gwFilterStr = options.get(GEOWAVE_FILTER);
			if (gwFilterStr != null) {
				final byte[] geowaveFilterBytes = ByteArrayUtils.byteArrayFromString(gwFilterStr);
				geowaveFilter = PersistenceUtils.fromBinary(
						geowaveFilterBytes,
						DistributableQueryFilter.class);
			}
			final String modelStr = options.get(MODEL);
			final byte[] modelBytes = ByteArrayUtils.byteArrayFromString(modelStr);
			model = PersistenceUtils.fromBinary(
					modelBytes,
					CommonIndexModel.class);

			final String dataAdapterStr = options.get(DATA_ADAPTER);
			final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(dataAdapterStr);
			dataAdapter = PersistenceUtils.fromBinary(
					dataAdapterBytes,
					FeatureDataAdapter.class);
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}

}
