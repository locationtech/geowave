package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.spi.SPIServiceRegistry;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Logger;
import org.geotools.factory.GeoTools;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class CQLQueryFilter implements
		DistributableQueryFilter
{
	private final static Logger LOGGER = Logger.getLogger(CQLQueryFilter.class);
	private static final Object MUTEX = new Object();
	private static boolean classLoaderInitialized = false;
	private GeotoolsFeatureDataAdapter adapter;
	private Filter filter;

	protected CQLQueryFilter() {
		super();
	}

	public CQLQueryFilter(
			final Filter filter,
			final GeotoolsFeatureDataAdapter adapter ) {
		this.filter = filter;
		this.adapter = adapter;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		if ((filter != null) && (indexModel != null) && (adapter != null)) {
			if (adapter.getAdapterId().equals(
					persistenceEncoding.getAdapterId())) {
				final PersistentDataset<Object> adapterExtendedValues = new PersistentDataset<Object>();
				if (persistenceEncoding instanceof IndexedAdapterPersistenceEncoding) {
					final PersistentDataset<Object> existingExtValues = ((IndexedAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData();
					if (existingExtValues != null) {
						for (final PersistentValue<Object> val : existingExtValues.getValues()) {
							adapterExtendedValues.addValue(val);
						}
					}
				}
				final PersistentDataset<byte[]> stillUnknownValues = new PersistentDataset<byte[]>();
				final List<PersistentValue<byte[]>> unknownDataValues = persistenceEncoding.getUnknownData().getValues();
				for (final PersistentValue<byte[]> v : unknownDataValues) {
					final FieldReader<Object> reader = adapter.getReader(v.getId());
					final Object value = reader.readField(v.getValue());
					adapterExtendedValues.addValue(new PersistentValue<Object>(
							v.getId(),
							value));
				}
				if (persistenceEncoding instanceof IndexedAdapterPersistenceEncoding) {
					for (final PersistentValue<Object> v : ((IndexedAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData().getValues()) {
						adapterExtendedValues.addValue(v);
					}
				}
				final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
						persistenceEncoding.getAdapterId(),
						persistenceEncoding.getDataId(),
						persistenceEncoding.getIndexInsertionId(),
						persistenceEncoding.getDuplicateCount(),
						persistenceEncoding.getCommonData(),
						stillUnknownValues,
						adapterExtendedValues);

				final SimpleFeature feature = adapter.decode(
						encoding,
						new PrimaryIndex(
								null, // because we know the feature data
										// adapter doesn't use the numeric index
										// strategy and only the common index
										// model to decode the simple feature,
										// we pass along a null strategy to
										// eliminate the necessity to send a
										// serialization of the strategy in the
										// options of this iterator
								indexModel));
				if (feature == null) {
					return false;
				}
				return filter.evaluate(feature);
			}
		}
		return true;
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
				final ClassLoader urlCL = java.security.AccessController.doPrivileged(new java.security.PrivilegedAction<URLClassLoader>() {
					@Override
					public URLClassLoader run() {
						final URLClassLoader ucl = new URLClassLoader(
								fileUrls,
								cl);
						return ucl;
					}
				});
				GeoTools.addClassLoader(urlCL);
				SPIServiceRegistry.registerClassLoader(urlCL);

			}
			classLoaderInitialized = true;
		}

	}

	@Override
	public byte[] toBinary() {
		byte[] filterBytes;
		if (filter == null) {
			LOGGER.warn("CQL filter is null");
			filterBytes = new byte[] {};
		}
		else {
			filterBytes = StringUtils.stringToBinary(ECQL.toCQL(filter));
		}
		byte[] adapterBytes;
		if (adapter != null) {
			adapterBytes = PersistenceUtils.toBinary(adapter);
		}
		else {
			LOGGER.warn("Feature Data Adapter is null");
			adapterBytes = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + adapterBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(adapterBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		try {
			initClassLoader(CQLQueryFilter.class);
		}
		catch (final MalformedURLException e) {
			LOGGER.error(
					"Unable to initialize GeoTools class loader",
					e);
		}
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int adapterBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];
			buf.get(filterBytes);
			final String cql = StringUtils.stringFromBinary(filterBytes);
			try {
				filter = ECQL.toFilter(cql);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						cql,
						e);
			}
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}

		if (adapterBytesLength > 0) {
			final byte[] adapterBytes = new byte[adapterBytesLength];
			buf.get(adapterBytes);

			try {
				adapter = PersistenceUtils.fromBinary(
						adapterBytes,
						GeotoolsFeatureDataAdapter.class);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			LOGGER.warn("Feature Data Adapter is empty bytes");
			adapter = null;
		}
	}
}
