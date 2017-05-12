package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class CQLQueryFilter implements
		DistributableQueryFilter
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CQLQueryFilter.class);
	private GeotoolsFeatureDataAdapter adapter;
	private Filter filter;

	protected CQLQueryFilter() {
		super();
	}

	public CQLQueryFilter(
			final Filter filter,
			final GeotoolsFeatureDataAdapter adapter ) {
		try {
			// We do not have a way to transform a filter directly from one to
			// another.
			this.filter = FilterToCQLTool.toFilter(FilterToCQLTool.toCQL(filter));
		}
		catch (CQLException e) {
			LOGGER.trace(
					"Filter is not a CQL Expression",
					e);
			this.filter = filter;
		}
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
				if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
					((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
							adapter,
							indexModel);
					final PersistentDataset<Object> existingExtValues = ((AbstractAdapterPersistenceEncoding) persistenceEncoding)
							.getAdapterExtendedData();
					if (existingExtValues != null) {
						for (final PersistentValue<Object> val : existingExtValues.getValues()) {
							adapterExtendedValues.addValue(val);
						}
					}
				}
				final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
						persistenceEncoding.getAdapterId(),
						persistenceEncoding.getDataId(),
						persistenceEncoding.getIndexInsertionId(),
						persistenceEncoding.getDuplicateCount(),
						persistenceEncoding.getCommonData(),
						new PersistentDataset<byte[]>(),
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

	@Override
	public byte[] toBinary() {
		byte[] filterBytes;
		if (filter == null) {
			LOGGER.warn("CQL filter is null");
			filterBytes = new byte[] {};
		}
		else {
			filterBytes = StringUtils.stringToBinary(FilterToCQLTool.toCQL(filter));
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
			FeatureDataUtils.initClassLoader();
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
