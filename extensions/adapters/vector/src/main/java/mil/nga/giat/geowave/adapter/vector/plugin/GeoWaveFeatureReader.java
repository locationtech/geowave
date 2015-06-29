package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;

import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureStatistic;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.adapter.vector.wms.DistributableRenderer;
import mil.nga.giat.geowave.core.geotime.DimensionalityType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.apache.log4j.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This class wraps a geotools data store as well as one for statistics (for
 * example to display Heatmaps) into a GeoTools FeatureReader for simple feature
 * data. It acts as a helper for GeoWave's GeoTools data store.
 * 
 */
public class GeoWaveFeatureReader implements
		FeatureReader<SimpleFeatureType, SimpleFeature>
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveFeatureReader.class);

	private final GeoWaveDataStoreComponents components;
	private final GeoWaveFeatureCollection featureCollection;
	private final GeoWaveTransaction transaction;

	public GeoWaveFeatureReader(
			final Query query,
			final GeoWaveTransaction transaction,
			final GeoWaveDataStoreComponents components )
			throws IOException {
		this.components = components;
		this.transaction = transaction;
		// make sure the the buffer cache is flushed to the underlying datastore
		// the case where pending additions have not been committed is not
		// supported under the WFS-T spec, but the geotools API
		// does not prevent it.
		transaction.flush();
		featureCollection = new GeoWaveFeatureCollection(
				this,
				query);
	}

	public GeoWaveTransaction getTransaction() {
		return transaction;
	}

	public GeoWaveDataStoreComponents getComponents() {
		return components;
	}

	@Override
	public void close()
			throws IOException {
		if (featureCollection.getOpenIterator() != null) {
			featureCollection.closeIterator(featureCollection.getOpenIterator());
		}
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		if (featureCollection.isDistributedRenderQuery()) {
			return GeoWaveFeatureCollection.getDistributedRenderFeatureType();
		}
		return components.getAdapter().getType();
	}

	@Override
	public boolean hasNext()
			throws IOException {
		Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
		if (it != null) {
			return it.hasNext();
		}
		it = featureCollection.openIterator();
		return it.hasNext();
	}

	@Override
	public SimpleFeature next()
			throws IOException,
			IllegalArgumentException,
			NoSuchElementException {
		Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
		if (it != null) {
			return it.next();
		}
		it = featureCollection.openIterator();
		return it.next();
	}

	public CloseableIterator<SimpleFeature> getNoData() {
		return new CloseableIterator.Empty<SimpleFeature>();
	}

	public CloseableIterator<SimpleFeature> issueQuery(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final QueryIssuer issuer ) {

		final List<CloseableIterator<SimpleFeature>> results = new ArrayList<CloseableIterator<SimpleFeature>>();
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = components.getDataStatistics(transaction);

		try (CloseableIterator<Index> indexIt = getComponents().getDataStore().getIndices()) {
			while (indexIt.hasNext()) {
				final Index index = indexIt.next();

				final Constraints timeConstraints = QueryIndexHelper.composeTimeBoundedConstraints(
						components.getAdapter().getType(),
						components.getAdapter().getTimeDescriptors(),
						statsMap,
						timeBounds);

				/*
				 * Inspect for SPATIAL_TEMPORAL type index. Most queries issued
				 * from GeoServer, where time is an 'enabled' dimension, provide
				 * time constraints. Often they only an upper bound. The
				 * statistics were used to clip the bounds prior to this point.
				 * However, the range may be still too wide. Ideally, spatial
				 * temporal indexing should not be used in these cases.
				 * Eventually all this logic should be replaced by a
				 * QueryPlanner that takes the full set of constraints and has
				 * in-depth knowledge of each indices capabilities.
				 */
				if ((jtsBounds == null) || (DimensionalityType.SPATIAL_TEMPORAL.isCompatible(index) && timeConstraints.isEmpty())) {
					// full table scan
					results.add(issuer.query(
							index,
							null));
				}
				else {

					final Constraints geoConstraints = QueryIndexHelper.composeGeometricConstraints(
							components.getAdapter().getType(),
							statsMap,
							jtsBounds);

					if (timeConstraints.isSupported(index)) {

						results.add(issuer.query(
								index,
								composeQuery(
										jtsBounds,
										geoConstraints,
										timeConstraints)));
					}
					else {
						// just geo
						results.add(issuer.query(
								index,
								composeQuery(
										jtsBounds,
										geoConstraints,
										null)));
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close index iterator for query",
					e);
		}
		return interweaveTransaction(new CloseableIteratorWrapper<SimpleFeature>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<SimpleFeature> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(results.iterator())));
	}

	private class BaseIssuer implements
			QueryIssuer
	{

		final Filter filter;
		final Integer limit;

		public BaseIssuer(
				final Filter filter,
				final Integer limit ) {
			super();

			this.filter = filter;
			this.limit = limit;
		}

		@Override
		public CloseableIterator<SimpleFeature> query(
				final Index index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					components.getAdapter(),
					index,
					query,
					filter,
					(limit != null) && (limit >= 0) ? limit : null,
					transaction.composeAuthorizations());
		}
	}

	private class EnvelopQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		final ReferencedEnvelope envelope;
		final int width;
		final int height;
		final double pixelSize;

		public EnvelopQueryIssuer(
				final int width,
				final int height,
				final double pixelSize,
				final Filter filter,
				final Integer limit,
				final ReferencedEnvelope envelope ) {
			super(
					filter,
					limit);
			this.width = width;
			this.height = height;
			this.pixelSize = pixelSize;
			this.envelope = envelope;

		}

		@Override
		public CloseableIterator<SimpleFeature> query(
				final Index index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					components.getAdapter(),
					index,
					query,
					width,
					height,
					pixelSize,
					filter,
					envelope,
					limit,
					transaction.composeAuthorizations());
		}

	}

	private class RenderQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		final DistributableRenderer renderer;

		public RenderQueryIssuer(
				final Filter filter,
				final DistributableRenderer renderer ) {
			super(
					filter,
					null);
			this.renderer = renderer;

		}

		@Override
		public CloseableIterator<SimpleFeature> query(
				final Index index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					components.getAdapter(),
					index,
					query,
					filter,
					renderer,
					transaction.composeAuthorizations());
		}

	}

	private class IdQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		final List<ByteArrayId> ids;

		public IdQueryIssuer(
				final List<ByteArrayId> ids ) {
			super(
					null,
					null);
			this.ids = ids;

		}

		@SuppressWarnings("unchecked")
		@Override
		public CloseableIterator<SimpleFeature> query(
				final Index index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {

			return (CloseableIterator<SimpleFeature>) components.getDataStore().query(
					ids,
					query);
		}

	}

	public CloseableIterator<SimpleFeature> renderData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final Filter filter,
			final DistributableRenderer renderer ) {
		return issueQuery(
				jtsBounds,
				timeBounds,
				new RenderQueryIssuer(
						filter,
						renderer));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final int width,
			final int height,
			final double pixelSize,
			final Filter filter,
			final ReferencedEnvelope envelope,
			final Integer limit ) {
		return issueQuery(
				jtsBounds,
				timeBounds,
				new EnvelopQueryIssuer(
						width,
						height,
						pixelSize,
						filter,
						limit,
						envelope));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final Integer limit ) {
		return issueQuery(
				jtsBounds,
				timeBounds,
				new BaseIssuer(
						null,
						limit));

	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final Filter filter,
			final Integer limit ) {
		if (filter instanceof FidFilterImpl) {
			final List<SimpleFeature> retVal = new ArrayList<SimpleFeature>();
			final Set<String> fids = ((FidFilterImpl) filter).getIDs();
			for (final String fid : fids) {
				retVal.add((SimpleFeature) components.getDataStore().getEntry(
						components.getCurrentIndex(),
						new ByteArrayId(
								fid),
						components.getAdapter().getAdapterId(),
						transaction.composeAuthorizations()));
			}
			return new CloseableIterator.Wrapper(
					retVal.iterator());
		}
		return issueQuery(
				jtsBounds,
				timeBounds,
				new BaseIssuer(
						filter,
						limit));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final int level,
			final String statsName ) {
		return issueQuery(
				jtsBounds,
				timeBounds,
				new IdQueryIssuer(
						Arrays.asList(new ByteArrayId[] {
							new ByteArrayId(
									StringUtils.stringToBinary("l" + level + "_stats" + statsName))
						})));

	}

	public GeoWaveFeatureCollection getFeatureCollection() {
		return featureCollection;
	}

	private CloseableIterator<SimpleFeature> interweaveTransaction(
			final CloseableIterator<SimpleFeature> it ) {
		return transaction.interweaveTransaction(it);

	}

	protected List<DataStatistics<SimpleFeature>> getStatsFor(
			final String name ) {
		final List<DataStatistics<SimpleFeature>> stats = new LinkedList<DataStatistics<SimpleFeature>>();
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = components.getDataStatistics(transaction);
		for (final Map.Entry<ByteArrayId, DataStatistics<SimpleFeature>> stat : statsMap.entrySet()) {
			if ((stat.getValue() instanceof FeatureStatistic) && ((FeatureStatistic) stat.getValue()).getFieldName().endsWith(
					name)) {
				stats.add(stat.getValue());
			}
		}
		return stats;
	}

	protected TemporalConstraintsSet clipIndexedTemporalConstraints(
			final TemporalConstraintsSet constraintsSet ) {
		return QueryIndexHelper.clipIndexedTemporalConstraints(
				components.getDataStatistics(transaction),
				components.getAdapter().getTimeDescriptors(),
				constraintsSet);
	}

	protected Geometry clipIndexedBBOXConstraints(
			final Geometry bbox ) {
		return QueryIndexHelper.clipIndexedBBOXConstraints(
				getFeatureType(),
				bbox,
				components.getDataStatistics(transaction));
	}

	private BasicQuery composeQuery(
			final Geometry jtsBounds,
			final Constraints geoConstraints,
			final Constraints temporalConstraints ) {

		if ((geoConstraints == null) && (temporalConstraints != null)) {
			final Constraints statBasedGeoConstraints = QueryIndexHelper.getBBOXIndexConstraints(
					getFeatureType(),
					components.getDataStatistics(transaction));
			return new BasicQuery(
					statBasedGeoConstraints.merge(temporalConstraints));
		}
		else if ((jtsBounds != null) && (temporalConstraints == null)) {
			return new SpatialQuery(
					geoConstraints,
					jtsBounds);
		}
		else if ((jtsBounds != null) && (temporalConstraints != null)) {
			return new SpatialQuery(
					geoConstraints.merge(temporalConstraints),
					jtsBounds);
		}
		return null;
	}

	public Object convertToType(
			final String attrName,
			final Object value ) {
		final SimpleFeatureType featureType = components.getAdapter().getType();
		final AttributeDescriptor descriptor = featureType.getDescriptor(attrName);
		if (descriptor == null) {
			return value;
		}
		final Class<?> attrClass = descriptor.getType().getBinding();
		if (attrClass.isInstance(value)) {
			return value;
		}
		if (Number.class.isAssignableFrom(attrClass) && Number.class.isInstance(value)) {
			if (Double.class.isAssignableFrom(attrClass)) {
				return ((Number) value).doubleValue();
			}
			if (Float.class.isAssignableFrom(attrClass)) {
				return ((Number) value).floatValue();
			}
			if (Long.class.isAssignableFrom(attrClass)) {
				return ((Number) value).longValue();
			}
			if (Integer.class.isAssignableFrom(attrClass)) {
				return ((Number) value).intValue();
			}
			if (Short.class.isAssignableFrom(attrClass)) {
				return ((Number) value).shortValue();
			}
			if (Byte.class.isAssignableFrom(attrClass)) {
				return ((Number) value).byteValue();
			}
			if (BigInteger.class.isAssignableFrom(attrClass)) {
				return BigInteger.valueOf(((Number) value).longValue());
			}
			if (BigDecimal.class.isAssignableFrom(attrClass)) {
				return BigDecimal.valueOf(((Number) value).doubleValue());
			}
		}
		if (Calendar.class.isAssignableFrom(attrClass)) {
			if (Date.class.isInstance(value)) {
				final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				c.setTime((Date) value);
				return c;
			}
		}
		if (Timestamp.class.isAssignableFrom(attrClass)) {
			if (Date.class.isInstance(value)) {
				final Timestamp ts = new Timestamp(
						((Date) value).getTime());
				return ts;
			}
		}
		return value;
	}
}
