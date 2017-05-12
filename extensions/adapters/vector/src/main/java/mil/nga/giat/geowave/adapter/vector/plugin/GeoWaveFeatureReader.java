package mil.nga.giat.geowave.adapter.vector.plugin;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderAggregation;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderOptions;
import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderResult;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureStatistic;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.core.geotime.GeometryUtils.GeoConstraintsWrapper;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;

/**
 * This class wraps a geotools data store as well as one for statistics (for
 * example to display Heatmaps) into a GeoTools FeatureReader for simple feature
 * data. It acts as a helper for GeoWave's GeoTools data store.
 *
 */
public class GeoWaveFeatureReader implements
		FeatureReader<SimpleFeatureType, SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveFeatureReader.class);

	private final GeoWaveDataStoreComponents components;
	private final GeoWaveFeatureCollection featureCollection;
	private final GeoWaveTransaction transaction;
	private final Query query;

	public GeoWaveFeatureReader(
			final Query query,
			final GeoWaveTransaction transaction,
			final GeoWaveDataStoreComponents components )
			throws IOException {
		this.components = components;
		this.transaction = transaction;
		featureCollection = new GeoWaveFeatureCollection(
				this,
				query);
		this.query = query;
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
		return components.getAdapter().getFeatureType();
	}

	@Override
	public boolean hasNext()
			throws IOException {
		Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
		if (it != null) {
			// protect againt GeoTools forgetting to call close()
			// on this FeatureReader, which causes a resource leak
			if (!it.hasNext()) {
				((CloseableIterator<?>) it).close();
			}
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

	public long getCount() {
		return featureCollection.getCount();
	}

	protected long getCountInternal(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final Filter filter,
			final Integer limit ) {
		final CountQueryIssuer countIssuer = new CountQueryIssuer(
				filter,
				limit);
		issueQuery(
				jtsBounds,
				timeBounds,
				countIssuer);
		return countIssuer.count;
	}

	private BasicQuery getQuery(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds ) {
		final Constraints timeConstraints = QueryIndexHelper.composeTimeBoundedConstraints(
				components.getAdapter().getFeatureType(),
				components.getAdapter().getTimeDescriptors(),
				statsMap,
				timeBounds);

		final GeoConstraintsWrapper geoConstraints = QueryIndexHelper.composeGeometricConstraints(
				getFeatureType(),
				statsMap,
				jtsBounds);

		/**
		 * NOTE: query to an index that requires a constraint and the constraint
		 * is missing equates to a full table scan. @see BasicQuery
		 */

		final BasicQuery query = composeQuery(
				geoConstraints,
				timeConstraints);
		query.setExact(timeBounds.isExact());
		return query;
	}

	public CloseableIterator<SimpleFeature> issueQuery(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final QueryIssuer issuer ) {

		final List<CloseableIterator<SimpleFeature>> results = new ArrayList<CloseableIterator<SimpleFeature>>();
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = transaction.getDataStatistics();

		final BasicQuery query = getQuery(
				statsMap,
				jtsBounds,
				timeBounds);

		try (CloseableIterator<Index<?, ?>> indexIt = getComponents().getIndices(
				statsMap,
				query)) {
			while (indexIt.hasNext()) {
				final PrimaryIndex index = (PrimaryIndex) indexIt.next();

				final CloseableIterator<SimpleFeature> it = issuer.query(
						index,
						query);
				if (it != null) {
					results.add(it);
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close index iterator for query",
					e);
		}
		if (results.isEmpty()) {
			return getNoData();
		}
		return interweaveTransaction(
				issuer.getLimit(),
				issuer.getFilter(),
				new CloseableIteratorWrapper<SimpleFeature>(

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

	protected static boolean hasAtLeastSpatial(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null)
				|| (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		boolean hasLatitude = false;
		boolean hasLongitude = false;
		for (final NumericDimensionDefinition dimension : index.getIndexStrategy().getOrderedDimensionDefinitions()) {
			if (dimension instanceof LatitudeDefinition) {
				hasLatitude = true;
			}
			if (dimension instanceof LatitudeDefinition) {
				hasLongitude = true;
			}
		}
		return hasLatitude && hasLongitude;
	}

	protected static boolean hasTime(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null)
				|| (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		for (final NumericDimensionDefinition dimension : index.getIndexStrategy().getOrderedDimensionDefinitions()) {
			if (dimension instanceof TimeDefinition) {
				return true;
			}
		}
		return false;
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
				final PrimaryIndex index,
				final BasicQuery query ) {
			final QueryOptions queryOptions = new QueryOptions(
					components.getAdapter(),
					index,
					limit,
					null,
					transaction.composeAuthorizations());
			if (subsetRequested()) {
				queryOptions.setFieldIds(
						getSubset(),
						components.getAdapter());
			}
			return components.getDataStore().query(
					queryOptions,
					CQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query));
		}

		@Override
		public Filter getFilter() {
			return filter;
		}

		@Override
		public Integer getLimit() {
			return limit;
		}
	}

	private class CountQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		private long count = 0;

		public CountQueryIssuer(
				final Filter filter,
				final Integer limit ) {
			super(
					filter,
					limit);
		}

		@Override
		public CloseableIterator<SimpleFeature> query(
				final PrimaryIndex index,
				final BasicQuery query ) {
			final QueryOptions queryOptions = new QueryOptions(
					components.getAdapter(),
					index,
					limit,
					null,
					transaction.composeAuthorizations());
			queryOptions.setAggregation(
					new CountAggregation(),
					components.getAdapter());

			try (final CloseableIterator<CountResult> result = components.getDataStore().query(
					queryOptions,
					CQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query))) {
				if (result.hasNext()) {
					final CountResult cntResult = result.next();
					if (cntResult != null) {
						count = cntResult.getCount();
					}
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close count iterator",
						e);
			}
			return null;
		}

		public long getCount() {
			return count;
		}
	}

	private class EnvelopeQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		final ReferencedEnvelope envelope;
		final int width;
		final int height;
		final double pixelSize;

		public EnvelopeQueryIssuer(
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
				final PrimaryIndex index,
				final BasicQuery query ) {

			final QueryOptions options = new QueryOptions(
					components.getAdapter(),
					index,
					transaction.composeAuthorizations());
			options.setLimit(limit);
			if (subsetRequested()) {
				options.setFieldIds(
						getSubset(),
						components.getAdapter());
			}
			final double east = envelope.getMaxX();
			final double west = envelope.getMinX();
			final double north = envelope.getMaxY();
			final double south = envelope.getMinY();

			try {
				final AffineTransform worldToScreen = RendererUtilities.worldToScreenTransform(
						new ReferencedEnvelope(
								new Envelope(
										west,
										east,
										south,
										north),
								CRS.decode("EPSG:4326")),
						new Rectangle(
								width,
								height));
				final MathTransform2D fullTransform = (MathTransform2D) ProjectiveTransform.create(worldToScreen);
				// calculate spans
				try {
					final double[] spans = Decimator.computeGeneralizationDistances(
							fullTransform.inverse(),
							new Rectangle(
									width,
									height),
							pixelSize);
					options.setMaxResolutionSubsamplingPerDimension(spans);
					return components.getDataStore().query(
							options,
							CQLQuery.createOptimalQuery(
									filter,
									components.getAdapter(),
									index,
									query));
				}
				catch (final TransformException e) {
					throw new IllegalArgumentException(
							"Unable to compute generalization distance",
							e);
				}
			}
			catch (MismatchedDimensionException | FactoryException e) {
				throw new IllegalArgumentException(
						"Unable to decode CRS EPSG:4326",
						e);
			}
		}
	}

	private class RenderQueryIssuer extends
			BaseIssuer implements
			QueryIssuer
	{
		final DistributedRenderOptions renderOptions;

		public RenderQueryIssuer(
				final Filter filter,
				final Integer limit,
				final DistributedRenderOptions renderOptions ) {
			super(
					filter,
					limit);
			this.renderOptions = renderOptions;

		}

		@Override
		public CloseableIterator<SimpleFeature> query(
				final PrimaryIndex index,
				final BasicQuery query ) {
			final QueryOptions queryOptions = new QueryOptions(
					components.getAdapter(),
					index,
					transaction.composeAuthorizations());
			if (subsetRequested()) {
				queryOptions.setFieldIds(
						getSubset(),
						components.getAdapter());
			}
			queryOptions.setAggregation(
					new DistributedRenderAggregation(
							renderOptions),
					components.getAdapter());
			try (CloseableIterator<DistributedRenderResult> resultIt = components.getDataStore().query(
					queryOptions,
					CQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query))) {
				if (resultIt.hasNext()) {
					final DistributedRenderResult result = resultIt.next();
					return new CloseableIterator.Wrapper(
							Iterators.singletonIterator(SimpleFeatureBuilder.build(
									GeoWaveFeatureCollection.getDistributedRenderFeatureType(),
									new Object[] {
										result,
										renderOptions
									},
									"render")));
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to get distributed rendering result",
						e);
			}
			return new CloseableIterator.Wrapper(
					Iterators.emptyIterator());
		}
	}

	public CloseableIterator<SimpleFeature> renderData(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final Filter filter,
			final Integer limit,
			final DistributedRenderOptions renderOptions ) {
		return issueQuery(
				jtsBounds,
				timeBounds,
				new RenderQueryIssuer(
						filter,
						limit,
						renderOptions));
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
				new EnvelopeQueryIssuer(
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
			final Set<String> fids = ((FidFilterImpl) filter).getIDs();
			final List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
			for (final String fid : fids) {
				ids.add(new ByteArrayId(
						fid));
			}

			final PrimaryIndex[] writeIndices = components.getAdapterIndices();
			final PrimaryIndex queryIndex = ((writeIndices != null) && (writeIndices.length > 0)) ? writeIndices[0]
					: null;

			final QueryOptions queryOptions = new QueryOptions(
					components.getAdapter(),
					queryIndex,
					limit,
					null,
					transaction.composeAuthorizations());
			if (subsetRequested()) {
				queryOptions.setFieldIds(
						getSubset(),
						components.getAdapter());
			}

			return components.getDataStore().query(
					queryOptions,
					new DataIdQuery(
							components.getAdapter().getAdapterId(),
							ids));
		}
		return issueQuery(
				jtsBounds,
				timeBounds,
				new BaseIssuer(
						filter,
						limit));
	}

	public GeoWaveFeatureCollection getFeatureCollection() {
		return featureCollection;
	}

	private CloseableIterator<SimpleFeature> interweaveTransaction(
			final Integer limit,
			final Filter filter,
			final CloseableIterator<SimpleFeature> it ) {
		return transaction.interweaveTransaction(
				limit,
				filter,
				it);

	}

	protected List<DataStatistics<SimpleFeature>> getStatsFor(
			final String name ) {
		final List<DataStatistics<SimpleFeature>> stats = new LinkedList<DataStatistics<SimpleFeature>>();
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = transaction.getDataStatistics();
		for (final Map.Entry<ByteArrayId, DataStatistics<SimpleFeature>> stat : statsMap.entrySet()) {
			if ((stat.getValue() instanceof FeatureStatistic)
					&& ((FeatureStatistic) stat.getValue()).getFieldName().endsWith(
							name)) {
				stats.add(stat.getValue());
			}
		}
		return stats;
	}

	protected TemporalConstraintsSet clipIndexedTemporalConstraints(
			final TemporalConstraintsSet constraintsSet ) {
		return QueryIndexHelper.clipIndexedTemporalConstraints(
				transaction.getDataStatistics(),
				components.getAdapter().getTimeDescriptors(),
				constraintsSet);
	}

	protected Geometry clipIndexedBBOXConstraints(
			final Geometry bbox ) {
		return QueryIndexHelper.clipIndexedBBOXConstraints(
				getFeatureType(),
				bbox,
				transaction.getDataStatistics());
	}

	private BasicQuery composeQuery(
			final GeoConstraintsWrapper geoConstraints,
			final Constraints temporalConstraints ) {

		// TODO: this actually doesn't boost performance much, if at
		// all, and one key is missing - the query geometry has to be
		// topologically equivalent to its envelope and the ingested
		// geometry has to be topologically equivalent to its envelope
		// this could be kept as a statistic on ingest, but considering
		// it doesn't boost performance it may not be worthwhile
		// pursuing

		// if (geoConstraints.isConstraintsMatchGeometry()) {
		// return new BasicQuery(
		// geoConstraints.getConstraints().merge(
		// temporalConstraints));
		// }
		// else {
		return new SpatialQuery(
				geoConstraints.getConstraints().merge(
						temporalConstraints),
				geoConstraints.getGeometry());
		// }
	}

	public Object convertToType(
			final String attrName,
			final Object value ) {
		final SimpleFeatureType featureType = components.getAdapter().getFeatureType();
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

	private boolean subsetRequested() {
		if (query == null) {
			return false;
		}
		return !(query.getPropertyNames() == Query.ALL_NAMES);
	}

	private List<String> getSubset() {
		if (query == null) {
			return Collections.emptyList();
		}
		return Arrays.asList(query.getPropertyNames());
	}
}
