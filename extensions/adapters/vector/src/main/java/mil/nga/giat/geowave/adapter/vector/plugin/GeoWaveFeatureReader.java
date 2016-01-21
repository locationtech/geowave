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
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;

import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.adapter.vector.render.DistributableRenderer;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureStatistic;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
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

import org.apache.log4j.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
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

	public CloseableIterator<SimpleFeature> issueQuery(
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBounds,
			final QueryIssuer issuer ) {

		final List<CloseableIterator<SimpleFeature>> results = new ArrayList<CloseableIterator<SimpleFeature>>();
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = transaction.getDataStatistics();

		final Constraints timeConstraints = QueryIndexHelper.composeTimeBoundedConstraints(
				components.getAdapter().getType(),
				components.getAdapter().getTimeDescriptors(),
				statsMap,
				timeBounds);

		final Constraints geoConstraints = QueryIndexHelper.composeGeometricConstraints(
				getFeatureType(),
				transaction.getDataStatistics(),
				jtsBounds);

		/**
		 * NOTE: query to an index that requires a constraint and the constraint
		 * is missing equates to a full table scan. @see BasicQuery
		 */

		final BasicQuery query = composeQuery(
				jtsBounds,
				geoConstraints,
				timeConstraints);

		try (CloseableIterator<Index<?, ?>> indexIt = getComponents().getIndices(
				statsMap,
				query)) {
			while (indexIt.hasNext()) {
				final PrimaryIndex index = (PrimaryIndex) indexIt.next();
				results.add(issuer.query(
						index,
						query));

			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close index iterator for query",
					e);
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
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
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
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
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
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					new QueryOptions(
							components.getAdapter(),
							index,
							limit,
							null,
							transaction.composeAuthorizations()),
					new CQLQuery(
							query,
							filter,
							components.getAdapter()));
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
				final mil.nga.giat.geowave.core.store.query.Query query ) {

			final QueryOptions options = new QueryOptions(
					components.getAdapter(),
					index,
					transaction.composeAuthorizations());
			options.setLimit(limit);
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
							new CQLQuery(
									query,
									filter,
									components.getAdapter()));
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
				final PrimaryIndex index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					new QueryOptions(
							components.getAdapter(),
							index,
							transaction.composeAuthorizations()),
					new CQLQuery(
							query,
							filter,
							components.getAdapter()));
			// TODO: ? need to figure out how to add back CqlQueryRenderIterator
			// renderer,
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
				final PrimaryIndex index,
				final mil.nga.giat.geowave.core.store.query.Query query ) {
			return components.getDataStore().query(
					new QueryOptions(
							components.getAdapter(),
							index,
							limit,
							null,
							transaction.composeAuthorizations()),
					new DataIdQuery(
							components.getAdapter().getAdapterId(),
							ids));

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

			final List<PrimaryIndex> writeIndices = components.getWriteIndices();
			final PrimaryIndex queryIndex = ((writeIndices != null) && (writeIndices.size() > 0)) ? writeIndices.get(0) : null;

			return components.getDataStore().query(
					new QueryOptions(
							components.getAdapter(),
							queryIndex,
							limit,
							null,
							transaction.composeAuthorizations()),
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
			final Geometry jtsBounds,
			final Constraints geoConstraints,
			final Constraints temporalConstraints ) {

		if (jtsBounds == null) {
			return new BasicQuery(
					geoConstraints.merge(temporalConstraints));
		}
		else {
			return new SpatialQuery(
					geoConstraints.merge(temporalConstraints),
					jtsBounds);
		}
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
