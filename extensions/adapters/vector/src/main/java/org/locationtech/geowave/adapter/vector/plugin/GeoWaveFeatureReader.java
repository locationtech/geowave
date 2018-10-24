/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.plugin;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.FidFilterImpl;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.util.QueryIndexHelper;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
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
		return new CloseableIterator.Empty<>();
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
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
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

		final List<CloseableIterator<SimpleFeature>> results = new ArrayList<>();
		final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap = transaction.getDataStatistics();

		final BasicQuery query = getQuery(
				statsMap,
				jtsBounds,
				timeBounds);

		boolean spatialOnly = false;
		if (this.query.getHints().containsKey(
				SubsampleProcess.SUBSAMPLE_ENABLED) && (Boolean) this.query.getHints().get(
				SubsampleProcess.SUBSAMPLE_ENABLED)) {
			spatialOnly = true;
		}
		try (CloseableIterator<Index> indexIt = getComponents().getIndices(
				statsMap,
				query,
				spatialOnly)) {
			while (indexIt.hasNext()) {
				final Index index = indexIt.next();

				final CloseableIterator<SimpleFeature> it = issuer.query(
						index,
						query);
				if (it != null) {
					results.add(it);
				}
			}
		}
		if (results.isEmpty()) {
			return getNoData();
		}
		return interweaveTransaction(
				issuer.getLimit(),
				issuer.getFilter(),
				new CloseableIteratorWrapper<>(

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
			final Index index ) {
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
			final Index index ) {
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
				final Index index,
				final BasicQuery query ) {
			VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder().addTypeName(
					components.getAdapter().getTypeName()).indexName(
					index.getName()).setAuthorizations(
					transaction.composeAuthorizations()).constraints(
					OptimalCQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query));
			if (limit != null) {
				bldr = bldr.limit(limit);
			}
			if (subsetRequested()) {
				bldr = bldr.subsetFields(
						components.getAdapter().getTypeName(),
						getSubset());
			}
			return components.getDataStore().query(
					bldr.build());
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
				final Index index,
				final BasicQuery query ) {
			VectorAggregationQueryBuilder<Persistable, Long> bldr = (VectorAggregationQueryBuilder) VectorAggregationQueryBuilder
					.newBuilder()
					.count(
							components.getAdapter().getTypeName())
					.indexName(
							index.getName())
					.setAuthorizations(
							transaction.composeAuthorizations())
					.constraints(
							OptimalCQLQuery.createOptimalQuery(
									filter,
									components.getAdapter(),
									index,
									query));
			if (limit != null) {
				bldr = bldr.limit(limit);
			}
			final Long count = components.getDataStore().aggregate(
					bldr.build());
			if (count != null) {
				this.count = count;
			}
			return null;
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
				final Index index,
				final BasicQuery query ) {

			VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder().addTypeName(
					components.getAdapter().getTypeName()).indexName(
					index.getName()).setAuthorizations(
					transaction.composeAuthorizations()).constraints(
					OptimalCQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query));
			if (limit != null) {
				bldr = bldr.limit(limit);
			}
			if (subsetRequested()) {
				bldr = bldr.subsetFields(
						components.getAdapter().getTypeName(),
						getSubset());
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
					bldr = bldr.addHint(
							DataStoreUtils.MAX_RESOLUTION_SUBSAMPLING_PER_DIMENSION,
							spans);
					return components.getDataStore().query(
							bldr.build());
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
				final Index index,
				final BasicQuery query ) {
			final VectorAggregationQueryBuilder<DistributedRenderOptions, DistributedRenderResult> bldr = (VectorAggregationQueryBuilder) VectorAggregationQueryBuilder
					.newBuilder()
					.indexName(
							index.getName())
					.setAuthorizations(
							transaction.composeAuthorizations());
			bldr.aggregate(
					components.getAdapter().getTypeName(),
					new DistributedRenderAggregation(
							renderOptions)).constraints(
					OptimalCQLQuery.createOptimalQuery(
							filter,
							components.getAdapter(),
							index,
							query));
			final DistributedRenderResult result = components.getDataStore().aggregate(
					bldr.build());
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
			final ByteArray[] ids = new ByteArray[fids.size()];
			int i = 0;
			for (final String fid : fids) {
				ids[i++] = new ByteArray(
						fid);
			}

			final Index[] writeIndices = components.getAdapterIndices();
			final String queryIndexName = ((writeIndices != null) && (writeIndices.length > 0)) ? writeIndices[0]
					.getName() : null;
			VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder().addTypeName(
					components.getAdapter().getTypeName()).indexName(
					queryIndexName).setAuthorizations(
					transaction.composeAuthorizations());
			if (limit != null) {
				bldr = bldr.limit(limit);
			}
			if (subsetRequested()) {
				bldr = bldr.subsetFields(
						components.getAdapter().getTypeName(),
						getSubset());
			}

			return components.getDataStore().query(
					bldr.constraints(
							bldr.constraintsFactory().dataIds(
									ids)).build());
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

	protected List<InternalDataStatistics<SimpleFeature, ?, ?>> getStatsFor(
			final String name ) {
		final List<InternalDataStatistics<SimpleFeature, ?, ?>> stats = new LinkedList<>();
		final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap = transaction.getDataStatistics();
		for (final Map.Entry<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stat : statsMap.entrySet()) {
			if ((stat.getValue() instanceof FieldNameStatistic)
					&& ((FieldNameStatistic) stat.getValue()).getFieldName().endsWith(
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

	private String[] getSubset() {
		if (query == null) {
			return new String[0];
		}

		if ((query.getFilter() != null) && !components.getGTstore().getDataStoreOptions().isServerSideLibraryEnabled()) {
			final ExtractAttributesFilter attributesVisitor = new ExtractAttributesFilter();
			final Object obj = query.getFilter().accept(
					attributesVisitor,
					null);

			if ((obj != null) && (obj instanceof Collection)) {
				final Set<String> properties = Sets.newHashSet(query.getPropertyNames());
				for (final String prop : (Collection<String>) obj) {
					properties.add(prop);
				}
				return properties.toArray(new String[0]);
			}
		}
		return query.getPropertyNames();
	}
}
