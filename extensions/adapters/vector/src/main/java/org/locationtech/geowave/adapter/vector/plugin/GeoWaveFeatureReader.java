/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.AttributeExpressionImpl;
import org.geotools.filter.FidFilterImpl;
import org.geotools.filter.spatial.BBOXImpl;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.util.QueryIndexHelper;
import org.locationtech.geowave.core.geotime.index.SpatialIndexFilter;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.CQLToGeoWaveConversionException;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.CQLToGeoWaveFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.geotime.util.SpatialIndexUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.geowave.core.store.query.constraints.OptimalExpressionQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * This class wraps a geotools data store as well as one for statistics (for example to display
 * Heatmaps) into a GeoTools FeatureReader for simple feature data. It acts as a helper for
 * GeoWave's GeoTools data store.
 */
public class GeoWaveFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveFeatureReader.class);

  private final GeoWaveDataStoreComponents components;
  private final GeoWaveFeatureCollection featureCollection;
  private final GeoWaveTransaction transaction;
  private final Query query;
  private final Filter filter;
  private final Object geoWaveFilter;

  public GeoWaveFeatureReader(
      final Query query,
      final GeoWaveTransaction transaction,
      final GeoWaveDataStoreComponents components) throws IOException {
    this.components = components;
    this.transaction = transaction;
    featureCollection = new GeoWaveFeatureCollection(this, query);
    this.query = query;
    this.filter = getFilter(query);
    Object gwfilter = null;
    try {
      gwfilter = this.filter.accept(new CQLToGeoWaveFilterVisitor(components.getAdapter()), null);
    } catch (CQLToGeoWaveConversionException | InvalidFilterException e) {
      // Incompatible with GeoWave filter expressions, fall back to regular optimal CQL query
    }
    geoWaveFilter = gwfilter;
  }

  public GeoWaveTransaction getTransaction() {
    return transaction;
  }

  public GeoWaveDataStoreComponents getComponents() {
    return components;
  }

  public org.locationtech.geowave.core.store.query.filter.expression.Filter getGeoWaveFilter() {
    return (org.locationtech.geowave.core.store.query.filter.expression.Filter) geoWaveFilter;
  }

  @Override
  public void close() throws IOException {
    if (featureCollection.getOpenIterator() != null) {
      featureCollection.closeIterator(featureCollection.getOpenIterator());
    }
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    return components.getFeatureType();
  }

  @Override
  public boolean hasNext() throws IOException {
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
  public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
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
      final Integer limit) {
    final CountQueryIssuer countIssuer = new CountQueryIssuer(limit);
    issueQuery(jtsBounds, timeBounds, countIssuer);
    return countIssuer.count;
  }

  private BasicQueryByClass getQuery(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds) {
    final GeoConstraintsWrapper geoConstraints =
        QueryIndexHelper.composeGeometricConstraints(getFeatureType(), jtsBounds);

    if (timeBounds == null) {
      // if timeBounds are unspecified just use the geoConstraints
      return new ExplicitSpatialQuery(
          geoConstraints.getConstraints(),
          geoConstraints.getGeometry(),
          GeometryUtils.getCrsCode(components.getCRS()));
    } else {

      final ConstraintsByClass timeConstraints =
          QueryIndexHelper.composeTimeBoundedConstraints(
              components.getFeatureType(),
              components.getAdapter().getTimeDescriptors(),
              timeBounds);

      /**
       * NOTE: query to an index that requires a constraint and the constraint is missing equates to
       * a full table scan. @see BasicQuery
       */
      final BasicQueryByClass query =
          new ExplicitSpatialQuery(
              geoConstraints.getConstraints().merge(timeConstraints),
              geoConstraints.getGeometry(),
              GeometryUtils.getCrsCode(components.getCRS()));
      query.setExact(timeBounds.isExact());
      return query;
    }

  }

  public CloseableIterator<SimpleFeature> issueQuery(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final QueryIssuer issuer) {
    final List<CloseableIterator<SimpleFeature>> results = new ArrayList<>();
    boolean spatialOnly = false;
    if (this.query.getHints().containsKey(SubsampleProcess.SUBSAMPLE_ENABLED)
        && (Boolean) this.query.getHints().get(SubsampleProcess.SUBSAMPLE_ENABLED)) {
      spatialOnly = true;
    }
    if (!spatialOnly && getGeoWaveFilter() != null) {
      results.add(issuer.query(null, null, spatialOnly));
    } else {
      final BasicQueryByClass query = getQuery(jtsBounds, timeBounds);
      final StatisticsCache statsCache =
          getComponents().getGTstore().getIndexQueryStrategy().requiresStats()
              ? transaction.getDataStatistics()
              : null;
      try (CloseableIterator<Index> indexIt =
          getComponents().getIndices(statsCache, query, spatialOnly)) {
        while (indexIt.hasNext()) {
          final Index index = indexIt.next();

          final CloseableIterator<SimpleFeature> it = issuer.query(index, query, spatialOnly);
          if (it != null) {
            results.add(it);
          }
        }
      }
    }
    if (results.isEmpty()) {
      return getNoData();
    }
    return interweaveTransaction(
        issuer.getLimit(),
        issuer.getFilter(),
        new CloseableIteratorWrapper<>(new Closeable() {
          @Override
          public void close() throws IOException {
            for (final CloseableIterator<SimpleFeature> result : results) {
              result.close();
            }
          }
        }, Iterators.concat(results.iterator())));
  }

  protected static boolean hasTime(final Index index) {
    if ((index == null)
        || (index.getIndexStrategy() == null)
        || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
      return false;
    }
    for (final NumericDimensionDefinition dimension : index.getIndexStrategy().getOrderedDimensionDefinitions()) {
      if ((dimension instanceof TimeDefinition) || (dimension instanceof SimpleTimeDefinition)) {
        return true;
      }
    }
    return false;
  }

  private QueryConstraints createQueryConstraints(
      final Index index,
      final BasicQueryByClass baseQuery,
      final boolean spatialOnly) {
    if (getGeoWaveFilter() != null) {
      return new OptimalExpressionQuery(
          getGeoWaveFilter(),
          spatialOnly ? new SpatialIndexFilter() : null);
    }

    final AdapterToIndexMapping indexMapping =
        components.getAdapterIndexMappingStore().getMapping(
            components.getAdapter().getAdapterId(),
            index.getName());
    return OptimalCQLQuery.createOptimalQuery(
        filter,
        components.getAdapter(),
        index,
        indexMapping,
        baseQuery);
  }

  public Filter getFilter(final Query query) {
    final Filter filter = query.getFilter();
    if (filter instanceof BBOXImpl) {
      final BBOXImpl bbox = ((BBOXImpl) filter);
      final Expression exp1 = bbox.getExpression1();
      if (exp1 instanceof PropertyName) {
        final String propName = ((PropertyName) exp1).getPropertyName();
        if ((propName == null) || propName.isEmpty()) {
          bbox.setExpression1(
              new AttributeExpressionImpl(
                  components.getAdapter().getFeatureType().getGeometryDescriptor().getLocalName()));
        }
      }
    }
    return filter;
  }

  private class BaseIssuer implements QueryIssuer {

    final Integer limit;

    public BaseIssuer(final Integer limit) {
      super();

      this.limit = limit;
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      VectorQueryBuilder bldr =
          VectorQueryBuilder.newBuilder().addTypeName(
              components.getAdapter().getTypeName()).setAuthorizations(
                  transaction.composeAuthorizations()).constraints(
                      createQueryConstraints(index, query, spatialOnly));
      if (index != null) {
        bldr.indexName(index.getName());
      }
      if (limit != null) {
        bldr = bldr.limit(limit);
      }
      if (subsetRequested()) {
        bldr = bldr.subsetFields(components.getAdapter().getTypeName(), getSubset());
      }
      return components.getDataStore().query(bldr.build());
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

  private class CountQueryIssuer extends BaseIssuer implements QueryIssuer {
    private long count = 0;

    public CountQueryIssuer(final Integer limit) {
      super(limit);
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      VectorAggregationQueryBuilder<Persistable, Long> bldr =
          (VectorAggregationQueryBuilder) VectorAggregationQueryBuilder.newBuilder().count(
              components.getAdapter().getTypeName()).setAuthorizations(
                  transaction.composeAuthorizations()).constraints(
                      createQueryConstraints(index, query, spatialOnly));
      if (index != null) {
        bldr.indexName(index.getName());
      }
      if (limit != null) {
        bldr = bldr.limit(limit);
      }
      final Long count = components.getDataStore().aggregate(bldr.build());
      if (count != null) {
        this.count = count;
      }
      return null;
    }
  }

  private class EnvelopeQueryIssuer extends BaseIssuer implements QueryIssuer {
    final ReferencedEnvelope envelope;
    final int width;
    final int height;
    final double pixelSize;

    public EnvelopeQueryIssuer(
        final int width,
        final int height,
        final double pixelSize,
        final Integer limit,
        final ReferencedEnvelope envelope) {
      super(limit);
      this.width = width;
      this.height = height;
      this.pixelSize = pixelSize;
      this.envelope = envelope;
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      VectorQueryBuilder bldr =
          VectorQueryBuilder.newBuilder().addTypeName(
              components.getAdapter().getTypeName()).setAuthorizations(
                  transaction.composeAuthorizations()).constraints(
                      createQueryConstraints(index, query, spatialOnly));
      if (index != null) {
        bldr.indexName(index.getName());
      }
      if (limit != null) {
        bldr = bldr.limit(limit);
      }
      if (subsetRequested()) {
        bldr = bldr.subsetFields(components.getAdapter().getTypeName(), getSubset());
      }
      final double east = envelope.getMaxX();
      final double west = envelope.getMinX();
      final double north = envelope.getMaxY();
      final double south = envelope.getMinY();

      try {
        final AffineTransform worldToScreen =
            RendererUtilities.worldToScreenTransform(
                new ReferencedEnvelope(
                    new Envelope(west, east, south, north),
                    envelope.getCoordinateReferenceSystem()),
                new Rectangle(width, height));
        final MathTransform2D fullTransform =
            (MathTransform2D) ProjectiveTransform.create(worldToScreen);
        // calculate spans
        try {
          if (index != null) {
            final double[] spans =
                Decimator.computeGeneralizationDistances(
                    fullTransform.inverse(),
                    new Rectangle(width, height),
                    pixelSize);
            final NumericDimensionDefinition[] dimensions =
                index.getIndexStrategy().getOrderedDimensionDefinitions();
            final double[] maxResolutionSubsampling = new double[dimensions.length];
            for (int i = 0; i < dimensions.length; i++) {
              if (SpatialIndexUtils.isLongitudeDimension(dimensions[i])) {
                maxResolutionSubsampling[i] = spans[0];
              } else if (SpatialIndexUtils.isLatitudeDimension(dimensions[i])) {
                maxResolutionSubsampling[i] = spans[1];
              } else {
                // Ignore all other dimensions
                maxResolutionSubsampling[i] = 0;
              }
            }
            bldr =
                bldr.addHint(
                    DataStoreUtils.MAX_RESOLUTION_SUBSAMPLING_PER_DIMENSION,
                    maxResolutionSubsampling);
          }
          return components.getDataStore().query(bldr.build());
        } catch (final TransformException e) {
          throw new IllegalArgumentException("Unable to compute generalization distance", e);
        }
      } catch (final MismatchedDimensionException e) {
        throw new IllegalArgumentException("Unable to create Reference Envelope", e);
      }
    }
  }

  private class RenderQueryIssuer extends BaseIssuer implements QueryIssuer {
    final DistributedRenderOptions renderOptions;

    public RenderQueryIssuer(final Integer limit, final DistributedRenderOptions renderOptions) {
      super(limit);
      this.renderOptions = renderOptions;
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      final VectorAggregationQueryBuilder<DistributedRenderOptions, DistributedRenderResult> bldr =
          (VectorAggregationQueryBuilder) VectorAggregationQueryBuilder.newBuilder().setAuthorizations(
              transaction.composeAuthorizations());
      if (index != null) {
        bldr.indexName(index.getName());
      }
      bldr.aggregate(
          components.getAdapter().getTypeName(),
          new DistributedRenderAggregation(renderOptions)).constraints(
              createQueryConstraints(index, query, spatialOnly));
      final DistributedRenderResult result = components.getDataStore().aggregate(bldr.build());
      return new CloseableIterator.Wrapper<>(
          Iterators.singletonIterator(
              SimpleFeatureBuilder.build(
                  GeoWaveFeatureCollection.getDistributedRenderFeatureType(),
                  new Object[] {result, renderOptions},
                  "render")));
    }
  }

  public CloseableIterator<SimpleFeature> renderData(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final Integer limit,
      final DistributedRenderOptions renderOptions) {
    return issueQuery(jtsBounds, timeBounds, new RenderQueryIssuer(limit, renderOptions));
  }

  public CloseableIterator<SimpleFeature> getData(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final int width,
      final int height,
      final double pixelSize,
      final ReferencedEnvelope envelope,
      final Integer limit) {
    return issueQuery(
        jtsBounds,
        timeBounds,
        new EnvelopeQueryIssuer(width, height, pixelSize, limit, envelope));
  }

  public CloseableIterator<SimpleFeature> getData(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final Integer limit) {
    if (filter instanceof FidFilterImpl) {
      final Set<String> fids = ((FidFilterImpl) filter).getFidsSet();
      final byte[][] ids = new byte[fids.size()][];
      int i = 0;
      for (final String fid : fids) {
        ids[i++] = StringUtils.stringToBinary(fid);
      }

      final Index[] writeIndices = components.getAdapterIndices();
      final String queryIndexName =
          ((writeIndices != null) && (writeIndices.length > 0)) ? writeIndices[0].getName() : null;
      VectorQueryBuilder bldr =
          VectorQueryBuilder.newBuilder().addTypeName(
              components.getAdapter().getTypeName()).indexName(queryIndexName).setAuthorizations(
                  transaction.composeAuthorizations());
      if (limit != null) {
        bldr = bldr.limit(limit);
      }
      if (subsetRequested()) {
        bldr = bldr.subsetFields(components.getAdapter().getTypeName(), getSubset());
      }

      return components.getDataStore().query(
          bldr.constraints(bldr.constraintsFactory().dataIds(ids)).build());
    }
    return issueQuery(jtsBounds, timeBounds, new BaseIssuer(limit));
  }

  public GeoWaveFeatureCollection getFeatureCollection() {
    return featureCollection;
  }

  private CloseableIterator<SimpleFeature> interweaveTransaction(
      final Integer limit,
      final Filter filter,
      final CloseableIterator<SimpleFeature> it) {
    return transaction.interweaveTransaction(limit, filter, it);
  }

  protected TemporalConstraintsSet clipIndexedTemporalConstraints(
      final TemporalConstraintsSet constraintsSet) {
    return QueryIndexHelper.clipIndexedTemporalConstraints(
        transaction.getDataStatistics(),
        components.getAdapter().getTimeDescriptors(),
        constraintsSet);
  }

  protected Geometry clipIndexedBBOXConstraints(final Geometry bbox) {
    return QueryIndexHelper.clipIndexedBBOXConstraints(
        transaction.getDataStatistics(),
        components.getAdapter().getFeatureType(),
        components.getCRS(),
        bbox);
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

    if ((query.getFilter() != null)
        && !components.getGTstore().getDataStoreOptions().isServerSideLibraryEnabled()) {
      final ExtractAttributesFilter attributesVisitor = new ExtractAttributesFilter();
      final Object obj = query.getFilter().accept(attributesVisitor, null);

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
