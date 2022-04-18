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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.AttributeExpressionImpl;
import org.geotools.filter.FidFilterImpl;
import org.geotools.filter.spatial.BBOXImpl;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.util.factory.Hints;
import org.jaitools.numeric.Statistic;
//import org.locationtech.geowave.adapter.vector.plugin.GeoWaveHeatMapFinal.HeatmapCellCounter;
//import org.locationtech.geowave.analytic.mapreduce.kde; //.GaussianFilter;
import org.locationtech.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderAggregation;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.adapter.vector.util.QueryIndexHelper;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.index.SpatialIndexFilter;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.aggregate.SpatialSimpleFeatureBinningStrategy;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.CQLToGeoWaveConversionException;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.CQLToGeoWaveFilterVisitor;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.geotime.util.SpatialIndexUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.query.aggregate.BinningAggregation;
import org.locationtech.geowave.core.store.query.aggregate.CountAggregation;
import org.locationtech.geowave.core.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.query.aggregate.FieldSumAggregation;
import org.locationtech.geowave.core.store.query.aggregate.OptimalCountAggregation;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.geowave.core.store.query.constraints.OptimalExpressionQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.expression.InvalidFilterException;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.query.AbstractStatisticQuery;
import org.locationtech.geowave.core.store.statistics.query.DataTypeStatisticQueryBuilder;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform2D;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.statistics.extended.StatisticType;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import tech.units.indriya.AbstractSystemOfUnits;
import org.locationtech.geowave.adapter.vector.plugin.heatmap.HeatMapAggregations;
import org.locationtech.geowave.adapter.vector.plugin.heatmap.HeatMapStatistics;
import org.locationtech.geowave.adapter.vector.plugin.heatmap.HeatMapUtils;

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
    System.out.println("READER 1. STARTING GeoWaveFeatureReader (CALLED MULTIPLE TIMES)");
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
    System.out.println("READER STARTING getTransaction");
    
    return transaction;
  }

  public GeoWaveDataStoreComponents getComponents() {
    System.out.println("READER 2. STARTING getComponents (CALLED MULTIPLE TIMES)");
    //TODO: THIS METHOD IS CALLED TWICE THEN AGAIN MULTIPLE TIMES; WHY?????
    
    return components;
  }

  public org.locationtech.geowave.core.store.query.filter.expression.Filter getGeoWaveFilter() {
    System.out.println("READER 5. STARTING getGeoWaveFilter (CALLED MULTIPLE TIMES)");
    //TODO: THIS METHOD IS CALLED MULTIPLE TIMES; WHY????
    
    return (org.locationtech.geowave.core.store.query.filter.expression.Filter) geoWaveFilter;
  }

  @Override
  public void close() throws IOException {
    System.out.println("READER 16. STARTING close()");
    
    if (featureCollection.getOpenIterator() != null) {
      featureCollection.closeIterator(featureCollection.getOpenIterator());
    }
  }

  @Override
  public SimpleFeatureType getFeatureType() {
    System.out.println("READER 12. STARTING getFeatureType (CALLED MULTIPLE TIMES)");
    
    return components.getFeatureType();
  }

  @Override
  public boolean hasNext() throws IOException {
    System.out.println("READER 18. STARTING hasNext (CALLED MULTIPLE TIMES)");
    
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
    System.out.println("READER 25. STARTING next() (CALLED MULTIPLE TIMES)");
    
    Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
    if (it != null) {
      return it.next();
    }
    it = featureCollection.openIterator();
    return it.next();
  }

  public CloseableIterator<SimpleFeature> getNoData() {
    System.out.println("READER 15. STARTING getNoData");
    
    return new CloseableIterator.Empty<>();
  }

  public long getCount() {
    System.out.println("READER 4. STARTING getCount");
    
    return featureCollection.getCount();
  }

  protected long getCountInternal(      
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final Integer limit) {
    System.out.println("READER 7. STARTING getCountInternal");
    
    final CountQueryIssuer countIssuer = new CountQueryIssuer(limit);
    issueQuery(jtsBounds, timeBounds, countIssuer);
    return countIssuer.count;
  }

  private BasicQueryByClass getQuery(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds) {
    System.out.println("READER 11. STARTING getQuery (CALLED MUTLIPLE TIMES)");
    System.out.println("\tjtsBounds: " + jtsBounds);
    System.out.println("\ttimeBounds: " + timeBounds);
    
    final GeoConstraintsWrapper geoConstraints =
        QueryIndexHelper.composeGeometricConstraints(getFeatureType(), jtsBounds);

    if (timeBounds == null) {
      System.out.println("\ttimeBounds is NULL - USE CONSTRAINTS");
      // if timeBounds are unspecified just use the geoConstraints
      return new ExplicitSpatialQuery(
          geoConstraints.getConstraints(),
          geoConstraints.getGeometry(),
          GeometryUtils.getCrsCode(components.getCRS()));
    } else {
      System.out.println("\ttimeBounds NOT NULL - USE CONSTRAINTS BY CLASS");

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
  
  
//  public CloseableIterator<SimpleFeature> issueQueryHeatmap(
  public FeatureIterator<SimpleFeature> issueQueryHeatmap(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final QueryIssuerHeatMap issuer) {
    System.out.println("READER 10. STARTING issueQuery: " + issuer);
    
    // Set defaults (to be overriden by user preferences)
    String queryType = GeoWaveHeatMapFinal.CNT_AGGR; //use this as default unless specified by user through UI.
    String weightAttr = "count"; //TODO: what should this be set to?
    int pixelsPerCell = 1; //set the default to 1 for now
    Boolean createStats = false; //set this to false for now

    if (this.query.getHints().containsKey(GeoWaveHeatMapFinal.HEATMAP_ENABLED)
        && (Boolean) this.query.getHints().get(GeoWaveHeatMapFinal.HEATMAP_ENABLED)) {
      System.out.println("\tREADER - GETTING HEATMAP USER PREFS");
      
      // Get user specified parameters
      queryType = (String) this.query.getHints().get(GeoWaveHeatMapFinal.QUERY_TYPE);      
      weightAttr = (String) this.query.getHints().get(GeoWaveHeatMapFinal.WEIGHT_ATTR);
      pixelsPerCell = (Integer) this.query.getHints().get(GeoWaveHeatMapFinal.PIXELS_PER_CELL);
      createStats = (Boolean) this.query.getHints().get(GeoWaveHeatMapFinal.CREATE_STATS);
    }
    
    System.out.println("\tREADER - QUERY TYPE: " + queryType);
    System.out.println("\tREADER - WEIGHT ATTR: " + weightAttr);
    System.out.println("\tREADER - PIXELS PER CELL: " + pixelsPerCell);
    
    return issuer.query(queryType, weightAttr, pixelsPerCell, createStats);
  }
  

  public CloseableIterator<SimpleFeature> issueQuery(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final QueryIssuer issuer) {
    System.out.println("READER 10. STARTING issueQuery: " + issuer);
    final List<CloseableIterator<SimpleFeature>> results = new ArrayList<>();
    boolean spatialOnly = false;
    if (this.query.getHints().containsKey(SubsampleProcess.SUBSAMPLE_ENABLED)
        && (Boolean) this.query.getHints().get(SubsampleProcess.SUBSAMPLE_ENABLED)) {
      spatialOnly = true;
    }
//    // -------------------------------------HEATMAP----------------------------------------------------
    //TODO: IS THIS NEEDED FOR INTIALIZING THE PLUGIN????
    if (this.query.getHints().containsKey(GeoWaveHeatMapFinal.HEATMAP_ENABLED)
        && (Boolean) this.query.getHints().get(GeoWaveHeatMapFinal.HEATMAP_ENABLED)) {
      System.out.println("\tREADER - ENABLE SPATIAL ONLY FOR HEATMAP PROCESS");
      spatialOnly = true;
      
      Hints heatMapHints = this.query.getHints();
      System.out.println("\tHINTS CNT: " + heatMapHints.size());
      // dataStore.aggregate(agg);
      // Make heatmap aggregation query issuer here?

    }

    // ------------------------------------------------------------------------------------------------
      
    if (!spatialOnly && getGeoWaveFilter() != null) {
      System.out.println("\tREADER - NOT JUST SPATIAL - SPATIAL ONLY = FALSE");
      results.add(issuer.query(null, null, spatialOnly));
    } else {
      System.out.println("\tREADER - JUST SPATIAL - SPATIAL ONLY = TRUE");
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
    System.out.println("\tREADER - RESULTS CNT: " + results.size());
    if (results.isEmpty()) {
      System.out.println("\tRETURNING NO DATA");
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
    System.out.println("READER STARTING hasTime");
    
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
    System.out.println("READER 14. STARTING createQueryConstraints");
    
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
    System.out.println("READER 3. STARTING getFilter (CALLED MULTIPLE TIMES)");
    
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
      
      System.out.println("READER 8. STARTING BaseIssuer (CALLED MULTIPLE TIMES)");
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      System.out.println("READER 20. STARTING query");
      
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
      System.out.println("READER 17. STARTING getFilter");
      
      return filter;
    }

    @Override
    public Integer getLimit() {
      System.out.println("READER 23. STARTING getLimit");
      
      return limit;
    }
  }

  private class CountQueryIssuer extends BaseIssuer implements QueryIssuer {
    private long count = 0;

    public CountQueryIssuer(final Integer limit) {
      super(limit);
      
      System.out.println("READER 9. STARTING CountQueryIssuer");      
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      System.out.println("READER 13. STARTING CountQueryIssuer CloseableIterator"); 
      
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
      
      System.out.println("READER STARTING EnvelopeQueryIssuer");
    }
    
    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      System.out.println("READER STARTING EnvelopeQueryIssuer CloseableIterator");
      
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
  
  
  // --------------------------HEATMAP----------------------------------------------------
  private class HeatMapQueryIssuer extends BaseIssuer implements QueryIssuerHeatMap {
    final Geometry jtsBounds;
    final ReferencedEnvelope outputBbox;
    final int width;
    final int height;

    public HeatMapQueryIssuer(
        final Geometry jtsBounds,
        final ReferencedEnvelope outputBbox,
        final int width,
        final int height,
        final Integer limit) {
      super(limit);
      this.jtsBounds = jtsBounds;
      this.outputBbox = outputBbox;
      this.width = width;
      this.height = height;

      System.out.println("READER STARTING HeatMapQueryIssuer");
    }

    public FeatureIterator<SimpleFeature> query(
        final String queryType,
        final String weightAttr,
        final Integer pixelsPerCell,
        final Boolean createStats) {
      System.out.println("READER STARTING HeatMapQueryIssuer CloseableIterator");
      
      System.out.println("\tQUERY TYPE: " + queryType);
      System.out.println("\tWEIGHT ATTR: " + weightAttr);
      System.out.println("\tPIXELS PER CELL: " + pixelsPerCell);
      System.out.println("\tCREATE STATS: " + createStats);

      System.out.println("\tOUTPUT HEIGHT: " + height);
      System.out.println("\tOUTPUT WIDTH: " + width);
      System.out.println("\tOUTPUT BBOX: " + outputBbox);
      
      SimpleFeatureCollection newFeatures = null;
      
      // Get an appropriate Geohash precision for the GeoServer extent
      int geohashPrec =
          HeatMapUtils.autoSelectGeohashPrecision(
              height,
              width,
              pixelsPerCell,
              jtsBounds);

      // Build the count aggregation query and get the resulting SimpleFeatureCollection
      if (queryType.equals(GeoWaveHeatMapFinal.CNT_AGGR)) {
        System.out.println("READER - PROCESSING COUNT AGGR");
        newFeatures =
            HeatMapAggregations.buildCountAggrQuery(
                components,
                geohashPrec,
                weightAttr);
      }
      
      // Build the sum aggregation query and get the resulting SimpleFeatureCollection
      if (queryType.equals(GeoWaveHeatMapFinal.SUM_AGGR)) {
        System.out.println("READER - PROCESSING SUM AGGR");
        newFeatures =
            HeatMapAggregations.buildFieldSumAggrQuery(
                components,
                geohashPrec,
                weightAttr);
      } 
      
      // Build the count statistics query and get the resulting SimpleFeatureCollection
      if (queryType.equals(GeoWaveHeatMapFinal.CNT_STATS)) {
        System.out.println("READER - PROCESSING COUNT STATS");
        newFeatures =
            HeatMapStatistics.buildCountStatsQuery(
                components,
                geohashPrec,
                weightAttr,
                createStats);
      }
      
      // Build the sum statistics query and get the resulting SimpleFeatureCollection
      if (queryType.equals(GeoWaveHeatMapFinal.SUM_STATS)) {
        System.out.println("READER - PROCESSING SUM STATS");
        newFeatures =
            HeatMapStatistics.buildFieldStatsQuery(
                components,
                geohashPrec,
                weightAttr,
                createStats);
      } 
      
      if (newFeatures == null) {
        System.out.println("\tYOU MUST SPECIFICY A QUERY TYPE: CNT_AGGR, SUM_AGGR, CNT_STATS, or SUM_STATS.");
        LOGGER.warn("YOU MUST SPECIFICY A QUERY TYPE: CNT_AGGR, SUM_AGGR, CNT_STATS, or SUM_STATS.");
      }

      SimpleFeatureIterator simpFeatIter = newFeatures.features();
      System.out.println("\tRETURNING SIMPLE FEATURE ITERATOR");
      return simpFeatIter;

    }
  }
  //---------------------------------------------------------------------------------------------
  

  private class RenderQueryIssuer extends BaseIssuer implements QueryIssuer {
    final DistributedRenderOptions renderOptions;

    public RenderQueryIssuer(final Integer limit, final DistributedRenderOptions renderOptions) {
      super(limit);
      this.renderOptions = renderOptions;
      
      System.out.println("READER STARTING RenderQueryIssuer");
    }

    @Override
    public CloseableIterator<SimpleFeature> query(
        final Index index,
        final BasicQueryByClass query,
        final boolean spatialOnly) {
      System.out.println("READER STARTING RenderQueryIssuer CloseableIterator");
      
      
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
    System.out.println("READER STARTING renderData");
    
    return issueQuery(jtsBounds, timeBounds, new RenderQueryIssuer(limit, renderOptions));
  }

  //------------------------------HEATMAP----------------------------------------------------------------------
  
  public interface CellCounter {
    public void increment(long cellId, double weight);
  }
  
  // Customizable way to get data as an iterator
  public CloseableIterator<SimpleFeature> getData(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final int width,
      final int height,
      final double pixelSize,
      final ReferencedEnvelope envelope,
      final Integer limit) {
    System.out.println("READER STARTING CloseableIterator");
    
    return issueQuery(
        jtsBounds,
        timeBounds,
        new EnvelopeQueryIssuer(width, height, pixelSize, limit, envelope));
  }
  
  //-------------------------HEATMAP---------------------------------------------------------
//  public CloseableIterator<SimpleFeature> getData(
  public FeatureIterator<SimpleFeature> getDataHeatMap(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final ReferencedEnvelope outputBbox,
      final int width,
      final int height,
      final Integer limit) {
    System.out.println("READER STARTING getData for HEATMAP");
    System.out.println("\tJTS Bounds: " + jtsBounds);
    System.out.println("\tOUTPUT BBOX: " + outputBbox);
    
    return issueQueryHeatmap(
        jtsBounds,
        timeBounds,
        new HeatMapQueryIssuer(jtsBounds, outputBbox, width, height, limit));
  }
  //-------------------------------------------------------------------------------------------

  public CloseableIterator<SimpleFeature> getData(
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBounds,
      final Integer limit) {
    System.out.println("READER 19. STARTING getData");
    
    if (filter instanceof FidFilterImpl) {
      System.out.println("\tFILTER INSTANCEOF FID FILTER IMPL");
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
    System.out.println("READER STARTING getFeatureCollection");
    
    return featureCollection;
  }

  private CloseableIterator<SimpleFeature> interweaveTransaction(
      final Integer limit,
      final Filter filter,
      final CloseableIterator<SimpleFeature> it) {
    System.out.println("READER 24. STARTING interweaveTransaction");
    
    return transaction.interweaveTransaction(limit, filter, it);
  }

  protected TemporalConstraintsSet clipIndexedTemporalConstraints(
      final TemporalConstraintsSet constraintsSet) {
    System.out.println("READER STARTING clipIndexedTemporalConstraints");
    
    return QueryIndexHelper.clipIndexedTemporalConstraints(
        transaction.getDataStatistics(),
        components.getAdapter().getTimeDescriptors(),
        constraintsSet);
  }

  protected Geometry clipIndexedBBOXConstraints(final Geometry bbox) {
    System.out.println("READER 6. STARTING clipIndexedBBOXConstraints (CALLED MUTLIPLE TIMES)");
    
    return QueryIndexHelper.clipIndexedBBOXConstraints(
        transaction.getDataStatistics(),
        components.getAdapter().getFeatureType(),
        components.getCRS(),
        bbox);
  }

  private boolean subsetRequested() {
    System.out.println("READER 21. STARTING subsetRequested");
    
    if (query == null) {
      return false;
    }
    return !(query.getPropertyNames() == Query.ALL_NAMES);
  }

  private String[] getSubset() {
    System.out.println("READER 22. STARTING getSubset");
    
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
