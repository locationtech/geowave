/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Iterator;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.store.DataFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderOptions;
import org.locationtech.geowave.adapter.vector.render.DistributedRenderResult;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
//import org.locationtech.geowave.core.geotime.util.CellCounter;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.ExtractTimeFilterVisitor;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a helper for the GeoWave GeoTools data store. It represents a collection of feature
 * data by encapsulating a GeoWave reader and a query object in order to open the appropriate cursor
 * to iterate over data. It uses Keys within the Query hints to determine whether to perform special
 * purpose queries such as decimation, distributed rendering, subsampling, and heatmap processes.
 */
public class GeoWaveFeatureCollection extends DataFeatureCollection {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveFeatureCollection.class);
  private final GeoWaveFeatureReader reader;
  private CloseableIterator<SimpleFeature> featureCursor;
  private final Query query;
  private static SimpleFeatureType distributedRenderFeatureType;

  public GeoWaveFeatureCollection(final GeoWaveFeatureReader reader, final Query query) {
    System.out.println("GWFC 4. STARTING GeoWaveFeatureCollection");
    this.reader = reader;
    this.query =
        validateQuery(GeoWaveFeatureCollection.getSchema(reader, query).getTypeName(), query);
    System.out.println("\tREADER: " + reader);
    System.out.println("\tQUERY: " + query);
  }

  @Override
  public int getCount() {
    System.out.println("GWFC 5. STARTING getCount()");
    
    if (query.getFilter().equals(Filter.INCLUDE)) {
      // GEOWAVE-60 optimization
      final CountValue count =
          reader.getTransaction().getDataStatistics().getAdapterStatistic(
              CountStatistic.STATS_TYPE);
      System.out.println("\tCOUNT: " + count);
      if (count != null) {        
        return count.getValue().intValue();
      }
    } else if (query.getFilter().equals(Filter.EXCLUDE)) {
      return 0;
    }

    QueryConstraints constraints;
    try {
      constraints = getQueryConstraints();
      System.out.println("\tCONSTRAINTS: " + constraints);

      return (int) reader.getCountInternal(
          constraints.jtsBounds,
          constraints.timeBounds,
          constraints.limit);
    } catch (TransformException | FactoryException e) {

      LOGGER.warn("Unable to transform geometry, can't get count", e);
    }
    // fallback
    return 0;
  }

  @Override
  public ReferencedEnvelope getBounds() {
    System.out.println("STARTING getBounds from GeoWaveFeatureCollection.java");

    double minx = Double.MAX_VALUE, maxx = -Double.MAX_VALUE, miny = Double.MAX_VALUE,
        maxy = -Double.MAX_VALUE;
    
    System.out.println("\tminx init: " + minx);
    System.out.println("\tmaxx init: " + maxx);
    System.out.println("\tminy init: " + miny);
    System.out.println("\tmaxy init: " + maxy);    
    
    try {
      // GEOWAVE-60 optimization
      final BoundingBoxValue boundingBox =
          reader.getTransaction().getDataStatistics().getFieldStatistic(
              BoundingBoxStatistic.STATS_TYPE,
              reader.getFeatureType().getGeometryDescriptor().getLocalName());
      
      System.out.println("\tBBOX: " + boundingBox);

      if (boundingBox != null) {
        return new ReferencedEnvelope(
            boundingBox.getMinX(),
            boundingBox.getMaxX(),
            boundingBox.getMinY(),
            boundingBox.getMaxY(),
            reader.getFeatureType().getCoordinateReferenceSystem());
      }
      final Iterator<SimpleFeature> iterator = openIterator();
      if (!iterator.hasNext()) {
        return null;
      }
      while (iterator.hasNext()) {
        final BoundingBox bbox = iterator.next().getBounds();
        minx = Math.min(bbox.getMinX(), minx);
        maxx = Math.max(bbox.getMaxX(), maxx);
        miny = Math.min(bbox.getMinY(), miny);
        maxy = Math.max(bbox.getMaxY(), maxy);
      }
      close(iterator);
      
      System.out.println("\tminx: " + minx);
      System.out.println("\tmaxx: " + maxx);
      System.out.println("\tminy: " + miny);
      System.out.println("\tmaxy: " + maxy);    
      
    } catch (final Exception e) {
      LOGGER.warn("Error calculating bounds", e);
      return new ReferencedEnvelope(-180, 180, -90, 90, GeometryUtils.getDefaultCRS());
    }
    return new ReferencedEnvelope(minx, maxx, miny, maxy, GeometryUtils.getDefaultCRS());
  }

  @Override
  public SimpleFeatureType getSchema() {
    System.out.println("GWFC 1. STARTING getSchema");
    
    if (isDistributedRenderQuery()) {
      return getDistributedRenderFeatureType();
    }
    return reader.getFeatureType();
  }

  public static synchronized SimpleFeatureType getDistributedRenderFeatureType() {
    System.out.println("STARTING getDistributedRenderFeatureType from GeoWaveFeatureCollection.java");
    
    if (distributedRenderFeatureType == null) {
      distributedRenderFeatureType = createDistributedRenderFeatureType();
    }
    System.out.println("\tdistributedRenderFeatureType: " + distributedRenderFeatureType);
    return distributedRenderFeatureType;
  }

  private static SimpleFeatureType createDistributedRenderFeatureType() {
    System.out.println("STARTING createDistributedRenderFeatureType from GeoWaveFeatureCollection.java");
    
    final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
    typeBuilder.setName("distributed_render");
    typeBuilder.add("result", DistributedRenderResult.class);
    typeBuilder.add("options", DistributedRenderOptions.class);
    return typeBuilder.buildFeatureType();
  }

  protected boolean isDistributedRenderQuery() {
    System.out.println("GWFC 16. STARTING isDistributedRenderQuery()");
    return GeoWaveFeatureCollection.isDistributedRenderQuery(query);
  }

  protected static final boolean isDistributedRenderQuery(final Query query) {
    System.out.println("GWFC 2. STARTING isDistributedRenderQuery(query)");
    
    return query.getHints().containsKey(DistributedRenderProcess.OPTIONS);
  }

  private static SimpleFeatureType getSchema(final GeoWaveFeatureReader reader, final Query query) {
    System.out.println("GWFC 13. STARTING getSchema");
    
    if (GeoWaveFeatureCollection.isDistributedRenderQuery(query)) {
      System.out.println("\tis a distributed render query");
      return getDistributedRenderFeatureType();
    }
    System.out.println("\tfeature type: " + reader.getComponents().getFeatureType());
    return reader.getComponents().getFeatureType();
  }

  protected QueryConstraints getQueryConstraints() throws TransformException, FactoryException {
    System.out.println("GWFC 6. STARTING getQueryConstraints");
    
    final ReferencedEnvelope referencedEnvelope = getEnvelope(query);
    final Geometry jtsBounds;
    final TemporalConstraintsSet timeBounds;
    if (reader.getGeoWaveFilter() == null
        || query.getHints().containsKey(SubsampleProcess.SUBSAMPLE_ENABLED)
        || query.getHints().containsKey(GeoWaveHeatMapFinal.HEATMAP_ENABLED)) {  //HEATMAP
      System.out.println("\t**PLUGIN ENABLED - GeoWaveFeatureCollection.java");
      jtsBounds = getBBox(query, referencedEnvelope);
      timeBounds = getBoundedTime(query);
    } else {
      // This will be handled by the geowave filter
      jtsBounds = null;
      timeBounds = null;
    }
    Integer limit = getLimit(query);
    final Integer startIndex = getStartIndex(query);

    // limit becomes a 'soft' constraint since GeoServer will enforce
    // the limit
    final Long max =
        (limit != null) ? limit.longValue() + (startIndex == null ? 0 : startIndex.longValue())
            : null;
    // limit only used if less than an integer max value.
    limit = ((max != null) && (max.longValue() < Integer.MAX_VALUE)) ? max.intValue() : null;
    
    System.out.println("\tstartIndex: " + startIndex);
    System.out.println("\tjtsBounds: " + jtsBounds);
    System.out.println("\ttimeBounds: " + timeBounds);
    System.out.println("\treferencedEnvelope: " + referencedEnvelope);
    System.out.println("\tlimit: " + limit);
    
    return new QueryConstraints(jtsBounds, timeBounds, referencedEnvelope, limit);
  }

  @Override
  protected Iterator<SimpleFeature> openIterator() {
    System.out.println("GWFC 14. STARTING openIterator");
    try {
      return openIterator(getQueryConstraints());

    } catch (TransformException | FactoryException e) {
      LOGGER.warn("Unable to transform geometry", e);
    }
    return featureCursor;
  }

  private Iterator<SimpleFeature> openIterator(final QueryConstraints constraints) {
    System.out.println("GWFC 11.5. STARTING openIterator");

    if (reader.getGeoWaveFilter() == null
        && (((constraints.jtsBounds != null) && constraints.jtsBounds.isEmpty())
            || ((constraints.timeBounds != null) && constraints.timeBounds.isEmpty()))) {
      // return nothing if either constraint is empty
      featureCursor = reader.getNoData();
    } else if (query.getFilter() == Filter.EXCLUDE) {
      featureCursor = reader.getNoData();
    } else if (isDistributedRenderQuery()) {
      featureCursor =
          reader.renderData(
              constraints.jtsBounds,
              constraints.timeBounds,
              constraints.limit,
              (DistributedRenderOptions) query.getHints().get(DistributedRenderProcess.OPTIONS));
    } else if (query.getHints().containsKey(SubsampleProcess.OUTPUT_WIDTH)
        && query.getHints().containsKey(SubsampleProcess.OUTPUT_HEIGHT)
        && query.getHints().containsKey(SubsampleProcess.OUTPUT_BBOX)) {
      double pixelSize = 1;
      if (query.getHints().containsKey(SubsampleProcess.PIXEL_SIZE)) {
        pixelSize = (Double) query.getHints().get(SubsampleProcess.PIXEL_SIZE);
      }
      featureCursor =
          reader.getData(
              constraints.jtsBounds,
              constraints.timeBounds,
              (Integer) query.getHints().get(SubsampleProcess.OUTPUT_WIDTH),
              (Integer) query.getHints().get(SubsampleProcess.OUTPUT_HEIGHT),
              pixelSize,
              constraints.referencedEnvelope,
              constraints.limit);

      //----------------------HEATMAP-------------------------------------------------
    } else if (query.getHints().containsKey(GeoWaveHeatMapFinal.OUTPUT_WIDTH)
        && query.getHints().containsKey(GeoWaveHeatMapFinal.OUTPUT_HEIGHT)
        && query.getHints().containsKey(GeoWaveHeatMapFinal.OUTPUT_BBOX)) {
      System.out.println("\tHEATMAP ENABLED PROCESS in GWFC.java");

      System.out.println("\tOUTPUT_BBOX: " + GeoWaveHeatMapFinal.OUTPUT_BBOX);
      
      // ORIGINAL NON-AGGREGATION METHOD: This gets all the data points - Default for testing purposes only (WORKS!)
//      featureCursor =
//          reader.getData(constraints.jtsBounds, constraints.timeBounds, constraints.limit);
      
      // NEW HEAT MAP AGGREGATION
      featureCursor = new CloseableIterator.Wrapper<SimpleFeature> (DataUtilities.iterator(reader.getDataHeatMap(
          constraints.jtsBounds, 
          constraints.timeBounds,
          (ReferencedEnvelope) query.getHints().get(GeoWaveHeatMapFinal.OUTPUT_BBOX),
          (Integer) query.getHints().get(GeoWaveHeatMapFinal.OUTPUT_WIDTH),
          (Integer) query.getHints().get(GeoWaveHeatMapFinal.OUTPUT_HEIGHT),
          constraints.limit)));
      //TODO: pass in OUTPUT_BBOX here as the envelope to use later to calc the GeoHash precision to use.

      //------------------------------------------------------------------------------
      
    } else {
      featureCursor =
          reader.getData(constraints.jtsBounds, constraints.timeBounds, constraints.limit);
    }
    System.out.println("\treturning featureCursor: " + featureCursor);
    return featureCursor;
  }

  private ReferencedEnvelope getEnvelope(final Query query)
      throws TransformException, FactoryException {
    System.out.println("GWFC 7. STARTING getEnvelope");
    
    if (query.getHints().containsKey(SubsampleProcess.OUTPUT_BBOX)) {
      return ((ReferencedEnvelope) query.getHints().get(SubsampleProcess.OUTPUT_BBOX)).transform(
          reader.getFeatureType().getCoordinateReferenceSystem(),
          true);
    }
    //-------------------------------HEATMAP-------------------------------------------------------------
//    if (query.getHints().containsKey(HeatMapProcess.OUTPUT_BBOX)) {
    if (query.getHints().containsKey(GeoWaveHeatMapFinal.OUTPUT_BBOX)) {
      System.out.println("\tgetEnvelope for HEATMAP in GWFC.java");
//      return ((ReferencedEnvelope) query.getHints().get(HeatMapProcess.OUTPUT_BBOX)).transform(
      return ((ReferencedEnvelope) query.getHints().get(GeoWaveHeatMapFinal.OUTPUT_BBOX)).transform(
          reader.getFeatureType().getCoordinateReferenceSystem(),
          true);
    }
    //----------------------------------------------------------------------------------------------------
    return null;
  }

  private Geometry getBBox(final Query query, final ReferencedEnvelope envelope) {
    System.out.println("GWFC 7.5. STARTING getBBox");
    
    if (envelope != null) {
      return new GeometryFactory().toGeometry(envelope);
    }
    final String geomAtrributeName =
        reader.getComponents().getFeatureType().getGeometryDescriptor().getLocalName();
    final ExtractGeometryFilterVisitorResult geoAndCompareOp =
        ExtractGeometryFilterVisitor.getConstraints(
            query.getFilter(),
            reader.getComponents().getCRS(),
            geomAtrributeName);
    if (geoAndCompareOp == null) {
      return reader.clipIndexedBBOXConstraints(null);
    } else {
      return reader.clipIndexedBBOXConstraints(geoAndCompareOp.getGeometry());
    }
  }

  private Query validateQuery(final String typeName, final Query query) {
    System.out.println("GWFC 3. STARTING validateQuery");
    
    return query == null ? new Query(typeName, Filter.EXCLUDE) : query;
  }

  private Integer getStartIndex(final Query query) {
    System.out.println("GWFC 10. STARTING getStartIndex");
    
    return query.getStartIndex();
  }

  private Integer getLimit(final Query query) {
    System.out.println("GWFC 9. STARTING getLimit");
    
    if (!query.isMaxFeaturesUnlimited() && (query.getMaxFeatures() >= 0)) {
      return query.getMaxFeatures();
    }
    return null;
  }

  @Override
  public void accepts(
      final org.opengis.feature.FeatureVisitor visitor,
      final org.opengis.util.ProgressListener progress) throws IOException {
    System.out.println("STARTING accepts from GeoWaveFeatureCollection.java");
    
    if (!GeoWaveGTPluginUtils.accepts(
        reader.getComponents().getStatsStore(),
        reader.getComponents().getAdapter(),
        visitor,
        progress,
        reader.getFeatureType())) {
      DataUtilities.visit(this, visitor, progress);
    }
  }

  /**
   * @param query the query
   * @return the temporal constraints of the query
   */
  protected TemporalConstraintsSet getBoundedTime(final Query query) {
    System.out.println("GWFC 8. STARTING getBoundedTime");
    
    if (query == null) {
      return null;
    }
    final TemporalConstraintsSet constraints =
        new ExtractTimeFilterVisitor(
            reader.getComponents().getAdapter().getTimeDescriptors()).getConstraints(query);
    return constraints.isEmpty() ? null : reader.clipIndexedTemporalConstraints(constraints);
  }

  @Override
  public FeatureReader<SimpleFeatureType, SimpleFeature> reader() {
    System.out.println("STARTING reader from GeoWaveFeatureCollection.java");
    
    return reader;
  }

  @Override
  protected void closeIterator(final Iterator<SimpleFeature> close) {
    System.out.println("GWFC 17. STARTING closeIterator");
    
    featureCursor.close();
  }

  public Iterator<SimpleFeature> getOpenIterator() {
    System.out.println("GWFC 12. STARTING getOpenIterator");
    //TODO: THIS ITERATOR ITERATES OVER ALL FEATURES TWICE!!!  WHY??????
    
    return featureCursor;
  }

  @Override
  public void close(final FeatureIterator<SimpleFeature> iterator) {
    System.out.println("STARTING close ITERATOR from GeoWaveFeatureCollection.java");
    
    featureCursor = null;
    super.close(iterator);
  }

  @Override
  public boolean isEmpty() {
    System.out.println("STARTING isEmpty from GeoWaveFeatureCollection.java");
    
    try {
      return !reader.hasNext();
    } catch (final IOException e) {
      LOGGER.warn("Error checking reader", e);
    }
    return true;
  }

  private static class QueryConstraints {    
    Geometry jtsBounds;
    TemporalConstraintsSet timeBounds;
    ReferencedEnvelope referencedEnvelope;
    Integer limit;

    public QueryConstraints(
        final Geometry jtsBounds,
        final TemporalConstraintsSet timeBounds,
        final ReferencedEnvelope referencedEnvelope,
        final Integer limit) {
      super();
      this.jtsBounds = jtsBounds;
      this.timeBounds = timeBounds;
      this.referencedEnvelope = referencedEnvelope;
      this.limit = limit;
      
      System.out.println("GWFC 11. STARTING QueryConstraints");
    }
  }
}
