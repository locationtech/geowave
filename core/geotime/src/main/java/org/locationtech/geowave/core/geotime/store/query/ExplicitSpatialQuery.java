/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TWKBReader;
import org.locationtech.geowave.core.geotime.util.TWKBWriter;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Spatial Query class represents a query in two dimensions. The constraint that is applied
 * represents an intersection operation on the query geometry.
 */
public class ExplicitSpatialQuery extends BasicQueryByClass {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExplicitSpatialQuery.class);

  private static class CrsCache {
    Geometry geometry;
    Map<String, List<MultiDimensionalNumericData>> constraintsPerIndexId;

    public CrsCache(
        final Geometry geometry,
        final Map<String, List<MultiDimensionalNumericData>> constraintsPerIndexId) {
      this.geometry = geometry;
      this.constraintsPerIndexId = constraintsPerIndexId;
    }
  }

  private Geometry queryGeometry;
  private String crsCode;
  private CompareOperation compareOp = CompareOperation.INTERSECTS;
  private BasicQueryCompareOperation nonSpatialCompareOp = BasicQueryCompareOperation.INTERSECTS;
  private final Map<String, CrsCache> crsCodeCache = new HashMap<>();
  private CoordinateReferenceSystem crs;

  /**
   * Convenience constructor used to construct a SpatialQuery object that has an X and Y dimension
   * (axis).
   *
   * @param queryGeometry spatial geometry of the query
   */
  public ExplicitSpatialQuery(final Geometry queryGeometry) {
    this(GeometryUtils.basicConstraintsFromGeometry(queryGeometry), queryGeometry);
  }

  public ExplicitSpatialQuery(final ConstraintsByClass constraints, final Geometry queryGeometry) {
    this(constraints, queryGeometry, (String) null);
  }

  public ExplicitSpatialQuery(
      final ConstraintsByClass constraints,
      final Geometry queryGeometry,
      final String crsCode) {
    this(
        constraints,
        queryGeometry,
        crsCode,
        CompareOperation.INTERSECTS,
        BasicQueryCompareOperation.INTERSECTS);
  }


  public ExplicitSpatialQuery(final Geometry queryGeometry, final String crsCode) {
    this(
        GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
        queryGeometry,
        crsCode,
        CompareOperation.INTERSECTS,
        BasicQueryCompareOperation.INTERSECTS);
  }

  /**
   * Convenience constructor used to construct a SpatialQuery object that has an X and Y dimension
   * (axis).
   *
   * @param queryGeometry spatial geometry of the query
   * @param compareOp the compare operation to use
   */
  public ExplicitSpatialQuery(final Geometry queryGeometry, final CompareOperation compareOp) {
    this(GeometryUtils.basicConstraintsFromGeometry(queryGeometry), queryGeometry, compareOp);
  }

  /**
   * Convenience constructor can be used when you already have linear constraints for the query. The
   * queryGeometry and compareOp is used for fine grained post filtering.
   *
   * @param constraints linear constraints
   * @param queryGeometry spatial geometry of the query
   * @param compareOp the compare operation to use
   */
  public ExplicitSpatialQuery(
      final ConstraintsByClass constraints,
      final Geometry queryGeometry,
      final CompareOperation compareOp) {
    this(constraints, queryGeometry, compareOp, BasicQueryCompareOperation.INTERSECTS);
  }

  public ExplicitSpatialQuery(
      final Geometry queryGeometry,
      final String crsCode,
      final CompareOperation compareOp) {
    this(
        GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
        queryGeometry,
        crsCode,
        compareOp == null ? CompareOperation.INTERSECTS : compareOp,
        BasicQueryCompareOperation.INTERSECTS);
  }

  /**
   * Convenience constructor can be used when you already have linear constraints for the query. The
   * queryGeometry and compareOp is used for fine grained post filtering.
   *
   * @param constraints linear constraints
   * @param queryGeometry spatial geometry of the query
   * @param compareOp predicate associated query geometry
   * @param nonSpatialCompareOp predicate associated non-spatial fields (i.e Time)
   */
  public ExplicitSpatialQuery(
      final ConstraintsByClass constraints,
      final Geometry queryGeometry,
      final CompareOperation compareOp,
      final BasicQueryCompareOperation nonSpatialCompareOp) {
    this(
        constraints,
        queryGeometry,
        null,
        compareOp == null ? CompareOperation.INTERSECTS : compareOp,
        nonSpatialCompareOp);
  }

  public ExplicitSpatialQuery(
      final ConstraintsByClass constraints,
      final Geometry queryGeometry,
      final String crsCode,
      final CompareOperation compareOp,
      final BasicQueryCompareOperation nonSpatialCompareOp) {
    super(constraints, nonSpatialCompareOp);
    this.crsCode = crsCode;
    this.queryGeometry = queryGeometry;
    this.compareOp = compareOp;
    this.nonSpatialCompareOp = nonSpatialCompareOp;
  }

  public ExplicitSpatialQuery() {
    super();
  }

  /** @return queryGeometry the spatial geometry of the SpatialQuery object */
  public Geometry getQueryGeometry() {
    return queryGeometry;
  }

  public String getCrsCode() {
    return crsCode;
  }

  public CoordinateReferenceSystem getCrs() {
    return crs;
  }

  @Override
  protected QueryFilter createQueryFilter(
      final MultiDimensionalNumericData constraints,
      final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
      final NumericDimensionField<?>[] unconstrainedDimensionDefinitions,
      final Index index) {
    return new SpatialQueryFilter(
        constraints,
        orderedConstrainedDimensionFields,
        unconstrainedDimensionDefinitions,
        internalGetGeometry(index),
        compareOp,
        nonSpatialCompareOp);
  }

  protected Geometry internalGetGeometry(final Index index) {
    final String indexCrsStr = getCrs(index.getIndexModel());
    CrsCache cache = crsCodeCache.get(indexCrsStr);
    if (cache != null) {
      return cache.geometry;
    }
    cache = transformToIndex(indexCrsStr, index);
    crsCodeCache.put(indexCrsStr, cache);
    return cache.geometry;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    final String indexCrsStr = getCrs(index.getIndexModel());
    CrsCache cache = crsCodeCache.get(indexCrsStr);
    if (cache != null) {
      List<MultiDimensionalNumericData> indexConstraints =
          cache.constraintsPerIndexId.get(index.getName());
      if (indexConstraints == null) {
        if (GeometryUtils.crsMatches(crsCode, indexCrsStr) || (queryGeometry == null)) {
          indexConstraints = super.getIndexConstraints(index);
        } else {
          indexConstraints = indexConstraintsFromGeometry(cache.geometry, index);
        }
        cache.constraintsPerIndexId.put(index.getName(), indexConstraints);
      }
      return indexConstraints;
    }
    cache = transformToIndex(indexCrsStr, index);
    crsCodeCache.put(indexCrsStr, cache);
    return cache.constraintsPerIndexId.get(index.getName());
  }

  private CrsCache transformToIndex(final String indexCrsStr, final Index index) {
    if (GeometryUtils.crsMatches(crsCode, indexCrsStr) || (queryGeometry == null)) {
      final List<MultiDimensionalNumericData> constraints = super.getIndexConstraints(index);
      final Map<String, List<MultiDimensionalNumericData>> constraintsPerIndexId = new HashMap<>();
      constraintsPerIndexId.put(index.getName(), constraints);
      return new CrsCache(queryGeometry, constraintsPerIndexId);
    } else {
      if (crs == null) {

        if ((crsCode == null) || crsCode.isEmpty()) {
          crsCode = GeometryUtils.DEFAULT_CRS_STR;
        }

        try {
          crs = CRS.decode(crsCode, true);
        } catch (final FactoryException e) {
          LOGGER.warn("Unable to decode spatial query crs", e);
        }
      }
      CoordinateReferenceSystem indexCrs;
      if (GeometryUtils.isDefaultCrs(indexCrsStr)) {
        indexCrs = GeometryUtils.getDefaultCRS();
      } else {
        indexCrs = ((CustomCrsIndexModel) index.getIndexModel()).getCrs();
      }
      try {
        final MathTransform transform = CRS.findMathTransform(crs, indexCrs, true);
        // transform geometry
        final Geometry indexCrsQueryGeometry = JTS.transform(queryGeometry, transform);
        final List<MultiDimensionalNumericData> indexConstraints =
            indexConstraintsFromGeometry(indexCrsQueryGeometry, index);
        final Map<String, List<MultiDimensionalNumericData>> constraintsPerIndexId =
            new HashMap<>();
        constraintsPerIndexId.put(index.getName(), indexConstraints);
        return new CrsCache(indexCrsQueryGeometry, constraintsPerIndexId);
      } catch (final FactoryException e) {
        LOGGER.warn("Unable to create coordinate reference system transform", e);
      } catch (MismatchedDimensionException | TransformException e) {
        LOGGER.warn("Unable to transform query geometry into index CRS", e);
      }
    }
    final List<MultiDimensionalNumericData> constraints = super.getIndexConstraints(index);
    final Map<String, List<MultiDimensionalNumericData>> constraintsPerIndexId = new HashMap<>();
    constraintsPerIndexId.put(index.getName(), constraints);
    return new CrsCache(queryGeometry, constraintsPerIndexId);
  }

  private static List<MultiDimensionalNumericData> indexConstraintsFromGeometry(
      final Geometry geom,
      final Index index) {
    return GeometryUtils.basicConstraintsFromGeometry(geom).getIndexConstraints(index);
  }

  private static String getCrs(final CommonIndexModel indexModel) {
    if (indexModel instanceof CustomCrsIndexModel) {
      if (GeometryUtils.isDefaultCrs(((CustomCrsIndexModel) indexModel).getCrs())) {
        return null;
      }
      return GeometryUtils.getCrsCode(((CustomCrsIndexModel) indexModel).getCrs());
    }
    return null;
  }

  @Override
  public byte[] toBinary() {
    final byte[] crsBinary =
        GeometryUtils.isDefaultCrs(crsCode) ? new byte[0] : StringUtils.stringToBinary(crsCode);
    final byte[] superBinary = super.toBinary();
    final byte[] geometryBinary = new TWKBWriter().write(queryGeometry);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            superBinary.length
                + geometryBinary.length
                + crsBinary.length
                + VarintUtils.unsignedIntByteLength(compareOp.ordinal())
                + VarintUtils.unsignedIntByteLength(nonSpatialCompareOp.ordinal())
                + VarintUtils.unsignedIntByteLength(crsBinary.length)
                + VarintUtils.unsignedIntByteLength(superBinary.length));
    VarintUtils.writeUnsignedInt(compareOp.ordinal(), buf);
    VarintUtils.writeUnsignedInt(nonSpatialCompareOp.ordinal(), buf);
    VarintUtils.writeUnsignedInt(crsBinary.length, buf);
    VarintUtils.writeUnsignedInt(superBinary.length, buf);
    buf.put(crsBinary);
    buf.put(superBinary);
    buf.put(geometryBinary);

    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    compareOp = CompareOperation.values()[VarintUtils.readUnsignedInt(buf)];
    nonSpatialCompareOp = BasicQueryCompareOperation.values()[VarintUtils.readUnsignedInt(buf)];

    final int crsBinaryLength = VarintUtils.readUnsignedInt(buf);
    final int superBinaryLength = VarintUtils.readUnsignedInt(buf);
    final byte[] crsBinary = ByteArrayUtils.safeRead(buf, crsBinaryLength);
    crsCode = crsBinary.length > 0 ? StringUtils.stringFromBinary(crsBinary) : null;
    final byte[] superBinary = ByteArrayUtils.safeRead(buf, superBinaryLength);
    super.fromBinary(superBinary);
    try {
      queryGeometry = new TWKBReader().read(buf);
    } catch (final ParseException e) {
      LOGGER.warn("Unable to read query geometry as well-known binary", e);
    }
  }
}
