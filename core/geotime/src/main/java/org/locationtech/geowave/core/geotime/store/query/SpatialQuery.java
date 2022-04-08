/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialQuery extends AbstractVectorConstraints<ExplicitSpatialQuery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialQuery.class);

  public SpatialQuery() {
    super();
  }

  public SpatialQuery(final ExplicitSpatialQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitSpatialQuery newConstraints() {
    return new ExplicitSpatialQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasAtLeastSpatial(index);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter, final Index index) {
    return getFilter(adapter, index, delegateConstraints);
  }

  protected static Filter getFilter(
      final GeotoolsFeatureDataAdapter adapter,
      final Index index,
      final ExplicitSpatialQuery delegateConstraints) {
    final GeometryDescriptor geomDesc = adapter.getFeatureType().getGeometryDescriptor();
    final CoordinateReferenceSystem indexCrs = GeometryUtils.getIndexCrs(index);
    return GeometryUtils.geometryToSpatialOperator(
        transformToAdapter(indexCrs, delegateConstraints),
        geomDesc.getLocalName(),
        indexCrs);
  }

  private static Geometry transformToAdapter(
      final CoordinateReferenceSystem adapterCrs,
      final ExplicitSpatialQuery delegateConstraints) {
    final Geometry queryGeometry = delegateConstraints.getQueryGeometry();
    if (adapterCrs == null) {
      return queryGeometry;
    }
    final String indexCrsStr = GeometryUtils.getCrsCode(adapterCrs);
    if (indexCrsStr == null) {
      return queryGeometry;
    }
    if (GeometryUtils.crsMatches(delegateConstraints.getCrsCode(), indexCrsStr)
        || (queryGeometry == null)) {
      return queryGeometry;
    } else {
      CoordinateReferenceSystem crs = delegateConstraints.getCrs();
      if (crs == null) {
        String crsCode = delegateConstraints.getCrsCode();

        if ((crsCode == null) || crsCode.isEmpty()) {
          crsCode = GeometryUtils.DEFAULT_CRS_STR;
        }

        try {
          crs = CRS.decode(crsCode, true);
        } catch (final FactoryException e) {
          LOGGER.warn("Unable to decode spatial query crs", e);
        }
      }
      try {
        final MathTransform transform = CRS.findMathTransform(crs, adapterCrs, true);
        // transform geometry
        return JTS.transform(queryGeometry, transform);
      } catch (final FactoryException e) {
        LOGGER.warn("Unable to create coordinate reference system transform", e);
      } catch (MismatchedDimensionException | TransformException e) {
        LOGGER.warn("Unable to transform query geometry into index CRS", e);
      }
    }
    return queryGeometry;
  }
}
