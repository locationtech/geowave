/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.SpatialIndexUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class SpatialTemporalDimensionalityTypeProvider implements
    DimensionalityTypeProviderSpi<SpatialTemporalOptions> {
  private static final String DEFAULT_SPATIAL_TEMPORAL_ID_STR = "ST_IDX";
  // this is chosen to place metric CRSs always in the same bin
  public static final double DEFAULT_UNBOUNDED_CRS_INTERVAL = 40075017;

  // TODO should we use different default IDs for all the different
  // options, for now lets just use one
  public static final NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS =
      new NumericDimensionDefinition[] {
          new LongitudeDefinition(),
          new LatitudeDefinition(true),
          new TimeDefinition(SpatialTemporalOptions.DEFAULT_PERIODICITY)};

  @SuppressWarnings("rawtypes")
  public static NumericDimensionField[] getSpatialTemporalFields(
      final @Nullable Integer geometryPrecision) {
    return new NumericDimensionField[] {
        new LongitudeField(geometryPrecision),
        new LatitudeField(geometryPrecision, true),
        new TimeField(SpatialTemporalOptions.DEFAULT_PERIODICITY)};
  }

  public SpatialTemporalDimensionalityTypeProvider() {}

  @Override
  public String getDimensionalityTypeName() {
    return "spatial_temporal";
  }

  @Override
  public String getDimensionalityTypeDescription() {
    return "This dimensionality type matches all indices that only require Geometry and Time.";
  }

  @Override
  public SpatialTemporalOptions createOptions() {
    return new SpatialTemporalOptions();
  }

  @Override
  public Index createIndex(final DataStore dataStore, final SpatialTemporalOptions options) {
    return createIndexFromOptions(options);
  }

  public static Index createIndexFromOptions(final SpatialTemporalOptions options) {

    NumericDimensionDefinition[] dimensions;
    NumericDimensionField<?>[] fields = null;
    CoordinateReferenceSystem crs = null;
    boolean isDefaultCRS;
    String crsCode = null;
    final Integer geometryPrecision = options.getGeometryPrecision();

    if ((options.crs == null)
        || options.crs.isEmpty()
        || options.crs.equalsIgnoreCase(GeometryUtils.DEFAULT_CRS_STR)) {
      dimensions = SPATIAL_TEMPORAL_DIMENSIONS;
      fields = getSpatialTemporalFields(geometryPrecision);
      isDefaultCRS = true;
      crsCode = "EPSG:4326";
    } else {
      crs = GeometryUtils.decodeCRS(options.crs);
      final CoordinateSystem cs = crs.getCoordinateSystem();
      isDefaultCRS = false;
      crsCode = options.crs;
      dimensions = new NumericDimensionDefinition[cs.getDimension() + 1];
      fields = new NumericDimensionField[dimensions.length];

      for (int d = 0; d < (dimensions.length - 1); d++) {
        final CoordinateSystemAxis csa = cs.getAxis(d);
        if (!isUnbounded(csa)) {
          if (d == 0) {
            dimensions[d] =
                new CustomCRSBoundedSpatialDimensionX(csa.getMinimumValue(), csa.getMaximumValue());
            fields[d] =
                new CustomCRSSpatialField(
                    (CustomCRSBoundedSpatialDimensionX) dimensions[d],
                    geometryPrecision,
                    crs);
          }
          if (d == 1) {
            dimensions[d] =
                new CustomCRSBoundedSpatialDimensionY(csa.getMinimumValue(), csa.getMaximumValue());
            fields[d] =
                new CustomCRSSpatialField(
                    (CustomCRSBoundedSpatialDimensionY) dimensions[d],
                    geometryPrecision,
                    crs);
          }
        } else {
          if (d == 0) {
            dimensions[d] =
                new CustomCRSUnboundedSpatialDimensionX(DEFAULT_UNBOUNDED_CRS_INTERVAL, (byte) d);
            fields[d] =
                new CustomCRSSpatialField(
                    (CustomCRSUnboundedSpatialDimensionX) dimensions[d],
                    geometryPrecision,
                    crs);
          }
          if (d == 1) {
            dimensions[d] =
                new CustomCRSUnboundedSpatialDimensionY(DEFAULT_UNBOUNDED_CRS_INTERVAL, (byte) d);
            fields[d] =
                new CustomCRSSpatialField(
                    (CustomCRSUnboundedSpatialDimensionY) dimensions[d],
                    geometryPrecision,
                    crs);
          }
        }
      }

      dimensions[dimensions.length - 1] = new TimeDefinition(options.periodicity);
      fields[dimensions.length - 1] = new TimeField(options.periodicity);
    }

    BasicIndexModel indexModel = null;
    if (isDefaultCRS) {
      indexModel = new BasicIndexModel(fields);
    } else {
      indexModel = new CustomCrsIndexModel(fields, crsCode);
    }

    String combinedArrayID;
    if (isDefaultCRS) {
      combinedArrayID =
          DEFAULT_SPATIAL_TEMPORAL_ID_STR + "_" + options.bias + "_" + options.periodicity;
    } else {
      combinedArrayID =
          DEFAULT_SPATIAL_TEMPORAL_ID_STR
              + "_"
              + (crsCode.substring(crsCode.indexOf(":") + 1))
              + "_"
              + options.bias
              + "_"
              + options.periodicity;
    }
    final String combinedId = combinedArrayID;

    return new CustomNameIndex(
        XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
            dimensions,
            new int[] {
                options.bias.getSpatialPrecision(),
                options.bias.getSpatialPrecision(),
                options.bias.getTemporalPrecision()},
            SFCType.HILBERT,
            options.maxDuplicates),
        indexModel,
        combinedId);
  }

  private static boolean isUnbounded(final CoordinateSystemAxis csa) {
    final double min = csa.getMinimumValue();
    final double max = csa.getMaximumValue();

    if (!Double.isFinite(max) || !Double.isFinite(min)) {
      return true;
    }
    return false;
  }

  public static enum Bias {
    TEMPORAL, BALANCED, SPATIAL;

    // converter that will be used later
    public static Bias fromString(final String code) {

      for (final Bias output : Bias.values()) {
        if (output.toString().equalsIgnoreCase(code)) {
          return output;
        }
      }

      return null;
    }

    public int getSpatialPrecision() {
      switch (this) {
        case SPATIAL:
          return 25;
        case TEMPORAL:
          return 10;
        case BALANCED:
        default:
          return 20;
      }
    }

    public int getTemporalPrecision() {
      switch (this) {
        case SPATIAL:
          return 10;
        case TEMPORAL:
          return 40;
        case BALANCED:
        default:
          return 20;
      }
    }
  }

  public static class BiasConverter implements IStringConverter<Bias> {
    @Override
    public Bias convert(final String value) {
      final Bias convertedValue = Bias.fromString(value);

      if (convertedValue == null) {
        throw new ParameterException(
            "Value "
                + value
                + "can not be converted to an index bias. "
                + "Available values are: "
                + StringUtils.join(Bias.values(), ", ").toLowerCase(Locale.ENGLISH));
      }
      return convertedValue;
    }
  }

  public static class UnitConverter implements IStringConverter<Unit> {

    @Override
    public Unit convert(final String value) {
      final Unit convertedValue = Unit.fromString(value);

      if (convertedValue == null) {
        throw new ParameterException(
            "Value "
                + value
                + "can not be converted to Unit. "
                + "Available values are: "
                + StringUtils.join(Unit.values(), ", ").toLowerCase(Locale.ENGLISH));
      }
      return convertedValue;
    }
  }

  public static boolean isSpatialTemporal(final Index index) {
    if (index == null) {
      return false;
    }

    return isSpatialTemporal(index.getIndexStrategy());
  }

  public static boolean isSpatialTemporal(final NumericIndexStrategy indexStrategy) {
    if ((indexStrategy == null) || (indexStrategy.getOrderedDimensionDefinitions() == null)) {
      return false;
    }
    final NumericDimensionDefinition[] dimensions = indexStrategy.getOrderedDimensionDefinitions();
    if (dimensions.length < 3) {
      return false;
    }
    boolean hasLat = false, hasLon = false, hasTime = false;
    for (final NumericDimensionDefinition definition : dimensions) {
      if (definition instanceof TimeDefinition) {
        hasTime = true;
      } else if (SpatialIndexUtils.isLatitudeDimension(definition)) {
        hasLat = true;
      } else if (SpatialIndexUtils.isLongitudeDimension(definition)) {
        hasLon = true;
      }
    }
    return hasTime && hasLat && hasLon;
  }
}
