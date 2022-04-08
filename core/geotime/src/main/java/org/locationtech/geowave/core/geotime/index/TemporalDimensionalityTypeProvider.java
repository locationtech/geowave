/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeIndexStrategy;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexFactory;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.BasicNumericDimensionField;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class TemporalDimensionalityTypeProvider implements
    DimensionalityTypeProviderSpi<TemporalOptions> {
  private static final String DEFAULT_TEMPORAL_ID_STR = "TIME_IDX";

  public static final NumericDimensionDefinition[] TEMPORAL_DIMENSIONS =
      new NumericDimensionDefinition[] {
          new TimeDefinition(SpatialTemporalOptions.DEFAULT_PERIODICITY)};

  public static final NumericDimensionField<?>[] TEMPORAL_FIELDS =
      new NumericDimensionField[] {new TimeField(SpatialTemporalOptions.DEFAULT_PERIODICITY)};

  public TemporalDimensionalityTypeProvider() {}

  @Override
  public String getDimensionalityTypeName() {
    return "temporal";
  }

  @Override
  public String getDimensionalityTypeDescription() {
    return "This dimensionality type matches all indices that only require Time.";
  }

  @Override
  public TemporalOptions createOptions() {
    return new TemporalOptions();
  }

  @Override
  public Index createIndex(final DataStore dataStore, final TemporalOptions options) {
    return createIndexFromOptions(options);
  }

  public static Index createIndexFromOptions(final TemporalOptions options) {

    if (!options.noTimeRanges) {
      final NumericDimensionDefinition[] dimensions = TEMPORAL_DIMENSIONS;
      final NumericDimensionField<?>[] fields = TEMPORAL_FIELDS;

      dimensions[dimensions.length - 1] = new TimeDefinition(options.periodicity);
      fields[dimensions.length - 1] = new TimeField(options.periodicity);

      final BasicIndexModel indexModel = new BasicIndexModel(fields);

      final String combinedArrayID = DEFAULT_TEMPORAL_ID_STR + "_" + options.periodicity;
      return new CustomNameIndex(
          XZHierarchicalIndexFactory.createFullIncrementalTieredStrategy(
              dimensions,
              new int[] {63},
              SFCType.HILBERT,
              options.maxDuplicates),
          indexModel,
          combinedArrayID);
    }

    final BasicIndexModel indexModel =
        new BasicIndexModel(
            new NumericDimensionField[] {
                new BasicNumericDimensionField<>(TimeField.DEFAULT_FIELD_ID, Long.class)});
    return new CustomNameIndex(new SimpleTimeIndexStrategy(), indexModel, DEFAULT_TEMPORAL_ID_STR);
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

  public static boolean isTemporal(final Index index) {
    if (index == null) {
      return false;
    }

    return isTemporal(index.getIndexStrategy());
  }

  public static boolean isTemporal(final NumericIndexStrategy indexStrategy) {
    if ((indexStrategy == null) || (indexStrategy.getOrderedDimensionDefinitions() == null)) {
      return false;
    }
    final NumericDimensionDefinition[] dimensions = indexStrategy.getOrderedDimensionDefinitions();
    if (dimensions.length < 1) {
      return false;
    }
    for (final NumericDimensionDefinition definition : dimensions) {
      if ((definition instanceof TimeDefinition) || (definition instanceof SimpleTimeDefinition)) {
        return true;
      }
    }
    return false;
  }
}
