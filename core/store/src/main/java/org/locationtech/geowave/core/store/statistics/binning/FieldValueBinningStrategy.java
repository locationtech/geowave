/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Strings;
import com.clearspring.analytics.util.Lists;

/**
 * Statistic binning strategy that bins statistic values by the string representation of the value
 * of one or more fields.
 */
public class FieldValueBinningStrategy implements StatisticBinningStrategy {
  public static final String NAME = "FIELD_VALUE";

  @Parameter(
      names = "--binField",
      description = "Field that contains the bin value. This can be specified multiple times to bin on a combination of fields.",
      required = true)
  protected List<String> fields;

  public FieldValueBinningStrategy() {
    fields = Lists.newArrayList();
  }

  public FieldValueBinningStrategy(final String... fields) {
    this.fields = Arrays.asList(fields);
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by the value of one or more fields.";
  }

  @Override
  public void addFieldsUsed(final Set<String> fieldsUsed) {
    fieldsUsed.addAll(fields);
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    if (fields.isEmpty()) {
      return new ByteArray[0];
    } else if (fields.size() == 1) {
      return new ByteArray[] {getSingleBin(adapter.getFieldValue(entry, fields.get(0)))};
    }
    final ByteArray[] fieldValues =
        fields.stream().map(field -> getSingleBin(adapter.getFieldValue(entry, field))).toArray(
            ByteArray[]::new);
    return new ByteArray[] {getBin(fieldValues)};
  }

  protected static ByteArray getBin(final ByteArray[] fieldValues) {
    int length = 0;
    for (final ByteArray fieldValue : fieldValues) {
      length += fieldValue.getBytes().length;
    }
    final byte[] finalBin = new byte[length + (Character.BYTES * (fieldValues.length - 1))];
    final ByteBuffer binBuffer = ByteBuffer.wrap(finalBin);
    for (final ByteArray fieldValue : fieldValues) {
      binBuffer.put(fieldValue.getBytes());
      if (binBuffer.remaining() > 0) {
        binBuffer.putChar('|');
      }
    }
    return new ByteArray(binBuffer.array());
  }

  @Override
  public String getDefaultTag() {
    return Strings.join("|", fields);
  }

  protected ByteArray getSingleBin(final Object value) {
    if (value == null) {
      return new ByteArray();
    }
    return new ByteArray(value.toString());
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringsToBinary(fields.toArray(new String[fields.size()]));
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    fields = Arrays.asList(StringUtils.stringsFromBinary(bytes));
  }

  @Override
  public String binToString(final ByteArray bin) {
    return bin.getString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    if (fields.size() > 1) {
      return ArrayUtils.addAll(
          StatisticBinningStrategy.super.supportedConstraintClasses(),
          Map.class,
          Pair[].class);
    }
    return StatisticBinningStrategy.super.supportedConstraintClasses();
  }

  protected ByteArrayConstraints singleFieldConstraints(final Object constraint) {
    return StatisticBinningStrategy.super.constraints(constraint);
  }

  protected ByteArrayConstraints handleEmptyField(final Object constraint) {
    throw new IllegalArgumentException(
        "There are no fields in the binning strategy for these constraints");
  }

  @SuppressWarnings("unchecked")
  @Override
  public ByteArrayConstraints constraints(final Object constraint) {
    if (fields.isEmpty() && (constraint != null)) {
      return handleEmptyField(constraint);
    } else if (fields.size() > 1) {
      Map<String, Object> constraintMap;
      if (constraint instanceof Map) {
        constraintMap = (Map<String, Object>) constraint;

      } else if (constraint instanceof Pair[]) {
        if (((Pair[]) constraint).length != fields.size()) {
          throw new IllegalArgumentException(
              "org.apache.commons.lang3.tuple.Pair[] constraint of length "
                  + ((Pair[]) constraint).length
                  + " must be the same length as the number of fields "
                  + fields.size());
        }
        constraintMap =
            Arrays.stream(((Pair[]) constraint)).collect(
                Collectors.toMap((p) -> p.getKey().toString(), Pair::getValue));
      } else {
        throw new IllegalArgumentException(
            "There are multiple fields in the binning strategy; A java.util.Map or org.apache.commons.lang3.tuple.Pair[] constraint must be used with keys associated with field names and values of constraints per field");
      }
      final ByteArray[][] c = new ByteArray[fields.size()][];
      boolean allBins = true;
      for (int i = 0; i < fields.size(); i++) {
        final String field = fields.get(i);
        final ByteArrayConstraints constraints = singleFieldConstraints(constraintMap.get(field));
        if (constraints.isAllBins()) {
          if (!allBins) {
            throw new IllegalArgumentException(
                "Cannot use 'all bins' query for one field and not the other");
          }
        } else {
          allBins = false;
        }
        if (constraints.isPrefix() || (constraints.getBinRanges().length > 0)) {
          // can only use a prefix if its the last field or the rest of the fields are 'all bins'
          boolean isValid = true;
          for (final int j = i + 1; i < fields.size(); i++) {
            final String innerField = fields.get(j);
            final ByteArrayConstraints innerConstraints =
                singleFieldConstraints(constraintMap.get(innerField));
            if (!innerConstraints.isAllBins()) {
              isValid = false;
              break;
            } else {
              c[j] = new ByteArray[] {new ByteArray()};
            }
          }
          if (isValid) {
            if (constraints.getBinRanges().length > 0) {
              // we just prepend all combinations of prior byte arrays to the starts and the ends of
              // the bin ranges
              final ByteArray[][] ends = c.clone();
              final ByteArray[][] starts = c;
              starts[i] =
                  Arrays.stream(constraints.getBinRanges()).map(ByteArrayRange::getStart).toArray(
                      ByteArray[]::new);
              final ByteArray[] startsCombined = getAllCombinations(starts);

              ends[i] =
                  Arrays.stream(constraints.getBinRanges()).map(ByteArrayRange::getEnd).toArray(
                      ByteArray[]::new);
              final ByteArray[] endsCombined = getAllCombinations(ends);
              // now take these pair-wise and combine them back into ByteArrayRange's
              return new ExplicitConstraints(
                  IntStream.range(0, startsCombined.length).mapToObj(
                      k -> new ByteArrayRange(
                          startsCombined[k].getBytes(),
                          endsCombined[k].getBytes())).toArray(ByteArrayRange[]::new));
            } else {
              c[i] = constraints.getBins();
              return new ExplicitConstraints(getAllCombinations(c), true);
            }
          } else {
            throw new IllegalArgumentException(
                "Cannot use 'prefix' or 'range' query for a field that is preceding additional constraints");
          }
        }
        c[i] = constraints.getBins();
      }
      return new ExplicitConstraints(getAllCombinations(c), false);
    } else {
      return singleFieldConstraints(constraint);
    }
  }

  protected static ByteArray[] getAllCombinations(final ByteArray[][] perFieldBins) {
    return BinningStrategyUtils.getAllCombinations(perFieldBins, FieldValueBinningStrategy::getBin);
  }
}
