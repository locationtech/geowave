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
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Strings;
import com.clearspring.analytics.util.Lists;

/**
 * Statistic binning strategy that bins statistic values by the string representation of the value
 * of one or more fields.
 *
 * <p> Multi-field constraints combine like those of {@link CompositeBinningStrategy}: exact leading
 * fields multiply out and only 'all bins' may follow a field constrained to all bins, prefixes or
 * ranges. The separator between field bins ends each field, so the resulting prefixes and ranges
 * match exactly as long as field bins and constraint bounds do not contain the separator's first
 * byte, 0x00 (for strings, U+0000).
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
    return new ByteArray[] {joinFieldBins(fieldValues)};
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
      final List<ByteArrayConstraints> perFieldConstraints =
          fields.stream().map(field -> singleFieldConstraints(constraintMap.get(field))).collect(
              Collectors.toList());
      return BinningStrategyUtils.combineConstraints(perFieldConstraints, new Layout());
    } else {
      return singleFieldConstraints(constraint);
    }
  }

  /**
   * Join the bins of each field into a multi-field bin. Multi-field constraints are joined the same
   * way, so a subclass that encodes multi-field bins differently overrides this, and if it joins
   * them without a separator, also {@link #getFixedFieldBinLength()}.
   *
   * @param fieldBins the bin of each field, in field order; when building prefix and range
   *        constraints the last one may be a partial bin (a prefix, a range end, or empty)
   * @return the joined bin
   */
  protected ByteArray joinFieldBins(final ByteArray[] fieldBins) {
    return getBin(fieldBins);
  }

  /**
   * Get the length of every field's bin, for a subclass whose {@link #joinFieldBins} joins
   * fixed-length field bins without a separator. Multi-field prefixes and ranges rely on the
   * separator, or else on this length, to tell where a field's bin ends.
   *
   * @return the length in bytes of every field bin, or empty if field bins are separated
   */
  protected OptionalInt getFixedFieldBinLength() {
    return OptionalInt.empty();
  }

  private class Layout implements BinningStrategyUtils.ComponentLayout {
    @Override
    public ByteArray getBin(final ByteArray[] componentBins) {
      return joinFieldBins(componentBins);
    }

    @Override
    public ByteArray getPrefix(final ByteArray[] leadingBins, final ByteArray partialBin) {
      return joinFieldBins(ArrayUtils.add(leadingBins, partialBin));
    }

    @Override
    public ByteArray getExactPrefix(final ByteArray[] leadingBins, final ByteArray bin) {
      if (getFixedFieldBinLength().isPresent()) {
        return getPrefix(leadingBins, bin);
      }
      // ending with the separator stops a longer bin of the field from matching
      return joinFieldBins(
          ArrayUtils.add(ArrayUtils.add(leadingBins, bin), BinningStrategyUtils.EMPTY));
    }

    @Override
    public void checkDelimited(final int component, final ByteArrayConstraints constraints) {
      final OptionalInt length = getFixedFieldBinLength();
      if (length.isPresent()) {
        BinningStrategyUtils.checkFixedLength(length.getAsInt(), constraints, describe(component));
      } else if ((component == (fields.size() - 1))
          && (constraints.getBinRanges().length > 0)
          && !constraints.isPrefix()
          && (constraints.getBins().length > 0)) {
        // no separator follows the last field, and a range end matches everything it starts
        throw new IllegalArgumentException(
            "Cannot combine exact bins with ranges on "
                + describe(component)
                + ", the last field; use either exact bins or ranges for it");
      }
    }

    @Override
    public String describe(final int component) {
      return "field '" + fields.get(component) + "'";
    }
  }
}
