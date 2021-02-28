/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
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
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    if (fields.size() == 1) {
      return new ByteArray[] {getSingleBin(adapter.getFieldValue(entry, fields.get(0)))};
    }
    final ByteArray[] fieldValues =
        fields.stream().map(field -> getSingleBin(adapter.getFieldValue(entry, field))).toArray(
            ByteArray[]::new);
    int length = 0;
    for (final ByteArray fieldValue : fieldValues) {
      length += fieldValue.getBytes().length;
    }
    final byte[] finalBin = new byte[length + Character.BYTES * (fieldValues.length - 1)];
    ByteBuffer binBuffer = ByteBuffer.wrap(finalBin);
    for (final ByteArray fieldValue : fieldValues) {
      binBuffer.put(fieldValue.getBytes());
      if (binBuffer.remaining() > 0) {
        binBuffer.putChar('|');
      }
    }
    return new ByteArray[] {new ByteArray(binBuffer.array())};
  }

  @Override
  public String getDefaultTag() {
    return Strings.join("|", fields);
  }

  public static ByteArray getBin(final Object... values) {
    if (values == null) {
      return new ByteArray();
    }
    return new ByteArray(
        Arrays.stream(values).map(value -> value == null ? "" : value.toString()).collect(
            Collectors.joining("|")));
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
}
