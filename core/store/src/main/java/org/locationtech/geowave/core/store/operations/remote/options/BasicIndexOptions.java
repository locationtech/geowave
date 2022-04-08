/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations.remote.options;

import java.util.Arrays;
import org.locationtech.geowave.core.store.index.IndexPluginOptions.PartitionStrategy;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class BasicIndexOptions {

  @Parameter(
      names = {"-np", "--numPartitions"},
      description = "The number of partitions.  Default partitions will be 1.")
  protected int numPartitions = 1;

  @Parameter(
      names = {"-ps", "--partitionStrategy"},
      description = "The partition strategy to use.  Default will be none.",
      converter = PartitionStrategyConverter.class)
  protected PartitionStrategy partitionStrategy = PartitionStrategy.NONE;

  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public PartitionStrategy getPartitionStrategy() {
    return partitionStrategy;
  }

  public void setPartitionStrategy(final PartitionStrategy partitionStrategy) {
    this.partitionStrategy = partitionStrategy;
  }

  public static class PartitionStrategyConverter implements IStringConverter<PartitionStrategy> {

    @Override
    public PartitionStrategy convert(final String value) {
      final PartitionStrategy convertedValue = PartitionStrategy.fromString(value);

      if (convertedValue == null) {
        throw new ParameterException(
            "Value "
                + value
                + " can not be converted to PartitionStrategy. "
                + "Available values are: "
                + Arrays.toString(PartitionStrategy.values()));
      }
      return convertedValue;
    }
  }
}
