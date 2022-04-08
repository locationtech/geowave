/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Arrays;
import java.util.Set;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

/**
 * Base interface for statistic binning strategies. These strategies allow a statistic's values to
 * be split up by an arbitrary strategy. This allows a simple statistic to be used in many different
 * ways.
 */
public interface StatisticBinningStrategy extends Persistable, BinningStrategy {
  /**
   * Get the name of the binning strategy.
   *
   * @return the binning strategy name
   */
  String getStrategyName();

  /**
   * Get a human-readable description of the binning strategy.
   *
   * @return a description of the binning strategy
   */
  String getDescription();

  /**
   * Get a human-readable string of a bin.
   *
   * @param bin the bin
   * @return the string value of the bin
   */
  String binToString(final ByteArray bin);

  /**
   * Get a default tag for statistics that use this binning strategy.
   *
   * @return the default tag
   */
  String getDefaultTag();

  /**
   * Adds all of the field names used by the binning strategy to the provided set.
   */
  default void addFieldsUsed(final Set<String> fieldsUsed) {}

  default Class<?>[] supportedConstraintClasses() {
    return new Class<?>[] {
        ByteArray[].class,
        ByteArray.class,
        ByteArrayRange[].class,
        ByteArrayRange.class,
        String.class,
        String[].class,
        BinConstraints.class,
        ByteArrayConstraints.class};
  }

  default ByteArrayConstraints constraints(final Object constraints) {
    if (constraints instanceof ByteArray[]) {
      return new ExplicitConstraints((ByteArray[]) constraints);
    } else if (constraints instanceof ByteArray) {
      return new ExplicitConstraints(new ByteArray[] {(ByteArray) constraints});
    } else if (constraints instanceof String) {
      return new ExplicitConstraints(new ByteArray[] {new ByteArray((String) constraints)});
    } else if (constraints instanceof String[]) {
      return new ExplicitConstraints(
          Arrays.stream((String[]) constraints).map(ByteArray::new).toArray(ByteArray[]::new));
    } else if (constraints instanceof ByteArrayRange) {
      return new ExplicitConstraints(new ByteArrayRange[] {(ByteArrayRange) constraints});
    } else if (constraints instanceof ByteArrayRange[]) {
      return new ExplicitConstraints((ByteArrayRange[]) constraints);
    } else if (constraints instanceof ByteArrayConstraints) {
      return (ByteArrayConstraints) constraints;
    } else if (constraints instanceof BinConstraints) {
      return ((BinConstraints) constraints).constraints(null);
    }
    return new ExplicitConstraints();
  }
}
