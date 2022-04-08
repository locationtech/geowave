/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.stanag4676.parser.model;

// STANAG 4676
/**
 * Enumeration Provides an indication of whether the information pertains to operational data,
 * exercise data, or test data.
 */
public enum ExerciseIndicator {
  OPERATIONAL("OPERATIONAL"), EXERCISE("EXERCISE"), TEST("TEST");

  private String value;

  ExerciseIndicator() {
    value = "OPERATIONAL";
  }

  ExerciseIndicator(final String value) {
    this.value = value;
  }

  public static ExerciseIndicator fromString(final String value) {
    for (final ExerciseIndicator item : ExerciseIndicator.values()) {
      if (item.toString().equals(value)) {
        return item;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return value;
  }
}
