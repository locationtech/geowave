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
/** Enumeration Provides the lineage relationship between two entities. */
public enum LineageRelationType {
  /** a parent relationship with another specified entity */
  PARENT("PARENT"),

  /** a child relationship with another specified entity. */
  CHILD("CHILD"),

  /**
   * no relationship between tracks. Used when an update is required to terminate an existing
   * relationship
   */
  NONE("NONE");

  private String value;

  LineageRelationType() {
    value = LineageRelationType.values()[0].toString();
  }

  LineageRelationType(final String value) {
    this.value = value;
  }

  public static LineageRelationType fromString(final String value) {
    for (final LineageRelationType item : LineageRelationType.values()) {
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
