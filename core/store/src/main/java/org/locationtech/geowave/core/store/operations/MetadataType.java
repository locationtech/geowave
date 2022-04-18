/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

public enum MetadataType {
  ADAPTER,
  INDEX_MAPPINGS,
  INDEX,
  STATISTICS,
  STATISTIC_VALUES(true),
  INTERNAL_ADAPTER,
  STORE_PROPERTIES,
  LEGACY_STATISTICS("STATS", true),
  LEGACY_INDEX_MAPPINGS("AIM");

  private boolean statValues;
  private String id;

  private MetadataType() {
    this(null);
  }

  private MetadataType(final String id) {
    this(id, false);
  }

  private MetadataType(final boolean statValues) {
    this(null, statValues);
  }

  private MetadataType(final String id, final boolean statValues) {
    this.id = id == null ? name() : id;
    this.statValues = statValues;
  }

  @Override
  public String toString() {
    return id();
  }

  public String id() {
    return id;
  }

  public boolean isStatValues() {
    return statValues;
  }
}
