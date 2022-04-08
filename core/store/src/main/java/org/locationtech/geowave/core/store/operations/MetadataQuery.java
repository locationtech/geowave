/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.index.ByteArrayRange;

public class MetadataQuery {
  private final byte[] primaryId;
  private final byte[] secondaryId;
  private final String[] authorizations;
  private final boolean primaryIdPrefix;
  private final ByteArrayRange[] primaryIdRanges;

  public MetadataQuery() {
    this(null, null, false);
  }

  public MetadataQuery(final byte[] secondaryId, final String... authorizations) {
    this(null, secondaryId, false, authorizations);
  }

  public MetadataQuery(
      final byte[] primaryId,
      final byte[] secondaryId,
      final String... authorizations) {
    this(primaryId, secondaryId, false, authorizations);
  }

  public MetadataQuery(
      final byte[] primaryId,
      final byte[] secondaryId,
      final boolean primaryIdPrefix,
      final String... authorizations) {
    this.primaryId = primaryId;
    primaryIdRanges = null;
    this.secondaryId = secondaryId;
    this.authorizations = authorizations;
    this.primaryIdPrefix = primaryIdPrefix;
  }

  public MetadataQuery(
      final ByteArrayRange[] primaryIdRanges,
      final byte[] secondaryId,
      final String... authorizations) {
    this.primaryIdRanges = primaryIdRanges;
    primaryId = null;
    this.secondaryId = secondaryId;
    this.authorizations = authorizations;
    primaryIdPrefix = false;
  }

  public byte[] getPrimaryId() {
    return primaryId;
  }

  public byte[] getSecondaryId() {
    return secondaryId;
  }

  public boolean hasPrimaryId() {
    return (primaryId != null) && (primaryId.length > 0);
  }

  public boolean hasSecondaryId() {
    return (secondaryId != null) && (secondaryId.length > 0);
  }

  public boolean hasPrimaryIdRanges() {
    return (primaryIdRanges != null) && (primaryIdRanges.length > 0);
  }

  public boolean isExact() {
    return !primaryIdPrefix;
  }

  public boolean isPrefix() {
    return primaryIdPrefix;
  }

  public ByteArrayRange[] getPrimaryIdRanges() {
    return primaryIdRanges;
  }

  public String[] getAuthorizations() {
    return authorizations;
  }
}
