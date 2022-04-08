/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.split;

import com.beust.jcommander.Parameter;

public class SplitCommandLineOptions {
  @Parameter(
      names = "--indexName",
      description = "The geowave index name (optional; default is all indices)")
  private String indexName;

  @Parameter(
      names = "--num",
      required = true,
      description = "The number of partitions (or entries)")
  private long number;

  public String getIndexName() {
    return indexName;
  }

  public long getNumber() {
    return number;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public void setNumber(final long number) {
    this.number = number;
  }
}
