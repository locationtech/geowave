/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.export;

import java.util.List;
import com.beust.jcommander.Parameter;

public class VectorExportOptions {
  protected static final int DEFAULT_BATCH_SIZE = 10000;

  @Parameter(names = "--cqlFilter", description = "Filter exported data based on CQL filter")
  private String cqlFilter;

  @Parameter(names = "--typeNames", description = "Comma separated list of type names")
  private List<String> typeNames;

  @Parameter(names = "--indexName", description = "The index to export from")
  private String indexName;

  @Parameter(names = "--batchSize", description = "Records to process at a time")
  private int batchSize = DEFAULT_BATCH_SIZE;

  public String getCqlFilter() {
    return cqlFilter;
  }

  public List<String> getTypeNames() {
    return typeNames;
  }

  public String getIndexName() {
    return indexName;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = cqlFilter;
  }

  public void setTypeNames(final List<String> typeNames) {
    this.typeNames = typeNames;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public void setBatchSize(final int batchSize) {
    this.batchSize = batchSize;
  }
}
