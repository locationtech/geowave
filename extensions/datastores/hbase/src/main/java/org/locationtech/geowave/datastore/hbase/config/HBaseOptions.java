/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.config;

import org.apache.hadoop.hbase.HConstants;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import com.beust.jcommander.Parameter;

public class HBaseOptions extends BaseDataStoreOptions {
  public static final String COPROCESSOR_JAR_KEY = "coprocessorJar";

  @Parameter(
      names = "--scanCacheSize",
      description = "The number of rows passed to each scanner (higher values will enable faster scanners, but will use more memory)")
  protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;

  @Parameter(
      names = "--disableVerifyCoprocessors",
      description = "Disables coprocessor verification, which ensures that coprocessors have been added to the HBase table prior to executing server-side operations")
  protected boolean disableVerifyCoprocessors = false;

  protected boolean bigTable = false;

  @Parameter(
      names = {"--" + COPROCESSOR_JAR_KEY},
      description = "Path (HDFS URL) to the jar containing coprocessor classes")
  private String coprocessorJar;

  public HBaseOptions() {
    super();
  }

  public void setBigTable(final boolean bigTable) {
    this.bigTable = bigTable;
    if (bigTable) {
      enableServerSideLibrary = false;
    }
  }

  public boolean isBigTable() {
    return bigTable;
  }

  public int getScanCacheSize() {
    return scanCacheSize;
  }

  public void setScanCacheSize(final int scanCacheSize) {
    this.scanCacheSize = scanCacheSize;
  }

  public boolean isVerifyCoprocessors() {
    return !disableVerifyCoprocessors && enableServerSideLibrary;
  }

  public void setVerifyCoprocessors(final boolean verifyCoprocessors) {
    disableVerifyCoprocessors = !verifyCoprocessors;
  }

  public String getCoprocessorJar() {
    return coprocessorJar;
  }

  public void setCoprocessorJar(final String coprocessorJar) {
    this.coprocessorJar = coprocessorJar;
  }

  @Override
  protected int defaultMaxRangeDecomposition() {
    return 2000;
  }

  @Override
  protected int defaultAggregationMaxRangeDecomposition() {
    return 100;
  }
}
