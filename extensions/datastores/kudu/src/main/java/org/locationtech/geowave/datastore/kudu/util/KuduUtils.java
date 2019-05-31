/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.util;

import java.util.List;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(KuduUtils.class);

  public static final byte[] EMPTY_KEY = new byte[] {0};
  public static int KUDU_DEFAULT_MAX_RANGE_DECOMPOSITION = 250;
  public static int KUDU_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION = 250;
  public static int KUDU_DEFAULT_BUCKETS = 2;
  public static int KUDU_DEFAULT_REPLICAS = 1;

  public static void executeQuery(KuduScanner scanner, List<RowResultIterator> results) {
    try {
      while (scanner.hasMoreRows()) {
        RowResultIterator rows = scanner.nextRows();
        if (rows == null) {
          break;
        }
        results.add(rows);
      }
    } catch (KuduException e) {
      LOGGER.error("Error when reading rows", e);
    } finally {
      try {
        scanner.close();
      } catch (KuduException e) {
        LOGGER.error("Error while closing Kudu scanner", e);
      }
    }
  }
}
