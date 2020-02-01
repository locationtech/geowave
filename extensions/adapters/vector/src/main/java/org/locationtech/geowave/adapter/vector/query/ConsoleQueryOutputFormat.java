/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.adapter.vector.query.gwql.Result;
import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import org.locationtech.geowave.core.cli.utils.ConsolePrinter;

public class ConsoleQueryOutputFormat extends QueryOutputFormatSpi {

  public static final String FORMAT_NAME = "console";

  private static final int RESULTS_PER_PAGE = 24;
  private static final int MIN_COLUMN_SIZE = 5;

  public ConsoleQueryOutputFormat() {
    super(FORMAT_NAME);
  }

  @Override
  public void output(final ResultSet results) {
    // The column headers for display
    List<String> headers = new ArrayList<String>(results.columnCount());
    for (int i = 0; i < results.columnCount(); i++) {
      headers.add(results.columnName(i));
    }

    ConsolePrinter consolePrinter = new ConsolePrinter(MIN_COLUMN_SIZE, RESULTS_PER_PAGE);
    consolePrinter.print(headers, getRows(results, headers.size()));
    // If more results exist, we will paginate
    while (results.hasNext()) {
      System.out.println("Press <Enter> for more results...");
      try {
        System.in.read();
      } catch (final IOException ignore) {
        break;
      }
      consolePrinter.print(headers, getRows(results, headers.size()));
    }
  }


  // Convert to the more generic Object matrix structure for console printing
  private List<List<Object>> getRows(final ResultSet results, final int columns) {
    List<List<Object>> rows = new ArrayList<List<Object>>();
    while (results.hasNext() && rows.size() < RESULTS_PER_PAGE) {
      Result result = results.next();
      List<Object> values = new ArrayList<Object>(columns);
      for (int i = 0; i < columns; i++) {
        values.add(result.columnValue(i));
      }
      rows.add(values);
    }
    return rows;
  }

}
