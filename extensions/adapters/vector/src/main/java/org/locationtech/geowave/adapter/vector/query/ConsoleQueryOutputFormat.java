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
import java.util.List;
import org.locationtech.geowave.adapter.vector.query.gwql.Result;
import org.locationtech.geowave.adapter.vector.query.gwql.ResultSet;
import com.google.common.collect.Lists;

public class ConsoleQueryOutputFormat extends QueryOutputFormatSpi {
  public static final String FORMAT_NAME = "console";

  private static final int RESULTS_PER_PAGE = 24;
  private static final int MIN_COLUMN_SIZE = 5;

  public ConsoleQueryOutputFormat() {
    super(FORMAT_NAME);
  }

  @Override
  public void output(final ResultSet results) {
    while (results.hasNext()) {
      displayPage(results);

      if (results.hasNext()) {
        System.out.println("Press enter for more results...");
        try {
          System.in.read();
        } catch (final IOException e) {
          break;
        }
      }
    }
  }

  private void displayPage(final ResultSet results) {
    final int[] columnWidths = new int[results.columnCount()];
    for (int i = 0; i < results.columnCount(); i++) {
      columnWidths[i] = Math.max(MIN_COLUMN_SIZE, results.columnName(i).length() + 2);
    }
    final List<Result> pageResults = Lists.newArrayListWithCapacity(RESULTS_PER_PAGE);
    while (results.hasNext() && (pageResults.size() < RESULTS_PER_PAGE)) {
      final Result result = results.next();
      for (int i = 0; i < results.columnCount(); i++) {
        final Object value = result.columnValue(i);
        if (value != null) {
          columnWidths[i] = Math.max(columnWidths[i], value.toString().length() + 2);
        }
      }
      pageResults.add(result);
    }
    printHeader(results, columnWidths);
    pageResults.stream().forEach(r -> printRow(r, columnWidths));
    printFooter(columnWidths);
  }

  private void printHeader(final ResultSet results, final int[] columnWidths) {
    final StringBuilder line = new StringBuilder("+");
    final StringBuilder text = new StringBuilder("|");
    for (int i = 0; i < results.columnCount(); i++) {
      for (int j = 0; j < columnWidths[i]; j++) {
        line.append("-");
      }
      line.append("+");
      final String columnName = results.columnName(i);
      text.append(" ").append(columnName);
      for (int j = columnName.length() + 1; j < columnWidths[i]; j++) {
        text.append(" ");
      }
      text.append("|");
    }
    System.out.println(line.toString());
    System.out.println(text.toString());
    System.out.println(line.toString());

  }

  private void printRow(final Result result, final int[] columnWidths) {
    final StringBuilder text = new StringBuilder("|");
    for (int i = 0; i < columnWidths.length; i++) {
      final Object value = result.columnValue(i);
      final String valStr = value == null ? "" : value.toString();
      text.append(" ").append(valStr);
      for (int j = valStr.length() + 1; j < columnWidths[i]; j++) {
        text.append(" ");
      }
      text.append("|");
    }
    System.out.println(text.toString());
  }

  private void printFooter(final int[] columnWidths) {
    final StringBuilder line = new StringBuilder("+");
    for (int i = 0; i < columnWidths.length; i++) {
      for (int j = 0; j < columnWidths[i]; j++) {
        line.append("-");
      }
      line.append("+");
    }
    System.out.println(line.toString());
  }

}
