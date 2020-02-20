/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * A reusable generic facility for displaying console results
 */
public class ConsolePrinter {
  private static final int PADDING = 2;
  private int minColumnSize = 5;
  private int resultsPerPage = 24;

  /**
   * CTOR using default values
   */
  public ConsolePrinter() {}

  /**
   * CTOR
   * 
   * @param minColumnSize Fixed character width
   * @param resultsPerPage When exceeded, will prompt for keyboard input to paginate
   */
  public ConsolePrinter(final int minColumnSize, final int resultsPerPage) {
    this.minColumnSize = minColumnSize;
    this.resultsPerPage = resultsPerPage;
  }


  /**
   * Display output to the console
   * 
   * @param headers The label which appears at the top of each vertical column
   * @param rows A 2D matrix of values to display
   */
  public void print(final List<String> headers, final List<List<Object>> rows) {
    int[] columnWidths = getColumnWidths(headers, rows);
    printHeader(columnWidths, headers);

    for (int i = 0; i < rows.size(); i++) {
      if (i > 0 && i % resultsPerPage == 0) {
        System.out.println("Press <Enter> for more results...");
        try {
          System.in.read();
        } catch (final IOException ignore) {
          break;
        }
      }
      printRow(rows.get(i), columnWidths);
    }

    printFooter(columnWidths);
  }


  private void printHeader(final int[] columnWidths, final List<String> headers) {
    final StringBuilder line = new StringBuilder("+");
    final StringBuilder text = new StringBuilder("|");
    for (int i = 0; i < columnWidths.length; i++) {
      for (int j = 0; j < columnWidths[i]; j++) {
        line.append("-");
      }
      line.append("+");
      final String columnName = headers.get(i);
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

  private void printRow(final List<Object> result, final int[] columnWidths) {
    final StringBuilder text = new StringBuilder("|");
    for (int i = 0; i < columnWidths.length; i++) {
      final Object value = result.get(i);
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



  /**
   * The width of each column is the greatest of (column-label-length,
   * the-longest-value-in-the-column, minColumnSize)
   * 
   * @param headers
   * @param rows
   * @return
   */
  private int[] getColumnWidths(final List<String> headers, final List<List<Object>> rows) {
    int[] columnWidths = new int[headers.size()];

    // Evaluate the lengths of the column headers
    for (int i = 0; i < columnWidths.length; i++) {
      String header = StringUtils.trimToEmpty(headers.get(i));
      columnWidths[i] = Math.max(minColumnSize, header.length() + PADDING);
    }

    // Check each value. If the length of any single value is > current length of that
    // column, replace the current column length with the new max value
    for (List<Object> row : rows) {
      for (int i = 0; i < row.size(); i++) {
        Object val = row.get(i) == null ? "" : row.get(i);
        String value = StringUtils.trimToEmpty(String.valueOf(val));
        columnWidths[i] = Math.max(columnWidths[i], value.length() + PADDING);
      }
    }

    return columnWidths;
  }


}
