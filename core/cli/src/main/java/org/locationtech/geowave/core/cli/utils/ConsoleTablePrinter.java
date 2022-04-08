/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import com.beust.jcommander.internal.Console;

/**
 * A reusable generic facility for displaying console results
 */
public class ConsoleTablePrinter {
  private static final int PADDING = 2;
  private final int minColumnSize;
  private final int resultsPerPage;

  private final Console console;

  /**
   * CTOR using default values
   */
  public ConsoleTablePrinter(final Console console) {
    this(5, 24, console);
  }

  /**
   * CTOR
   * 
   * @param minColumnSize Fixed character width
   * @param resultsPerPage When exceeded, will prompt for keyboard input to paginate
   */
  public ConsoleTablePrinter(
      final int minColumnSize,
      final int resultsPerPage,
      final Console console) {
    this.minColumnSize = minColumnSize;
    this.resultsPerPage = resultsPerPage;
    this.console = console;
  }

  public void println(final String line) {
    console.println(line);
  }


  /**
   * Display output to the console. Column widths will be calculated for the each page.
   * 
   * @param headers The label which appears at the top of each vertical column
   * @param rowIter An iterator of rows to display
   */
  public void print(final List<String> headers, final Iterator<List<Object>> rowIter) {
    List<List<Object>> rows = new LinkedList<>();
    while (rowIter.hasNext()) {
      rows.clear();
      while (rowIter.hasNext() && rows.size() < resultsPerPage) {
        rows.add(rowIter.next());
      }
      int[] columnWidths = getColumnWidths(headers, rows);
      printHeader(columnWidths, headers);

      for (int i = 0; i < rows.size(); i++) {
        printRow(rows.get(i), columnWidths);
      }

      printFooter(columnWidths);
      if (rowIter.hasNext()) {
        console.println("Press <Enter> for more results...");
        try {
          System.in.read();
        } catch (final IOException ignore) {
          break;
        }
      }
    }
  }

  /**
   * Display output to the console. Column widths will be calculated for the whole table.
   * 
   * @param headers The label which appears at the top of each vertical column
   * @param rows A 2D matrix of values to display
   */
  public void print(final List<String> headers, final List<List<Object>> rows) {
    int[] columnWidths = getColumnWidths(headers, rows);
    printHeader(columnWidths, headers);

    for (int i = 0; i < rows.size(); i++) {
      if (i > 0 && i % resultsPerPage == 0) {
        console.println("Press <Enter> for more results...");
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
    console.println(line.toString());
    console.println(text.toString());
    console.println(line.toString());
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
    console.println(text.toString());
  }

  private void printFooter(final int[] columnWidths) {
    final StringBuilder line = new StringBuilder("+");
    for (int i = 0; i < columnWidths.length; i++) {
      for (int j = 0; j < columnWidths[i]; j++) {
        line.append("-");
      }
      line.append("+");
    }
    console.println(line.toString());
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
