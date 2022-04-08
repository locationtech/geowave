/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.query;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.query.gwql.Result;
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import com.beust.jcommander.Parameter;

public class CSVQueryOutputFormat extends QueryOutputFormatSpi {
  public static final String FORMAT_NAME = "csv";

  @Parameter(names = {"-o", "--outputFile"}, required = true, description = "Output file")
  private String outputFile;

  public CSVQueryOutputFormat() {
    super(FORMAT_NAME);
  }

  @Override
  public void output(final ResultSet results) {
    try (OutputStreamWriter writer =
        new OutputStreamWriter(new FileOutputStream(outputFile), StringUtils.getGeoWaveCharset())) {
      try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
        final String[] header = new String[results.columnCount()];
        for (int i = 0; i < results.columnCount(); i++) {
          header[i] = results.columnName(i);
        }
        printer.printRecord((Object[]) header);
        while (results.hasNext()) {
          final Result result = results.next();
          final Object[] values = new Object[results.columnCount()];
          for (int i = 0; i < results.columnCount(); i++) {
            values[i] = result.columnValue(i);
          }
          printer.printRecord(values);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing CSV: " + e.getMessage(), e);
    }
  }

}
