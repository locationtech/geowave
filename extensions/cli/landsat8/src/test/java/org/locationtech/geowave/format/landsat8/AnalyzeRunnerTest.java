/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import it.geosolutions.jaiext.JAIExt;

public class AnalyzeRunnerTest {
  private PrintStream outBak = null;
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  @Before
  public void setUpStreams() {
    outBak = System.out;
    System.setOut(new PrintStream(output));
  }

  @After
  public void cleanUpStreams() {
    System.setOut(outBak);
  }

  @Test
  public void testExecute() throws Exception {
    JAIExt.initJAIEXT();

    final Landsat8BasicCommandLineOptions options = new Landsat8BasicCommandLineOptions();
    options.setWorkspaceDir(Tests.WORKSPACE_DIR);
    options.setUseCachedScenes(true);
    options.setNBestScenes(1);
    options.setCqlFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA'");
    new AnalyzeRunner(options).runInternal(new ManualOperationParams());

    final String outputStr = new String(output.toByteArray());
    // Download Information
    assertThat(outputStr, containsString("Acquisition Date: "));
    assertThat(outputStr, containsString("Cloud Cover: "));
    assertThat(outputStr, containsString("Scene Download URL: "));

    // Totals Information
    assertThat(outputStr, containsString("<--   Totals   -->"));
    assertThat(outputStr, containsString("Total Scenes: "));
    assertThat(outputStr, containsString("Date Range: "));
    assertThat(outputStr, containsString("Cloud Cover Range: "));
    assertThat(outputStr, containsString("Average Cloud Cover: "));
    assertThat(outputStr, containsString("WRS2 Paths/Rows covered: "));
    assertThat(outputStr, containsString("Row Range: "));
    assertThat(outputStr, containsString("Path Range: "));
    assertThat(outputStr, containsString("Latitude Range: "));
    assertThat(outputStr, containsString("Longitude Range: "));
    assertThat(outputStr, containsString("Band BQA: "));
  }
}
