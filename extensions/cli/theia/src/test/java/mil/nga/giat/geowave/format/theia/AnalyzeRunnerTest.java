/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.format.theia.AnalyzeRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import it.geosolutions.jaiext.JAIExt;

public class AnalyzeRunnerTest
{
	private PrintStream outBak = null;
	private final ByteArrayOutputStream output = new ByteArrayOutputStream();

	@Before
	public void setUpStreams() {
		outBak = System.out;
		System.setOut(new PrintStream(
				output));
	}

	@After
	public void cleanUpStreams() {
		System.setOut(outBak);
	}

	@Test
	public void testExecute()
			throws Exception {
		JAIExt.initJAIEXT();

		TheiaBasicCommandLineOptions options = new TheiaBasicCommandLineOptions();
		options.setWorkspaceDir(Tests.WORKSPACE_DIR);
		options.setLocation("T30TWM");
		options.setStartDate(DateUtilities.parseISO("2018-01-28T00:00:00Z"));
		options.setEndDate(DateUtilities.parseISO("2018-01-30T00:00:00Z"));
		options
				.setCqlFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND location='T30TWM' AND (band='B4' OR band='B8')");

		new AnalyzeRunner(
				options).runInternal(new ManualOperationParams());

		String outputStr = new String(
				output.toByteArray());

		// Scene information
		assertThat(
				outputStr,
				containsString("Acquisition Date: "));
		assertThat(
				outputStr,
				containsString("Location: "));
		assertThat(
				outputStr,
				containsString("Product Identifier: "));
		assertThat(
				outputStr,
				containsString("Product Type: "));
		assertThat(
				outputStr,
				containsString("Collection: "));
		assertThat(
				outputStr,
				containsString("Platform: "));
		assertThat(
				outputStr,
				containsString("Quicklook: "));
		assertThat(
				outputStr,
				containsString("Thumbnail: "));
		assertThat(
				outputStr,
				containsString("Cloud Cover: "));
		assertThat(
				outputStr,
				containsString("Orbit Number: "));
		assertThat(
				outputStr,
				containsString("Relative Orbit Number: "));

		// Totals information
		assertThat(
				outputStr,
				containsString("<--   Totals   -->"));
		assertThat(
				outputStr,
				containsString("Total Scenes: "));
		assertThat(
				outputStr,
				containsString("Date Range: "));
		assertThat(
				outputStr,
				containsString("Cloud Cover Range: "));
		assertThat(
				outputStr,
				containsString("Average Cloud Cover: "));
		assertThat(
				outputStr,
				containsString("Latitude Range: "));
		assertThat(
				outputStr,
				containsString("Longitude Range: "));
		assertThat(
				outputStr,
				containsString("Processing Levels: "));
	}
}
