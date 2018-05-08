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
package mil.nga.giat.geowave.format.sentinel2;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;

import org.junit.Test;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.format.sentinel2.DownloadRunner;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2BasicCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2DownloadCommandLineOptions;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2ImageryProvider;

public class DownloadRunnerTest
{
	@Test
	public void testExecuteForTheia()
			throws Exception {
		final File sceneDir = new File(
				Tests.WORKSPACE_DIR + "/scenes/SENTINEL2A_20180101-105913-255_L2A_T30TXN_D");
		testExecute(
				"THEIA",
				sceneDir);
	}

	@Test
	public void testExecuteForAWS()
			throws Exception {
		final File sceneDir = new File(
				Tests.WORKSPACE_DIR + "/scenes/S2A_MSIL1C_20180104T110431_N0206_R094_T30TXN_20180104T130839");
		testExecute(
				"AWS",
				sceneDir);
	}

	public void testExecute(
			String providerName,
			File sceneDir )
			throws Exception {
		JAIExt.initJAIEXT();

		Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
		if (provider == null) {
			System.err.println("Unable to find '" + providerName
					+ "' Sentinel2 provider. Check if it is properly setup.");
			return;
		}

		if (!Tests.authenticationSettingsAreValid(providerName)) return;

		Date[] timePeriodSettings = Tests.timePeriodSettings(providerName);
		Date startDate = timePeriodSettings[0];
		Date endDate = timePeriodSettings[1];

		Sentinel2BasicCommandLineOptions analyzeOptions = new Sentinel2BasicCommandLineOptions();
		analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
		analyzeOptions.setProviderName(providerName);
		analyzeOptions.setCollection(provider.collections()[0]);
		analyzeOptions.setLocation("T30TXN");
		analyzeOptions.setStartDate(startDate);
		analyzeOptions.setEndDate(endDate);
		analyzeOptions
				.setCqlFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND location='T30TXN' AND (band='B4' OR band='B8')");

		String[] settings = Tests.authenticationSettings(providerName);
		String iden = settings[0];
		String pass = settings[1];

		Sentinel2DownloadCommandLineOptions downloadOptions = new Sentinel2DownloadCommandLineOptions();
		downloadOptions.setOverwriteIfExists(false);
		downloadOptions.setUserIdent(iden);
		downloadOptions.setPassword(pass);

		new DownloadRunner(
				analyzeOptions,
				downloadOptions).runInternal(new ManualOperationParams());

		assertTrue(
				"scenes directory exists",
				new File(
						Tests.WORKSPACE_DIR + "/scenes").isDirectory());
	}
}
