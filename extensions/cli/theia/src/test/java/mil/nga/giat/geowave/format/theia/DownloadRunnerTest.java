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

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.format.theia.DownloadRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaDownloadCommandLineOptions;

public class DownloadRunnerTest
{
	@Test
	public void testExecute()
			throws Exception {
		JAIExt.initJAIEXT();

		if (!Tests.authenticationSettingsAreValid()) return;

		TheiaBasicCommandLineOptions analyzeOptions = new TheiaBasicCommandLineOptions();
		analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
		analyzeOptions.setLocation("T30TXN");
		analyzeOptions.setStartDate(DateUtilities.parseISO("2018-01-01T00:00:00Z"));
		analyzeOptions.setEndDate(DateUtilities.parseISO("2018-01-02T00:00:00Z"));
		analyzeOptions
				.setCqlFilter("BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND location='T30TXN' AND (band='B4' OR band='B8')");

		String[] settings = Tests.authenticationSettings();
		String iden = settings[0];
		String pass = settings[1];

		TheiaDownloadCommandLineOptions downloadOptions = new TheiaDownloadCommandLineOptions();
		downloadOptions.setOverwriteIfExists(true);
		downloadOptions.setUserIdent(iden);
		downloadOptions.setPassword(pass);

		new DownloadRunner(
				analyzeOptions,
				downloadOptions).runInternal(new ManualOperationParams());

		final File sceneFile = new File(
				Tests.WORKSPACE_DIR + "/scenes/SENTINEL2A_20180101-105913-255_L2A_T30TXN_D.zip");
		final File sceneDir = new File(
				Tests.WORKSPACE_DIR + "/scenes/SENTINEL2A_20180101-105913-255_L2A_T30TXN_D");

		assertTrue(
				"scenes directory exists",
				new File(
						Tests.WORKSPACE_DIR + "/scenes").isDirectory());
		assertTrue(
				"scene file exists",
				sceneFile.exists());
		assertTrue(
				"scene path exists",
				sceneDir.isDirectory());

		FileUtil.fullyDelete(sceneDir);
		sceneFile.delete();
	}
}
