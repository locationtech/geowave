/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.landsat8;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Ignore;
import org.junit.Test;

import it.geosolutions.jaiext.JAIExt;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;

public class DownloadRunnerTest
{
	@Test
	@Ignore
	public void testExecute()
			throws Exception {
		JAIExt.initJAIEXT();

		Landsat8BasicCommandLineOptions analyzeOptions = new Landsat8BasicCommandLineOptions();
		analyzeOptions.setWorkspaceDir(Tests.WORKSPACE_DIR);
		analyzeOptions.setUseCachedScenes(true);
		analyzeOptions.setNBestScenes(1);
		analyzeOptions.setCqlFilter("BBOX(shape,-76.6,42.34,-76.4,42.54) and band='BQA' and sizeMB < 1");

		Landsat8DownloadCommandLineOptions downloadOptions = new Landsat8DownloadCommandLineOptions();
		downloadOptions.setOverwriteIfExists(false);

		new DownloadRunner(
				analyzeOptions,
				downloadOptions).runInternal(new ManualOperationParams());

		assertTrue(
				"images directory exists",
				new File(
						Tests.WORKSPACE_DIR + "/images").isDirectory());
		assertTrue(
				"scenes directory exists",
				new File(
						Tests.WORKSPACE_DIR + "/scenes").isDirectory());
		assertTrue(
				"wrs2_asc_desc directory exists",
				new File(
						Tests.WORKSPACE_DIR + "/wrs2_asc_desc").isDirectory());
	}
}
