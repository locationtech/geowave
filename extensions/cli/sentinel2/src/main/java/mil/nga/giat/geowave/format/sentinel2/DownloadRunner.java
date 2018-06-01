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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadRunner extends
		AnalyzeRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DownloadRunner.class);

	private static final String DOWNLOAD_DIRECTORY = "scenes";

	protected Sentinel2DownloadCommandLineOptions downloadOptions;

	public DownloadRunner(
			final Sentinel2BasicCommandLineOptions analyzeOptions,
			final Sentinel2DownloadCommandLineOptions downloadOptions ) {
		super(
				analyzeOptions);
		this.downloadOptions = downloadOptions;
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		super.nextScene(
				firstBandOfScene,
				analysisInfo);

		final String providerName = sentinel2Options.providerName();
		final String workspaceDir = sentinel2Options.getWorkspaceDir();
		final boolean overwriteIfExists = downloadOptions.isOverwriteIfExists();
		final String userIdent = downloadOptions.getUserIdent();
		final String password = downloadOptions.getPassword();

		Sentinel2ImageryProvider provider = Sentinel2ImageryProvider.getProvider(providerName);
		if (provider == null) {
			throw new RuntimeException(
					"Unable to find '" + providerName + "' Sentinel2 provider");
		}

		// First steps to download, check state of scene directory
		final File sceneDir = getSceneDirectory(
				firstBandOfScene,
				workspaceDir);
		if (overwriteIfExists) {
			if (sceneDir.exists() && !FileUtil.fullyDelete(sceneDir)) {
				LOGGER.warn("Unable to delete dir '" + sceneDir.getAbsolutePath() + "'");
			}
		}
		else if (sceneDir.exists()) {
			return;
		}
		if (!sceneDir.getParentFile().exists() && !sceneDir.getParentFile().mkdirs()) {
			LOGGER.warn("Unable to create directory '" + sceneDir.getParentFile().getAbsolutePath() + "'");
		}

		// Download files of scene
		try {
			provider.downloadScene(
					firstBandOfScene,
					workspaceDir,
					userIdent,
					password);
		}
		catch (IOException e) {
			LOGGER.error("Unable to download scene '"
					+ firstBandOfScene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME) + "'");
		}
	}

	/**
	 * Returns the path of the downloaded scene directory in the specified
	 * workspace directory
	 *
	 * @param scene
	 * @param workspaceDirectory
	 * @return
	 */
	public static File getSceneDirectory(
			final SimpleFeature scene,
			final String workspaceDirectory ) {
		final String scenesDir = workspaceDirectory + File.separator + DOWNLOAD_DIRECTORY;
		final String productId = (String) scene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);

		return new File(
				scenesDir + File.separator + productId);
	}

	/**
	 * Remove all downloaded files of the scene in the specified workspace
	 * directory
	 *
	 * @param scene
	 * @param workspaceDirectory
	 */
	protected static void cleanDownloadedFiles(
			final SimpleFeature scene,
			final String workspaceDirectory ) {
		final File sceneDir = getSceneDirectory(
				scene,
				workspaceDirectory);
		if (sceneDir.isDirectory()) {
			FileUtil.fullyDelete(sceneDir);
		}
	}
}
