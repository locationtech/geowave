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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.format.theia.AnalyzeRunner;
import mil.nga.giat.geowave.format.theia.DownloadRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaDownloadCommandLineOptions;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;

public class DownloadRunner extends
		AnalyzeRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DownloadRunner.class);

	private static final String AUNTHENTICATION_URL = "https://theia.cnes.fr/atdistrib/services/authenticate/";
	private static final String DOWNLOAD_URL = "https://theia.cnes.fr/atdistrib/resto2/collections/%s/%s/download/?issuerId=theia";
	private static final int DOWNLOAD_RETRY = 5;
	private static final String DOWNLOAD_DIRECTORY = "scenes";

	protected TheiaDownloadCommandLineOptions downloadOptions;

	public DownloadRunner(
			final TheiaBasicCommandLineOptions analyzeOptions,
			final TheiaDownloadCommandLineOptions downloadOptions ) {
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

		final String collection = (String) firstBandOfScene
				.getAttribute(SceneFeatureIterator.COLLECTION_ATTRIBUTE_NAME);
		final String productId = (String) firstBandOfScene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
		final String entityId = (String) firstBandOfScene.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);

		final String userIdent = downloadOptions.getUserIdent();
		final String password = downloadOptions.getPassword();
		final String tokenUrl = AUNTHENTICATION_URL;
		String authentication;
		String tokenId;

		// Check authentication parameters
		if (userIdent == null || userIdent.length() == 0 || password == null || password.length() == 0) {
			LOGGER.error("Invalid or empty authentication parameters (email and password)");
			return;
		}
		try {
			authentication = "ident=" + URLEncoder.encode(
					userIdent,
					"UTF-8") + "&pass=" + URLEncoder.encode(
					password,
					"UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			LOGGER.error("Invalid or empty authentication parameters (email and password)" + e.getMessage());
			return;
		}

		// Get a valid tokenId to download data
		InputStream inputStream = null;
		try {
			URL url = new URL(
					tokenUrl);

			HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			connection.setUseCaches(false);
			connection.setRequestProperty(
					HttpHeaders.USER_AGENT,
					"Mozilla/5.0");
			connection.setRequestMethod("POST");

			connection.setDoOutput(true);
			connection.setRequestProperty(
					HttpHeaders.CONTENT_TYPE,
					MediaType.APPLICATION_FORM_URLENCODED);
			connection.setRequestProperty(
					HttpHeaders.CONTENT_LENGTH,
					String.valueOf(authentication.length()));
			OutputStream os = connection.getOutputStream();
			os.write(authentication.getBytes("UTF-8"));
			os.flush();
			os.close();

			inputStream = connection.getInputStream();
			final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			IOUtils.copyLarge(
					inputStream,
					outputStream);
			tokenId = new String(
					outputStream.toByteArray(),
					java.nio.charset.StandardCharsets.UTF_8);
			IOUtils.closeQuietly(outputStream);
		}
		catch (final IOException e) {
			LOGGER.error("Unable to query a token to download '" + e.getMessage() + "'");
			return;
		}
		finally {
			if (inputStream != null) {
				IOUtils.closeQuietly(inputStream);
				inputStream = null;
			}
		}

		// Token is right?
		if (tokenId.length() == 0) {
			LOGGER.error("Unable to get a token to download. Check your ident and password");
			return;
		}

		// First steps to download the gzipped file
		final File compressedFile = getSceneFile(
				firstBandOfScene,
				theiaOptions.getWorkspaceDir());
		final File productDir = new File(
				compressedFile.getParent() + File.separator + productId);

		if (compressedFile.exists()) {
			if (downloadOptions.isOverwriteIfExists()) {
				if (!compressedFile.delete()) {
					LOGGER.warn("Unable to delete file '" + compressedFile.getAbsolutePath() + "'");
				}
				if (productDir.exists() && !FileUtil.fullyDelete(productDir)) {
					LOGGER.warn("Unable to delete dir '" + productDir.getAbsolutePath() + "'");
				}
			}
			else if (productDir.exists()) {
				return;
			}
		}
		if (!compressedFile.getParentFile().exists() && !compressedFile.getParentFile().mkdirs()) {
			LOGGER.warn("Unable to create directory '" + compressedFile.getParentFile().getAbsolutePath() + "'");
		}

		// Download the gzipped file
		final String downloadUrl = String.format(
				DOWNLOAD_URL,
				collection,
				entityId);
		int retry = 0;
		boolean success = false;
		while (!success && (retry < DOWNLOAD_RETRY)) {
			try {
				ClientConfig clientConfig = new DefaultClientConfig();

				Client client = Client.create(clientConfig);

				ClientResponse response = client.resource(
						downloadUrl).accept(
						"application/zip").header(
						javax.ws.rs.core.HttpHeaders.USER_AGENT,
						"Mozilla/5.0").header(
						javax.ws.rs.core.HttpHeaders.AUTHORIZATION,
						"Bearer " + tokenId).get(
						ClientResponse.class);

				String displaySize = FileUtils.byteCountToDisplaySize(response.getLength());
				System.out.println("\nDownloading file '" + productId + "' (" + displaySize + ")");
				System.out.print("Wait please... ");

				inputStream = response.getEntityInputStream();
				final FileOutputStream outputStream = new FileOutputStream(
						compressedFile);
				copyLarge(
						inputStream,
						outputStream,
						response.getLength());
				IOUtils.closeQuietly(outputStream);

				displaySize = FileUtils.byteCountToDisplaySize(compressedFile.length());
				System.out.println("File successfully downloaded! (" + displaySize + ")");

				ZipUtils.unZipFile(
						compressedFile,
						productDir.getAbsolutePath(),
						true);
				System.out.println("File successfully unzipped!");
				success = true;
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to read file from public '" + downloadUrl + "'; retry round " + ++retry,
						e);
			}
			finally {
				if (inputStream != null) {
					IOUtils.closeQuietly(inputStream);
					inputStream = null;
				}
			}
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
	protected static File getSceneDirectory(
			final SimpleFeature scene,
			final String workspaceDirectory ) {
		final String scenesDir = workspaceDirectory + File.separator + DOWNLOAD_DIRECTORY;
		final String productId = (String) scene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);

		return new File(
				scenesDir + File.separator + productId);
	}

	/**
	 * Returns the path of the downloaded scene file in the specified workspace
	 * directory
	 * 
	 * @param scene
	 * @param workspaceDirectory
	 * @return
	 */
	protected static File getSceneFile(
			final SimpleFeature scene,
			final String workspaceDirectory ) {
		final String scenesDir = workspaceDirectory + File.separator + DOWNLOAD_DIRECTORY;
		final String productId = (String) scene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);

		return new File(
				scenesDir + File.separator + productId + ".zip");
	}

	/**
	 * Returns the path of the downloaded coverage in the specified workspace
	 * directory
	 * 
	 * @param band
	 * @param workspaceDirectory
	 * @return
	 * @throws IOException
	 */
	protected static File getCoverageFile(
			final SimpleFeature band,
			final String workspaceDirectory )
			throws IOException {
		final String scenesDir = workspaceDirectory + File.separator + DOWNLOAD_DIRECTORY;
		final String productId = (String) band.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
		final String bandName = (String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);

		File file = new File(
				scenesDir + File.separator + productId);

		String[] fileList = file.list();

		if (fileList != null) {
			for (String name : fileList) {
				File temp = new File(
						file.getAbsolutePath() + File.separatorChar + name);

				if (temp.isDirectory() && name.toUpperCase().startsWith(
						productId.toUpperCase())) {
					// We provide the coverage in ground reflectance with the
					// correction of slope effects.
					// The full description of the product format is here:
					// 'https://theia.cnes.fr/atdistrib/documents/PSC-NT-411-0362-CNES_01_00_SENTINEL-2A_L2A_Products_Description.pdf'
					// A more succinct one is also available here:
					// 'http://www.cesbio.ups-tlse.fr/multitemp/?page_id=8352'
					//
					temp = new File(
							file.getAbsolutePath() + File.separatorChar + name + File.separatorChar + name + "_FRE_"
									+ bandName + ".tif");
					if (temp.exists()) return temp;
				}
			}
		}
		throw new IOException(
				"The file of the '" + productId + "_" + bandName + "' coverage does not exist");
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
		File sceneFile = getSceneFile(
				scene,
				workspaceDirectory);
		if (sceneFile.exists()) {
			try {
				sceneFile.delete();
			}
			catch (SecurityException e) {
				LOGGER.warn(
						"Unable to delete file from public '" + sceneFile.getAbsolutePath() + ".",
						e);
			}
		}
		File sceneDir = getSceneDirectory(
				scene,
				workspaceDirectory);
		if (sceneDir.isDirectory()) {
			FileUtil.fullyDelete(sceneDir);
		}
	}

	/**
	 * Copy bytes from a large (over 2GB) <code>InputStream</code> to an
	 * <code>OutputStream</code> showing the progress of the copy.
	 * 
	 * @param input
	 * @param output
	 * @param contentLength
	 * @return
	 * @throws IOException
	 */
	static long copyLarge(
			InputStream input,
			OutputStream output,
			int contentLength )
			throws IOException {
		long count = 0;
		int n = 0;

		final byte[] buffer = new byte[4096];
		final int EOF = -1;
		int percentDone = 0, lastPercentDone = -1;

		while (EOF != (n = input.read(buffer))) {
			output.write(
					buffer,
					0,
					n);
			count += n;

			if (contentLength != -1) {
				percentDone = (int) ((100L * count) / contentLength);

				if (lastPercentDone != percentDone) {
					lastPercentDone = percentDone;

					if ((percentDone % 10) == 0) {
						System.out.print(percentDone + "%");
					}
					else if ((percentDone % 3) == 0) {
						System.out.print(".");
					}
				}
			}
		}
		System.out.println();
		return count;
	}
}
