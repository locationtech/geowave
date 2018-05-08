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
package mil.nga.giat.geowave.format.sentinel2.theia;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.vividsolutions.jts.geom.Envelope;

import mil.nga.giat.geowave.adapter.raster.plugin.gdal.GDALGeoTiffReader;
import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.format.sentinel2.BandFeatureIterator;
import mil.nga.giat.geowave.format.sentinel2.DownloadRunner;
import mil.nga.giat.geowave.format.sentinel2.RasterBandData;
import mil.nga.giat.geowave.format.sentinel2.SceneFeatureIterator;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2ImageryProvider;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Sentinel2 imagery provider for the Theia repository. See:
 * https://theia.cnes.fr
 */
public class TheiaImageryProvider extends
		Sentinel2ImageryProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TheiaImageryProvider.class);

	private static final String SCENES_TYPE_NAME = "theia-sentinel2-scene";
	private static final String BANDS_TYPE_NAME = "theia-sentinel2-band";
	private static final double NO_DATA_VALUE = 0;

	private static final String SCENES_SEARCH_URL = "https://theia.cnes.fr/atdistrib/resto2/api/collections/%s/search.json?";
	private static final String AUNTHENTICATION_URL = "https://theia.cnes.fr/atdistrib/services/authenticate/";
	private static final String DOWNLOAD_URL = "https://theia.cnes.fr/atdistrib/resto2/collections/%s/%s/download/?issuerId=theia";
	private static final int DOWNLOAD_RETRY = 5;

	@Override
	public String providerName() {
		return "THEIA";
	}

	@Override
	public String description() {
		return "Sentinel2 provider for the Theia repository (https://theia.cnes.fr)";
	}

	@Override
	public String[] collections() {
		return new String[] {
			"SENTINEL2"
		};
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public SimpleFeatureTypeBuilder sceneFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException {
		return SceneFeatureIterator.defaultSceneFeatureTypeBuilder(SCENES_TYPE_NAME);
	}

	@Override
	public SimpleFeatureTypeBuilder bandFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException {
		return BandFeatureIterator.defaultBandFeatureTypeBuilder(BANDS_TYPE_NAME);
	}

	@Override
	public Iterator<SimpleFeature> searchScenes(
			final File scenesDir,
			final String collection,
			final String platform,
			final String location,
			final Envelope envelope,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber )
			throws IOException {

		final SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd");

		// Build the search URL to fetch products from Theia repository.
		String searchUrl = String.format(
				SCENES_SEARCH_URL,
				collection);
		if ((platform != null) && (platform.length() > 0)) {
			searchUrl += "platform=" + platform + "&";
		}
		if ((location != null) && (location.length() > 0)) {
			searchUrl += "location=" + location + "&";
		}
		if ((envelope != null) && (envelope.isNull() == false)) {
			searchUrl += String.format(
					Locale.ENGLISH,
					"box=%.6f,%.6f,%.6f,%.6f&",
					envelope.getMinX(),
					envelope.getMinY(),
					envelope.getMaxX(),
					envelope.getMaxY());
		}
		if (startDate != null) {
			searchUrl += "startDate=" + dateFormat.format(startDate) + "&";
		}
		if (endDate != null) {
			searchUrl += "completionDate=" + dateFormat.format(endDate) + "&";
		}
		if (orbitNumber > 0) {
			searchUrl += "orbitNumber=" + orbitNumber + "&";
		}
		if (relativeOrbitNumber > 0) {
			searchUrl += "relativeOrbitNumber=" + relativeOrbitNumber + "&";
		}
		searchUrl = searchUrl.substring(
				0,
				searchUrl.length() - 1);

		// Fetch the JSON meta data with found Theia products.
		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		try {
			final URL url = new URL(
					searchUrl);

			final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			// HP Fortify "Certificate Validation" False Positive
			// we allow for custom trust store to anchor acceptable certs
			// to reduce the level of trust if desired
			connection.setUseCaches(false);
			connection.setRequestProperty(
					HttpHeaders.USER_AGENT,
					"Mozilla/5.0");
			connection.setRequestMethod("GET");

			// allow for custom trust store to anchor acceptable certs, use an
			// expected file in the workspace directory
			final File customCertsFile = new File(
					scenesDir.getParentFile(),
					"theia-keystore.crt");
			applyCustomCertsFile(
					connection,
					customCertsFile);

			inputStream = connection.getInputStream();
			// HP Fortify "Resource Shutdown" false positive
			// The InputStream is being closed in the finally block
			IOUtils.copyLarge(
					inputStream,
					outputStream = new ByteArrayOutputStream());
			final String geoJson = new String(
					outputStream.toByteArray(),
					java.nio.charset.StandardCharsets.UTF_8);

			final JSONObject response = JSONObject.fromObject(geoJson);
			final JSONArray features = response.getJSONArray("features");

			SimpleFeatureTypeBuilder typeBuilder = sceneFeatureTypeBuilder();
			SimpleFeatureType type = typeBuilder.buildFeatureType();

			class TheiaJSONFeatureIterator extends
					JSONFeatureIterator
			{
				public TheiaJSONFeatureIterator(
						Sentinel2ImageryProvider provider,
						SimpleFeatureType featureType,
						Iterator<?> iterator ) {
					super(
							provider,
							featureType,
							iterator);
				}

				@Override
				public SimpleFeature next() {
					SimpleFeature feature = super.next();
					JSONObject jsonObject = null;

					if (feature != null && (jsonObject = super.currentObject()) != null) {
						final JSONObject properties = (JSONObject) jsonObject.get("properties");

						final String entityId = jsonObject.getString("id");
						final String collection = properties.getString(SceneFeatureIterator.COLLECTION_ATTRIBUTE_NAME);
						final String downloadUrl = String.format(
								DOWNLOAD_URL,
								collection,
								entityId);

						feature.setAttribute(
								SceneFeatureIterator.SCENE_DOWNLOAD_ATTRIBUTE_NAME,
								downloadUrl);
					}
					return feature;
				}
			}
			;
			return new TheiaJSONFeatureIterator(
					this,
					type,
					features.iterator());
		}
		catch (GeneralSecurityException | FactoryException e) {
			throw new IOException(
					e);
		}
		finally {
			if (outputStream != null) {
				IOUtils.closeQuietly(outputStream);
				outputStream = null;
			}
			if (inputStream != null) {
				IOUtils.closeQuietly(inputStream);
				inputStream = null;
			}
		}
	}

	@Override
	public boolean downloadScene(
			final SimpleFeature scene,
			final String workspaceDir,
			final String userIdent,
			final String password )
			throws IOException {
		final String tokenUrl = AUNTHENTICATION_URL;
		String authentication;
		String tokenId;

		final String collection = (String) scene.getAttribute(SceneFeatureIterator.COLLECTION_ATTRIBUTE_NAME);
		final String productId = (String) scene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
		final String entityId = (String) scene.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);

		// Check authentication parameters
		if ((userIdent == null) || (userIdent.length() == 0) || (password == null) || (password.length() == 0)) {
			LOGGER.error("Invalid or empty authentication parameters (email and password)");
			return false;
		}
		try {
			authentication = "ident=" + URLEncoder.encode(
					userIdent,
					"UTF-8") + "&pass=" + URLEncoder.encode(
					password,
					"UTF-8");
		}
		catch (final UnsupportedEncodingException e) {
			LOGGER.error("Invalid or empty authentication parameters (email and password)" + e.getMessage());
			return false;
		}

		// Get a valid tokenId to download data
		InputStream inputStream = null;
		try {
			final URL url = new URL(
					tokenUrl);

			final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
			// HP Fortify "Certificate Validation" False Positive
			// we allow for custom trust store to anchor acceptable certs
			// to reduce the level of trust if desired
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

			// allow for custom trust store to anchor acceptable certs, use an
			// expected file in the workspace directory
			final File customCertsFile = new File(
					workspaceDir,
					"theia-keystore.crt");
			applyCustomCertsFile(
					connection,
					customCertsFile);

			final OutputStream os = connection.getOutputStream();
			// HP Fortify "Resource Shutdown" false positive
			// The OutputStream is being closed
			os.write(authentication.getBytes("UTF-8"));
			// HP Fortify "Privacy Violation" false positive
			// In this case the password is being sent to an output
			// stream in order to authenticate the system and allow
			// us to perform the requested download.
			os.flush();
			os.close();

			inputStream = connection.getInputStream();
			// HP Fortify "Resource Shutdown" false positive
			// The InputStream is being closed in the finally block
			final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			IOUtils.copyLarge(
					inputStream,
					outputStream);
			tokenId = new String(
					outputStream.toByteArray(),
					java.nio.charset.StandardCharsets.UTF_8);
			IOUtils.closeQuietly(outputStream);
		}
		catch (final IOException | GeneralSecurityException e) {
			LOGGER.error("Unable to query a token to download '" + e.getMessage() + "'");
			return false;
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
			return false;
		}

		// First steps to download the gzipped file
		final File compressedFile = new File(
				workspaceDir + File.separator + DOWNLOAD_DIRECTORY + File.separator + productId + ".zip");
		final File productDir = DownloadRunner.getSceneDirectory(
				scene,
				workspaceDir);

		// Download the gzipped file
		final String downloadUrl = String.format(
				DOWNLOAD_URL,
				collection,
				entityId);
		int retry = 0;
		boolean success = false;
		while (!success && (retry < DOWNLOAD_RETRY)) {
			try {
				final ClientConfig clientConfig = new DefaultClientConfig();

				final Client client = Client.create(clientConfig);

				final ClientResponse response = client.resource(
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

				// HP Fortify "Resource Shutdown" false positive
				// The OutputStream is being closed
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
				if (!compressedFile.delete()) {
					LOGGER.warn("Unable to delete file '" + compressedFile.getAbsolutePath() + "'");
				}
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
		return success;
	}

	/**
	 * Fetch the coverage of the specified band in the specified workspace
	 * directory
	 *
	 * @param band
	 * @param workspaceDir
	 * @return
	 * @throws IOException
	 */
	@Override
	public RasterBandData getCoverage(
			final SimpleFeature band,
			final String workspaceDir )
			throws IOException {
		final File sceneDir = DownloadRunner.getSceneDirectory(
				band,
				workspaceDir);

		final String entityId = (String) band.getAttribute(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME);
		final String productId = (String) band.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
		final String bandName = (String) band.getAttribute(BandFeatureIterator.BAND_ATTRIBUTE_NAME);

		final File file = sceneDir;
		final String[] fileList = sceneDir.list();

		if (fileList != null) {
			for (final String name : fileList) {
				File temp = new File(
						file.getAbsolutePath() + File.separatorChar + name);

				if (temp.isDirectory() && name.toUpperCase(
						Locale.ENGLISH).startsWith(
						productId.toUpperCase(Locale.ENGLISH))) {
					// We provide the coverage in ground reflectance with the
					// correction of slope effects.
					// The full description of the product format is here:
					// 'https://theia.cnes.fr/atdistrib/documents/PSC-NT-411-0362-CNES_01_00_SENTINEL-2A_L2A_Products_Description.pdf'
					// A more succinct one is also available here:
					// 'http://www.cesbio.ups-tlse.fr/multitemp/?page_id=8352'
					//
					File geotiffFile = new File(
							file.getAbsolutePath() + File.separatorChar + name + File.separatorChar + name + "_FRE_"
									+ bandName + ".tif");
					if (geotiffFile.exists()) {
						final GDALGeoTiffReader reader = new GDALGeoTiffReader(
								geotiffFile);
						final GridCoverage2D coverage = reader.read(null);
						reader.dispose();
						return new RasterBandData(
								entityId + "_" + bandName,
								coverage,
								reader,
								NO_DATA_VALUE);
					}
				}
			}
		}
		throw new IOException(
				"The file of the '" + productId + "_" + bandName + "' coverage does not exist");
	}
}
