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
package mil.nga.giat.geowave.format.sentinel2.amazon;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverageio.gdal.jp2ecw.JP2ECWReader;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Envelope;

import it.geosolutions.imageio.plugins.jp2ecw.JP2GDALEcwImageReaderSpi;
import mil.nga.giat.geowave.format.sentinel2.BandFeatureIterator;
import mil.nga.giat.geowave.format.sentinel2.DownloadRunner;
import mil.nga.giat.geowave.format.sentinel2.RasterBandData;
import mil.nga.giat.geowave.format.sentinel2.SceneFeatureIterator;
import mil.nga.giat.geowave.format.sentinel2.Sentinel2ImageryProvider;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Sentinel2 imagery provider for Amazon Web Services (AWS) repository. See:
 * http
 * ://opensearch.sentinel-hub.com/resto/api/collections/Sentinel2/describe.xml
 */
public class AmazonImageryProvider extends
		Sentinel2ImageryProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AmazonImageryProvider.class);

	private static final String SCENES_TYPE_NAME = "aws-sentinel2-scene";
	private static final String BANDS_TYPE_NAME = "aws-sentinel2-band";
	private static final double NO_DATA_VALUE = 0;

	private static final String SCENES_SEARCH_URL = "http://opensearch.sentinel-hub.com/resto/api/collections/%s/search.json?";
	private static final String DOWNLOAD_URL = "http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/";

	// Default FilterFactory to use.
	private static final FilterFactory2 FF = CommonFactoryFinder.getFilterFactory2();

	// List of available AWS band names.
	private static final String AWS_RASTER_BANDS_NAMES = "B1;B2;B3;B4;B5;B6;B7;B8;B9;B10;B11;B12;B8A;TCI";
	// Map of interesting AWS resources.
	private static final Map<String, String> AWS_SCENE_RESOURCE_NAMES = new HashMap<String, String>();
	// Map of AWS collection names.
	private static final Map<String, String> AWS_COLLECTION_NAMES = new HashMap<String, String>();
	// Flag to indicate whether the native JP2ECW plugin is properly setup.
	private static int JP2ECW_PLUGIN_AVAILABLE_FLAG = 0;

	static {
		AWS_SCENE_RESOURCE_NAMES.put(
				"productInfo",
				"productInfo.json");
		AWS_SCENE_RESOURCE_NAMES.put(
				"preview",
				"preview.jpg");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B1",
				"B01.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B2",
				"B02.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B3",
				"B03.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B4",
				"B04.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B5",
				"B05.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B6",
				"B06.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B7",
				"B07.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B8",
				"B08.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B9",
				"B09.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B10",
				"B10.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B11",
				"B11.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B12",
				"B12.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"B8A",
				"B8A.jp2");
		AWS_SCENE_RESOURCE_NAMES.put(
				"TCI",
				"TCI.jp2");

		AWS_COLLECTION_NAMES.put(
				"SENTINEL2",
				"Sentinel2");
	}

	@Override
	public String providerName() {
		return "AWS";
	}

	@Override
	public String description() {
		return "Sentinel2 provider for the Amazon Web Services (AWS) repository";
	}

	@Override
	public String[] collections() {
		return new String[] {
			"SENTINEL2"
		};
	}

	@Override
	public boolean isAvailable() {
		/*
		 * TODO: At the present time, only the native JP2ECW plugin
		 * (JP2ECWReader) seems to load JP2 files of LEVEL1C Products. Both
		 * JP2Reader and JP2KReader fail trying to open these files.
		 */
		synchronized (AWS_SCENE_RESOURCE_NAMES) {
			if (JP2ECW_PLUGIN_AVAILABLE_FLAG == 0) {
				try {
					System.err.println("Testing whether the JP2ECW plugin for GDAL is available...");

					Class.forName("it.geosolutions.imageio.plugins.jp2ecw.JP2GDALEcwImageReaderSpi");
					boolean available = new JP2GDALEcwImageReaderSpi().isAvailable();

					if (available) {
						String ncs_env = System.getenv("NCS_USER_PREFS");

						if (ncs_env != null && ncs_env.length() == 0) {
							LOGGER.warn("NCS_USER_PREFS environment variable is empty, ignore JP2ECW plugin.");
							available = false;
						}
					}
					if (available) {
						System.err.println("JP2ECW plugin is available!");
						JP2ECW_PLUGIN_AVAILABLE_FLAG = 1;
						return true;
					}
				}
				catch (final Throwable e) {
					LOGGER.error(
							"Unable to validate the JP2ECW plugin for GDAL",
							e);
				}
				System.err
						.println("The native JP2ECW plugin for GDAL seems not to be set in your GDAL_PROVIDER_PATH environment variable. AWS Sentinel2 provider is not available.");
				JP2ECW_PLUGIN_AVAILABLE_FLAG = 2;
				return false;
			}
		}
		return JP2ECW_PLUGIN_AVAILABLE_FLAG == 1;
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
		Filter extraFilter = Filter.INCLUDE;

		// Build the search URL to fetch products from AWS repository.
		String searchUrl = String.format(
				SCENES_SEARCH_URL,
				AWS_COLLECTION_NAMES.get(collection));
		if ((platform != null) && (platform.length() > 0)) {
			extraFilter = FF.equals(
					FF.property("platform"),
					FF.literal(platform));
		}
		if ((location != null) && (location.length() > 0)) {
			Filter temp = FF.equals(
					FF.property("location"),
					FF.literal(location));
			if (extraFilter.equals(Filter.INCLUDE))
				extraFilter = temp;
			else
				extraFilter = FF.and(
						extraFilter,
						temp);
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

		// Fetch the JSON meta data with found AWS products.
		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		try {
			final URL url = new URL(
					searchUrl);

			final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setUseCaches(false);
			connection.setRequestProperty(
					HttpHeaders.USER_AGENT,
					"Mozilla/5.0");
			connection.setRequestMethod("GET");

			inputStream = connection.getInputStream();
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

			class AmazonJSONFeatureIterator extends
					JSONFeatureIterator
			{
				public AmazonJSONFeatureIterator(
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

						// Set missing basic values.
						String s3Path = properties.getString("s3Path");
						String[] path = s3Path.split("/");
						feature.setAttribute(
								SceneFeatureIterator.LOCATION_ATTRIBUTE_NAME,
								"T" + path[1] + path[2] + path[3]);
						feature.setAttribute(
								SceneFeatureIterator.BANDS_ATTRIBUTE_NAME,
								AWS_RASTER_BANDS_NAMES);
						feature.setAttribute(
								SceneFeatureIterator.SCENE_DOWNLOAD_ATTRIBUTE_NAME,
								DOWNLOAD_URL + s3Path);

						// Normalize values of this AWS repository.
						InputStream inputStream = null;
						ByteArrayOutputStream outputStream = null;
						try {
							final URL url = new URL(
									DOWNLOAD_URL + s3Path + "/productInfo.json");

							final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
							connection.setUseCaches(false);
							connection.setRequestProperty(
									HttpHeaders.USER_AGENT,
									"Mozilla/5.0");
							connection.setRequestMethod("GET");

							inputStream = connection.getInputStream();
							IOUtils.copyLarge(
									inputStream,
									outputStream = new ByteArrayOutputStream());
							final String geoJson = new String(
									outputStream.toByteArray(),
									java.nio.charset.StandardCharsets.UTF_8);
							final JSONObject response = JSONObject.fromObject(geoJson);

							final String name = response.getString("name");
							final String id = response.getString("id");
							feature.setAttribute(
									SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME,
									id);
							feature.setAttribute(
									SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME,
									name);
							feature.setAttribute(
									SceneFeatureIterator.COLLECTION_ATTRIBUTE_NAME,
									"SENTINEL2");

							final String platform = name.substring(
									0,
									4);
							final String level = (String) feature
									.getAttribute(SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME);
							if (!level.startsWith("LEVEL")) {
								feature.setAttribute(
										SceneFeatureIterator.PROCESSING_LEVEL_ATTRIBUTE_NAME,
										"LEVEL" + level);
							}
							if (platform.equalsIgnoreCase("S2A_")) {
								feature.setAttribute(
										SceneFeatureIterator.PLATFORM_ATTRIBUTE_NAME,
										"SENTINEL2A");
							}
							else if (platform.equalsIgnoreCase("S2B_")) {
								feature.setAttribute(
										SceneFeatureIterator.PLATFORM_ATTRIBUTE_NAME,
										"SENTINEL2B");
							}
						}
						catch (IOException e) {
							LOGGER.warn(
									"Unable to get 'productInfo.json' of '" + s3Path + "'",
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
					return feature;
				}
			}
			;

			Iterator<SimpleFeature> featureIterator = new AmazonJSONFeatureIterator(
					this,
					type,
					features.iterator());
			if (!extraFilter.equals(Filter.INCLUDE)) {
				final SceneFeatureIterator.CqlFilterPredicate filterPredicate = new SceneFeatureIterator.CqlFilterPredicate(
						extraFilter);
				featureIterator = Iterators.filter(
						featureIterator,
						filterPredicate);
			}
			return featureIterator;
		}
		catch (FactoryException e) {
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

	/**
	 * Download a resource from the specified URL.
	 * 
	 * @throws IOException
	 */
	private static boolean downloadFile(
			final String downloadUrl,
			final File sceneDir,
			final String resourceName )
			throws IOException {
		final String fileName = AWS_SCENE_RESOURCE_NAMES.get(resourceName);
		final String resourceUrl = downloadUrl + "/" + fileName;
		final File resourceFile = new File(
				sceneDir + File.separator + fileName);

		InputStream inputStream = null;
		FileOutputStream outputStream = null;
		try {
			final URL url = new URL(
					resourceUrl);

			final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setUseCaches(false);
			connection.setRequestProperty(
					HttpHeaders.USER_AGENT,
					"Mozilla/5.0");
			connection.setRequestMethod("GET");

			inputStream = connection.getInputStream();
			outputStream = new FileOutputStream(
					resourceFile);

			String displaySize = FileUtils.byteCountToDisplaySize(inputStream.available());
			System.out.print("Downloading File '" + resourceUrl + "' (" + displaySize + ")");

			IOUtils.copyLarge(
					inputStream,
					outputStream);
			IOUtils.closeQuietly(outputStream);

			displaySize = FileUtils.byteCountToDisplaySize(resourceFile.length());
			System.out.println(" -> ok: (" + displaySize + ")");
			return true;
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to download '" + resourceUrl + "'",
					e);
			System.out.println(" -> error: " + e.getMessage());
			return false;
		}
		finally {
			if (outputStream != null) {
				outputStream.close();
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

		final String productId = (String) scene.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
		System.out.println("\nDownloading scene '" + productId + "'");
		System.out.println("Wait please... ");

		final File sceneDir = DownloadRunner.getSceneDirectory(
				scene,
				workspaceDir);
		if (!sceneDir.exists() && !sceneDir.mkdirs()) {
			LOGGER.error("Unable to create directory '" + sceneDir.getAbsolutePath() + "'");
			return false;
		}

		final String downloadUrl = (String) scene.getAttribute(SceneFeatureIterator.SCENE_DOWNLOAD_ATTRIBUTE_NAME);
		int successCount = 0;

		// Download main resources.
		if (downloadFile(
				downloadUrl,
				sceneDir,
				"productInfo")) {
			successCount++;
		}
		if (downloadFile(
				downloadUrl,
				sceneDir,
				"preview")) {
			successCount++;
		}
		return successCount == 2;
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

		final File file = new File(
				sceneDir + File.separator + AWS_SCENE_RESOURCE_NAMES.get(bandName));
		if (!file.exists()) {
			final String downloadUrl = (String) band.getAttribute(SceneFeatureIterator.SCENE_DOWNLOAD_ATTRIBUTE_NAME);
			downloadFile(
					downloadUrl,
					sceneDir,
					bandName);
		}
		if (file.exists()) {
			final JP2ECWReader reader = new JP2ECWReader(
					file);
			final GridCoverage2D coverage = reader.read(null);
			reader.dispose();
			return new RasterBandData(
					entityId + "_" + bandName,
					coverage,
					reader,
					NO_DATA_VALUE);
		}
		throw new IOException(
				"The file of the '" + productId + "_" + bandName + "' coverage does not exist");
	}
}
