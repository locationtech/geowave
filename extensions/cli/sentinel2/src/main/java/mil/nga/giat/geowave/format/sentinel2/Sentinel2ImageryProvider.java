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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.util.Converters;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.format.sentinel2.amazon.AmazonImageryProvider;
import mil.nga.giat.geowave.format.sentinel2.theia.TheiaImageryProvider;
import net.sf.json.JSONObject;

/**
 * Defines a provider of Sentinel2 imagery.
 */
public abstract class Sentinel2ImageryProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(Sentinel2ImageryProvider.class);

	protected final static String DOWNLOAD_DIRECTORY = "scenes";

	// Available classes implementing Sentinel2 imagery providers.
	private final static Class<?>[] PROVIDER_CLASSES = new Class<?>[] {
		TheiaImageryProvider.class,
		AmazonImageryProvider.class
	};
	private final static Map<String, Sentinel2ImageryProvider> PROVIDERS = new HashMap<String, Sentinel2ImageryProvider>();
	static {
		for (Class<?> clazz : PROVIDER_CLASSES) {
			try {
				Sentinel2ImageryProvider provider = (Sentinel2ImageryProvider) clazz.newInstance();
				if (provider.isAvailable()) {
					PROVIDERS.put(
							provider.providerName().toUpperCase(),
							provider);
				}
			}
			catch (InstantiationException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to create new instance of " + clazz.getName(),
						e);
			}
		}
	}

	/**
	 * Returns the available providers implementing a Sentinel2 imagery
	 * repository.
	 */
	public static Sentinel2ImageryProvider[] getProviders() {
		return PROVIDERS.values().toArray(
				new Sentinel2ImageryProvider[PROVIDERS.size()]);
	}

	/**
	 * Returns the Sentinel2 provider with the specified name.
	 */
	public static Sentinel2ImageryProvider getProvider(
			String providerName ) {
		return PROVIDERS.get(providerName.toUpperCase());
	}

	/**
	 * Converts a JSONArray to an Iterator<SimpleFeature> instance.
	 */
	protected static class JSONFeatureIterator implements
			Iterator<SimpleFeature>
	{
		private Sentinel2ImageryProvider provider;
		private SimpleFeatureType featureType;
		private Iterator<?> iterator;
		private JSONObject currentObject;

		public JSONFeatureIterator(
				Sentinel2ImageryProvider provider,
				SimpleFeatureType featureType,
				Iterator<?> iterator ) {
			this.provider = provider;
			this.featureType = featureType;
			this.iterator = iterator;
		}

		public JSONObject currentObject() {
			return this.currentObject;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public SimpleFeature next() {
			final JSONObject jsonObject = this.currentObject = (JSONObject) this.iterator.next();

			final String id = jsonObject.getString("id");
			final JSONObject properties = (JSONObject) jsonObject.get("properties");

			final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
					featureType);
			final SimpleFeature feature = featureBuilder.buildFeature(id);

			// Main ID attribute
			feature.setAttribute(
					SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME,
					id);
			feature.setAttribute(
					SceneFeatureIterator.PROVIDER_NAME_ATTRIBUTE_NAME,
					provider.providerName());

			// Fill Geometry
			try {
				Geometry geometry = new GeometryJSON().read(jsonObject.get(
						"geometry").toString());
				geometry.setSRID(4326);
				feature.setDefaultGeometry(geometry);
			}
			catch (IOException e) {
				LOGGER.warn("Unable to read geometry '" + e.getMessage() + "'");
			}

			// Fill attributes
			final List<AttributeDescriptor> descriptorList = featureType.getAttributeDescriptors();

			for (int i = 3, icount = descriptorList.size(); i < icount; i++) {
				final AttributeDescriptor descriptor = descriptorList.get(i);

				final String name = descriptor.getLocalName();
				final Class<?> binding = descriptor.getType().getBinding();
				Object value = properties.get(name);

				if (value == null) {
					continue;
				}
				try {
					value = binding == Date.class ? DateUtilities.parseISO(value.toString()) : Converters.convert(
							value,
							binding);
				}
				catch (ParseException e) {
					LOGGER.warn("Unable to convert attribute '" + e.getMessage() + "'");
					value = null;
				}
				feature.setAttribute(
						name,
						value);
			}
			return feature;
		}
	}

	/**
	 * Provider Name (It should be unique).
	 */
	public abstract String providerName();

	/**
	 * Provider Description.
	 */
	public abstract String description();

	/**
	 * Returns the available Product collection of this Provider.
	 */
	public abstract String[] collections();

	/**
	 * Returns {@code true} if this provider is ready for ingest imagery.
	 */
	public abstract boolean isAvailable();

	/**
	 * Returns the SimpleFeatureTypeBuilder which provides the Scene schema of
	 * the repository.
	 */
	public abstract SimpleFeatureTypeBuilder sceneFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException;

	/**
	 * Returns the SimpleFeatureTypeBuilder which provides the Bands schema of
	 * the repository.
	 */
	public abstract SimpleFeatureTypeBuilder bandFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException;

	/**
	 * Returns the Product/Scene collection that matches the specified criteria.
	 */
	public abstract Iterator<SimpleFeature> searchScenes(
			final File scenesDir,
			final String collection,
			final String platform,
			final String location,
			final Envelope envelope,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber )
			throws IOException;

	/**
	 * Download the scene from the Sentinel2 repository.
	 */
	public abstract boolean downloadScene(
			final SimpleFeature scene,
			final String workspaceDir,
			final String userIdent,
			final String password )
			throws IOException;

	/**
	 * Fetch the coverage of the specified band in the specified workspace
	 * directory
	 */
	public abstract RasterBandData getCoverage(
			final SimpleFeature band,
			final String workspaceDir )
			throws IOException;

	/**
	 * Load CAs from a custom certs file.
	 */
	protected static boolean applyCustomCertsFile(
			HttpsURLConnection connection,
			final File customCertsFile )
			throws GeneralSecurityException,
			IOException {
		if (customCertsFile.exists()) {
			try {
				// Load CAs from an InputStream
				final CertificateFactory cf = CertificateFactory.getInstance("X.509");

				final InputStream caInput = new BufferedInputStream(
						new FileInputStream(
								customCertsFile));
				final Certificate ca = cf.generateCertificate(caInput);

				// Create a KeyStore containing our trusted CAs
				final String keyStoreType = KeyStore.getDefaultType();
				final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
				keyStore.load(
						null,
						null);
				keyStore.setCertificateEntry(
						"ca",
						ca);

				// Create a TrustManager that trusts the CAs in our KeyStore
				final String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
				final TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
				tmf.init(keyStore);

				// Create an SSLContext that uses our TrustManager
				final SSLContext context = SSLContext.getInstance("TLS");
				context.init(
						null,
						tmf.getTrustManagers(),
						null);
				connection.setSSLSocketFactory(context.getSocketFactory());

				return true;
			}
			catch (final GeneralSecurityException securityException) {
				LOGGER.error(
						"Unable to use keystore '" + customCertsFile.getAbsolutePath() + "'",
						securityException);
				throw securityException;
			}
		}
		return false;
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
	protected static long copyLarge(
			final InputStream input,
			final OutputStream output,
			final int contentLength )
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
