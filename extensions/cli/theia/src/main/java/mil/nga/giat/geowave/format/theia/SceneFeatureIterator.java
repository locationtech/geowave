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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.io.IOUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.geotools.util.Converters;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SceneFeatureIterator implements
		SimpleFeatureIterator
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SceneFeatureIterator.class);

	private static final String SCENES_SEARCH_URL = "https://theia.cnes.fr/atdistrib/resto2/api/collections/%s/search.json?";
	private static final String SCENES_DIR = "scenes";
	private static final String SCENES_TYPE_NAME = "theia-scene";

	// List of predefined attributes.
	public static final String SHAPE_ATTRIBUTE_NAME = "shape";
	public static final String ENTITY_ID_ATTRIBUTE_NAME = "entityId";
	public static final String LOCATION_ATTRIBUTE_NAME = "location";
	public static final String PRODUCT_ID_ATTRIBUTE_NAME = "productIdentifier";
	public static final String PRODUCT_TYPE_ATTRIBUTE_NAME = "productType";
	public static final String COLLECTION_ATTRIBUTE_NAME = "collection";
	public static final String PLATFORM_ATTRIBUTE_NAME = "platform";
	public static final String PROCESSING_LEVEL_ATTRIBUTE_NAME = "processingLevel";
	public static final String ACQUISITION_DATE_ATTRIBUTE_NAME = "startDate";
	public static final String QUICKLOOK_ATTRIBUTE_NAME = "quicklook";
	public static final String THUMBNAIL_ATTRIBUTE_NAME = "thumbnail";
	public static final String BANDS_ATTRIBUTE_NAME = "bands";
	public static final String RESOLUTION_ATTRIBUTE_NAME = "resolution";
	public static final String CLOUD_COVER_ATTRIBUTE_NAME = "cloudCover";
	public static final String SNOW_COVER_ATTRIBUTE_NAME = "snowCover";
	public static final String WATER_COVER_ATTRIBUTE_NAME = "waterCover";
	public static final String ORBIT_NUMBER_ATTRIBUTE_NAME = "orbitNumber";
	public static final String RELATIVE_ORBIT_NUMBER_ATTRIBUTE_NAME = "relativeOrbitNumber";

	private Iterator<SimpleFeature> iterator;
	private SimpleFeatureType type;

	/**
	 * Returns the SimpleFeatureTypeBuilder which provides the schema of the
	 * Theia repository.
	 *
	 * @return
	 * @throws NoSuchAuthorityCodeException
	 * @throws FactoryException
	 */
	public static SimpleFeatureTypeBuilder defaultSceneFeatureTypeBuilder()
			throws NoSuchAuthorityCodeException,
			FactoryException {
		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(SCENES_TYPE_NAME);
		typeBuilder.setCRS(CRS.decode(
				"EPSG:4326",
				true));
		typeBuilder.setDefaultGeometry(SHAPE_ATTRIBUTE_NAME);

		// shape (Geometry),
		// entityId (String), location (String), productIdentifier (String),
		// productType (String), collection (String), platform (String),
		// processingLevel (String), startDate (Date), quicklook (String),
		// thumbnail (String),
		// bands (String), resolution (Integer),
		// cloudCover (Integer), snowCover (Integer), waterCover (Integer),
		// orbitNumber (Integer), relativeOrbitNumber (Integer),
		// and the feature ID is entityId for the scene
		//
		typeBuilder.add(
				SHAPE_ATTRIBUTE_NAME,
				Polygon.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				ENTITY_ID_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				LOCATION_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PRODUCT_ID_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PRODUCT_TYPE_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				COLLECTION_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.minOccurs(
				1).maxOccurs(
				1).nillable(
				false).add(
				PLATFORM_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				PROCESSING_LEVEL_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				ACQUISITION_DATE_ATTRIBUTE_NAME,
				Date.class);
		typeBuilder.add(
				QUICKLOOK_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				THUMBNAIL_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				BANDS_ATTRIBUTE_NAME,
				String.class);
		typeBuilder.add(
				RESOLUTION_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				CLOUD_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				SNOW_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				WATER_COVER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				ORBIT_NUMBER_ATTRIBUTE_NAME,
				Integer.class);
		typeBuilder.add(
				RELATIVE_ORBIT_NUMBER_ATTRIBUTE_NAME,
				Integer.class);

		return typeBuilder;
	}

	/**
	 * Converts a JSONArray to an Iterator<SimpleFeature> instance.
	 */
	private static class JSONFeatureIterator implements
			Iterator<SimpleFeature>
	{
		private final SimpleFeatureType featureType;
		private final Iterator<?> iterator;

		public JSONFeatureIterator(
				final SimpleFeatureType featureType,
				final Iterator<?> iterator ) {
			this.featureType = featureType;
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public SimpleFeature next() {
			final JSONObject jsonObject = (JSONObject) iterator.next();

			final String id = jsonObject.getString("id");
			final JSONObject properties = (JSONObject) jsonObject.get("properties");

			final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
					featureType);
			final SimpleFeature feature = featureBuilder.buildFeature(id);

			// Main ID attribute
			feature.setAttribute(
					ENTITY_ID_ATTRIBUTE_NAME,
					id);

			// Fill Geometry
			try {
				final Geometry geometry = new GeometryJSON().read(jsonObject.get(
						"geometry").toString());
				geometry.setSRID(4326);
				feature.setDefaultGeometry(geometry);
			}
			catch (final IOException e) {
				LOGGER.warn("Unable to read geometry '" + e.getMessage() + "'");
			}

			// Fill attributes
			final List<AttributeDescriptor> descriptorList = featureType.getAttributeDescriptors();

			for (int i = 2, icount = descriptorList.size(); i < icount; i++) {
				final AttributeDescriptor descriptor = descriptorList.get(i);

				final String name = descriptor.getLocalName();
				final Class<?> binding = descriptor.getType().getBinding();
				Object value = properties.get(name);

				try {
					value = binding == Date.class ? DateUtilities.parseISO(value.toString()) : Converters.convert(
							value,
							binding);
				}
				catch (final ParseException e) {
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

	public SceneFeatureIterator(
			final String collection,
			final String platform,
			final String location,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber,
			final Filter cqlFilter,
			final String workspaceDir )
			throws NoSuchAuthorityCodeException,
			FactoryException,
			MalformedURLException,
			IOException,
			GeneralSecurityException {
		init(
				new File(
						workspaceDir,
						SCENES_DIR),
				collection,
				platform,
				location,
				startDate,
				endDate,
				orbitNumber,
				relativeOrbitNumber,
				cqlFilter);
	}

	private void init(
			final File scenesDir,
			final String collection,
			final String platform,
			final String location,
			final Date startDate,
			final Date endDate,
			final int orbitNumber,
			final int relativeOrbitNumber,
			final Filter cqlFilter )
			throws NoSuchAuthorityCodeException,
			FactoryException,
			IOException,
			GeneralSecurityException {

		final SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd");

		if (!scenesDir.exists() && !scenesDir.mkdirs()) {
			LOGGER.warn("Unable to create directory '" + scenesDir.getAbsolutePath() + "'");
		}

		// Split out the spatial part of the filter.
		Envelope envelope = null;
		if ((cqlFilter != null) && !cqlFilter.equals(Filter.INCLUDE)) {
			Envelope bounds = new Envelope();
			bounds = (Envelope) cqlFilter.accept(
					ExtractBoundsFilterVisitor.BOUNDS_VISITOR,
					bounds);

			if ((bounds != null) && !bounds.isNull() && !bounds.equals(infinity())) {
				envelope = bounds;
			}
		}

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
			if (customCertsFile.exists()) {
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
			}

			inputStream = connection.getInputStream();
			// HP Fortify "Resource Shutdown" false positive
			// The InputStream is being closed in the finally block
			IOUtils.copyLarge(
					inputStream,
					outputStream = new ByteArrayOutputStream());
			final String geoJson = new String(
					outputStream.toByteArray(),
					java.nio.charset.StandardCharsets.UTF_8);

			final SimpleFeatureTypeBuilder typeBuilder = SceneFeatureIterator.defaultSceneFeatureTypeBuilder();
			type = typeBuilder.buildFeatureType();

			final JSONObject response = JSONObject.fromObject(geoJson);
			final JSONArray features = response.getJSONArray("features");
			Iterator<SimpleFeature> featureItereator = new JSONFeatureIterator(
					type,
					features.iterator());

			if ((cqlFilter != null) && !cqlFilter.equals(Filter.INCLUDE)) {
				Filter actualFilter;

				if (hasOtherProperties(cqlFilter)) {
					final List<AttributeDescriptor> descriptorList = type.getAttributeDescriptors();

					final String[] propertyNames = new String[descriptorList.size()];
					for (int i = 0, icount = descriptorList.size(); i < icount; i++) {
						propertyNames[i] = descriptorList.get(
								i).getLocalName();
					}

					final PropertyIgnoringFilterVisitor visitor = new PropertyIgnoringFilterVisitor(
							propertyNames,
							type);
					actualFilter = (Filter) cqlFilter.accept(
							visitor,
							null);
				}
				else {
					actualFilter = cqlFilter;
				}

				final CqlFilterPredicate filterPredicate = new CqlFilterPredicate(
						actualFilter);
				featureItereator = Iterators.filter(
						featureItereator,
						filterPredicate);
			}
			iterator = featureItereator;
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

	private boolean hasOtherProperties(
			final Filter cqlFilter ) {
		final String[] attributes = DataUtilities.attributeNames(
				cqlFilter,
				type);

		for (final String attribute : attributes) {
			if (type.getDescriptor(attribute) == null) {
				return true;
			}
		}
		return false;
	}

	public SimpleFeatureType getFeatureType() {
		return type;
	}

	@Override
	public void close() {}

	@Override
	public boolean hasNext() {
		if (iterator != null) {
			return iterator.hasNext();
		}
		return false;
	}

	@Override
	public SimpleFeature next()
			throws NoSuchElementException {
		if (iterator != null) {
			return iterator.next();
		}
		return null;
	}

	private Envelope infinity() {
		return new Envelope(
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY,
				Double.NEGATIVE_INFINITY,
				Double.POSITIVE_INFINITY);
	}

	private static class CqlFilterPredicate implements
			Predicate<SimpleFeature>
	{
		private final Filter cqlFilter;

		public CqlFilterPredicate(
				final Filter cqlFilter ) {
			this.cqlFilter = cqlFilter;
		}

		@Override
		public boolean apply(
				final SimpleFeature input ) {
			return cqlFilter.evaluate(input);
		}
	}
}
