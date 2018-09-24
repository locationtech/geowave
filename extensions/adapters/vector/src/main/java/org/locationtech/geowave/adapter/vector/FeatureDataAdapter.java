/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.vector.index.SecondaryIndexManager;
import org.locationtech.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import org.locationtech.geowave.adapter.vector.stats.StatsManager;
import org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import org.locationtech.geowave.core.geotime.store.dimension.Time;
import org.locationtech.geowave.core.geotime.store.query.api.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.DataAdapterTypeId;
import org.locationtech.geowave.core.store.adapter.IndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.data.visibility.VisibilityManagement;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataAdapter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.HadoopDataAdapter;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * This data adapter will handle all reading/writing concerns for storing and
 * retrieving GeoTools SimpleFeature objects to and from a GeoWave persistent
 * store in Accumulo. <br>
 * <br>
 *
 * If the implementor needs to write rows with particular visibility, this can
 * be done by providing a FieldVisibilityHandler to a constructor or a
 * VisibilityManagement to a constructor. When using VisibilityManagement, the
 * feature attribute to contain the visibility meta-data is, by default, called
 * GEOWAVE_VISIBILITY. This can be overridden by setting the UserData property
 * 'visibility' to Boolean.TRUE for the feature attribute that describes the
 * attribute that contains the visibility meta-data.
 * persistedType.getDescriptor("someAttributeName").getUserData().put(
 * "visibility", Boolean.TRUE)<br>
 * <br>
 * The adapter will use the SimpleFeature's default geometry for spatial
 * indexing.<br>
 * <br>
 * The adaptor will use the first temporal attribute (a Calendar or Date object)
 * as the timestamp of a temporal index.<br>
 * <br>
 * If the feature type contains a UserData property 'time' for a specific time
 * attribute with Boolean.TRUE, then the attribute is used as the timestamp of a
 * temporal index.<br>
 * <br>
 * If the feature type contains UserData properties 'start' and 'end' for two
 * different time attributes with value Boolean.TRUE, then the attributes are
 * used for a range index.<br>
 * <br>
 * If the feature type contains a UserData property 'time' for *all* time
 * attributes with Boolean.FALSE, then a temporal index is not used.<br>
 * <br>
 * Statistics configurations are maintained in UserData. Each attribute may have
 * a UserData property called 'stats'. The associated value is an instance of
 * {@link org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection}
 * . The collection maintains a set of
 * {@link org.locationtech.geowave.adapter.vector.stats.StatsConfig}, one for
 * each type of statistic. The default statistics for geometry and temporal
 * constraints cannot be changed, as they are critical components to the
 * efficiency of query processing.
 *
 */
public class FeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature> implements
		GeotoolsFeatureDataAdapter,
		StatisticsProvider<SimpleFeature>,
		HadoopDataAdapter<SimpleFeature, FeatureWritable>,
		SecondaryIndexDataAdapter<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureDataAdapter.class);
	// the original coordinate system will always be represented internally by
	// the persisted type
	private SimpleFeatureType persistedFeatureType;

	// externally the reprojected type will always be advertised because all
	// features will be reprojected to EPSG:4326 and the advertised feature type
	// from the data adapter should match in CRS
	private SimpleFeatureType reprojectedFeatureType;
	private MathTransform transform;
	private StatsManager statsManager;
	private SecondaryIndexManager secondaryIndexManager;
	private TimeDescriptors timeDescriptors = null;
	// should change this anytime the serialized image changes. Stay negative.
	// so 0xa0, 0xa1, 0xa2 etc.
	final static byte VERSION = (byte) 0xa3;

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	protected FeatureDataAdapter() {}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor<br>
	 * Creates a FeatureDataAdapter for the specified SimpleFeatureType
	 *
	 * @param featureType
	 *            - feature type for this object
	 */
	public FeatureDataAdapter(
			final SimpleFeatureType featureType ) {
		this(
				featureType,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				null,
				null);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor<br>
	 * Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
	 * provided customIndexHandlers
	 *
	 * @param featureType
	 *            - feature type for this object
	 * @param customIndexHandlers
	 *            - l
	 */
	public FeatureDataAdapter(
			final SimpleFeatureType featureType,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this(
				featureType,
				customIndexHandlers,
				null,
				null);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor<br>
	 * Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
	 * provided visibilityManagement
	 *
	 * @param featureType
	 *            - feature type for this object
	 * @param visibilityManagement
	 */
	public FeatureDataAdapter(
			final SimpleFeatureType featureType,
			final VisibilityManagement<SimpleFeature> visibilityManagement ) {
		this(
				featureType,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				null,
				visibilityManagement);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Constructor<br>
	 * Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
	 * provided fieldVisiblityHandler
	 *
	 * @param featureType
	 *            - feature type for this object
	 * @param fieldVisiblityHandler
	 */
	public FeatureDataAdapter(
			final SimpleFeatureType featureType,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		this(
				featureType,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				fieldVisiblityHandler,
				null);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------
	/**
	 * Constructor<br>
	 * Creates a FeatureDataAdapter for the specified SimpleFeatureType with the
	 * provided customIndexHandlers, fieldVisibilityHandler and
	 * defaultVisibilityManagement
	 *
	 * @param featureType
	 *            - feature type for this object
	 * @param customIndexHandlers
	 * @param fieldVisiblityHandler
	 * @param defaultVisibilityManagement
	 */
	public FeatureDataAdapter(
			final SimpleFeatureType featureType,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler,
			final VisibilityManagement<SimpleFeature> defaultVisibilityManagement ) {

		super(
				customIndexHandlers,
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				fieldVisiblityHandler,
				updateVisibility(
						featureType,
						defaultVisibilityManagement));
		setFeatureType(featureType);
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------
	// Simplify for call from pyspark/jupyter
	public void init(
			final Index index ) {
		this.init(new Index[] {
			index
		});
	}

	@Override
	public void init(
			final Index... indices )
			throws RuntimeException {
		// TODO get projection here, make sure if multiple indices are given
		// that they match

		String indexCrsCode = null;
		for (final Index primaryindx : indices) {

			// for first iteration
			if (indexCrsCode == null) {
				if (primaryindx.getIndexModel() instanceof CustomCrsIndexModel) {
					indexCrsCode = ((CustomCrsIndexModel) primaryindx.getIndexModel()).getCrsCode();
				}
				else {
					indexCrsCode = GeometryUtils.DEFAULT_CRS_STR;
				}
			}
			else {
				if (primaryindx.getIndexModel() instanceof CustomCrsIndexModel) {
					// check if indexes have different CRS
					if (!indexCrsCode.equals(((CustomCrsIndexModel) primaryindx.getIndexModel()).getCrsCode())) {
						LOGGER.error("Multiple indices with different CRS is not supported");
						throw new RuntimeException(
								"Multiple indices with different CRS is not supported");
					}
					else {
						if (!indexCrsCode.equals(GeometryUtils.DEFAULT_CRS_STR)) {
							LOGGER.error("Multiple indices with different CRS is not supported");
							throw new RuntimeException(
									"Multiple indices with different CRS is not supported");
						}

					}
				}
			}
		}

		initCRS(indexCrsCode);
	}

	private void initCRS(
			String indexCrsCode ) {
		if ((indexCrsCode == null) || indexCrsCode.isEmpty()) {
			// TODO make sure we handle null/empty to make it default
			indexCrsCode = GeometryUtils.DEFAULT_CRS_STR;
		}
		CoordinateReferenceSystem persistedCRS = persistedFeatureType.getCoordinateReferenceSystem();

		if (persistedCRS == null) {
			persistedCRS = GeometryUtils.getDefaultCRS();
		}

		final CoordinateReferenceSystem indexCRS = decodeCRS(indexCrsCode);
		if (indexCRS.equals(persistedCRS)) {
			reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(
					persistedFeatureType,
					persistedCRS);
			transform = null;
		}
		else {
			reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(
					persistedFeatureType,
					indexCRS);
			try {
				transform = CRS.findMathTransform(
						persistedCRS,
						indexCRS,
						true);
			}
			catch (final FactoryException e) {
				LOGGER.warn(
						"Unable to create coordinate reference system transform",
						e);
			}
		}

		statsManager = new StatsManager(
				this,
				persistedFeatureType,
				reprojectedFeatureType,
				transform);
		secondaryIndexManager = new SecondaryIndexManager(
				persistedFeatureType,
				statsManager);
	}

	/**
	 * Helper method for establishing a visibility manager in the constructor
	 */
	private static SimpleFeatureType updateVisibility(
			final SimpleFeatureType featureType,
			final VisibilityManagement<SimpleFeature> defaultVisibilityManagement ) {
		final VisibilityConfiguration config = new VisibilityConfiguration(
				featureType);
		config.updateWithDefaultIfNeeded(
				featureType,
				defaultVisibilityManagement);

		return featureType;
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Set the FeatureType for this Data Adapter.
	 *
	 * @param featureType
	 *            - new feature type
	 */
	private void setFeatureType(
			final SimpleFeatureType featureType ) {
		persistedFeatureType = featureType;
		resetTimeDescriptors();
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Create List of NativeFieldHandlers based on the SimpleFeature type passed
	 * as parameter.
	 *
	 * @param featureType
	 *            - SFT to be used to determine handlers
	 *
	 * @return List of NativeFieldHandlers that correspond to attributes in
	 *         featureType
	 */
	protected List<NativeFieldHandler<SimpleFeature, Object>> getFieldHandlersFromFeatureType(
			final SimpleFeatureType featureType ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(
				featureType.getAttributeCount());

		for (final AttributeDescriptor attrDesc : featureType.getAttributeDescriptors()) {
			nativeHandlers.add(new FeatureAttributeHandler(
					attrDesc));
		}

		return nativeHandlers;
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Attempts to find a time descriptor (range or timestamp) within provided
	 * featureType and return index field handler for it.
	 *
	 * @param featureType
	 *            - feature type to be scanned.
	 *
	 * @return Index Field Handler for the time descriptor found in featureType
	 */
	protected IndexFieldHandler<SimpleFeature, Time, Object> getTimeRangeHandler(
			final SimpleFeatureType featureType ) {
		final VisibilityConfiguration config = new VisibilityConfiguration(
				featureType);
		final TimeDescriptors timeDescriptors = inferTimeAttributeDescriptor(featureType);

		if ((timeDescriptors.getStartRange() != null) && (timeDescriptors.getEndRange() != null)) {

			final FeatureAttributeHandler fah_startRange = new FeatureAttributeHandler(
					timeDescriptors.getStartRange());
			final FeatureAttributeHandler fah_endRange = new FeatureAttributeHandler(
					timeDescriptors.getEndRange());
			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler = config
					.getManager()
					.createVisibilityHandler(
							timeDescriptors.getStartRange().getLocalName(),
							fieldVisiblityHandler,
							config.getAttributeName());

			final FeatureTimeRangeHandler ftrh = new FeatureTimeRangeHandler(
					fah_startRange,
					fah_endRange,
					visibilityHandler);

			return (ftrh);
		}

		else if (timeDescriptors.getTime() != null) {
			// if we didn't succeed in identifying a start and end time,
			// just grab the first attribute and use it as a timestamp

			final FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler = config
					.getManager()
					.createVisibilityHandler(
							timeDescriptors.getTime().getLocalName(),
							fieldVisiblityHandler,
							config.getAttributeName());

			final FeatureTimestampHandler fth = new FeatureTimestampHandler(
					timeDescriptors.getTime(),
					visibilityHandler);

			return fth;
		}

		return null;
	}

	// -----------------------------------------------------------------------------------
	// -----------------------------------------------------------------------------------

	/**
	 * Get a List<> of the default index field handlers from the Simple Feature
	 * Type provided
	 *
	 * @param typeObj
	 *            - Simple Feature Type object
	 * @return - List of the default Index Field Handlers
	 */
	@Override
	protected List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
			final Object typeObj ) {
		if ((typeObj != null) && (typeObj instanceof SimpleFeatureType)) {

			final SimpleFeatureType internalType = (SimpleFeatureType) typeObj;
			final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> defaultHandlers = new ArrayList<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>();

			nativeFieldHandlers = getFieldHandlersFromFeatureType(internalType);

			// Add default handler for Time

			final IndexFieldHandler<SimpleFeature, Time, Object> timeHandler = getTimeRangeHandler(internalType);
			if (timeHandler != null) {
				defaultHandlers.add(timeHandler);
			}

			// Add default handler for Geometry

			final AttributeDescriptor descriptor = internalType.getGeometryDescriptor();
			final VisibilityConfiguration visConfig = new VisibilityConfiguration(
					internalType);

			defaultHandlers.add(new FeatureGeometryHandler(
					descriptor,
					visConfig.getManager().createVisibilityHandler(
							descriptor.getLocalName(),
							fieldVisiblityHandler,
							visConfig.getAttributeName())));

			return defaultHandlers;
		}

		LOGGER.warn("Simple Feature Type could not be used for handling the indexed data");
		return super.getDefaultTypeMatchingHandlers(reprojectedFeatureType);
	}

	/**
	 * Sets the namespace of the reprojected feature type associated with this
	 * data adapter
	 *
	 * @param namespaceURI
	 *            - new namespace URI
	 */
	public void setNamespace(
			final String namespaceURI ) {
		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.init(reprojectedFeatureType);
		builder.setNamespaceURI(namespaceURI);
		reprojectedFeatureType = builder.buildFeatureType();
	}

	// ----------------------------------------------------------------------------------
	/**
	 * Map of Field Readers associated with a Field ID
	 */
	private final Map<ByteArrayId, FieldReader<Object>> mapOfFieldIdToReaders = new HashMap<ByteArrayId, FieldReader<Object>>();

	/**
	 * {@inheritDoc}
	 *
	 * @return Field Reader for the given Field ID
	 */
	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		// Go to the map to get a reader for given fieldId

		FieldReader<Object> reader = mapOfFieldIdToReaders.get(fieldId);

		// Check the map to see if a reader has already been found.
		if (reader == null) {
			// Reader not in Map, go to the reprojected feature type and get the
			// default reader
			final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldId.getString());
			final Class<?> bindingClass = descriptor.getType().getBinding();
			reader = (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(bindingClass);

			// Add it to map for the next time
			mapOfFieldIdToReaders.put(
					fieldId,
					reader);
		}

		return reader;
	}

	// ----------------------------------------------------------------------------------
	/**
	 * Map of Field Writers associated with a Field ID
	 */
	private final Map<ByteArrayId, FieldWriter<SimpleFeature, Object>> mapOfFieldIdToWriters = new HashMap<ByteArrayId, FieldWriter<SimpleFeature, Object>>();

	/**
	 * {@inheritDoc}
	 *
	 * @return Field Writer for the given Field ID
	 */
	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		// Go to the map to get a writer for given fieldId

		FieldWriter<SimpleFeature, Object> writer = mapOfFieldIdToWriters.get(fieldId);

		// Check the map to see if a writer has already been found.
		if (writer == null) {
			final FieldVisibilityHandler<SimpleFeature, Object> handler = getLocalVisibilityHandler(fieldId);
			final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldId.getString());

			final Class<?> bindingClass = descriptor.getType().getBinding();
			if (handler != null) {
				writer = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(
						bindingClass,
						handler);
			}
			else {
				writer = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
			}
			if (writer == null) {
				LOGGER.error("BasicWriter not found for binding type:" + bindingClass.getName().toString());
			}

			mapOfFieldIdToWriters.put(
					fieldId,
					writer);
		}
		return writer;
	}

	// ----------------------------------------------------------------------------------

	private FieldVisibilityHandler<SimpleFeature, Object> getLocalVisibilityHandler(
			final ByteArrayId fieldId ) {
		final VisibilityConfiguration visConfig = new VisibilityConfiguration(
				reprojectedFeatureType);

		// See if there is a visibility config stored in the reprojected feature
		// type
		if (reprojectedFeatureType.getDescriptor(visConfig.getAttributeName()) == null) {
			// No, so return the default field visibility handler
			return fieldVisiblityHandler;
		}

		// Yes, then get the descriptor for the given field ID
		final AttributeDescriptor descriptor = reprojectedFeatureType.getDescriptor(fieldId.getString());

		return visConfig.getManager().createVisibilityHandler(
				descriptor.getLocalName(),
				fieldVisiblityHandler,
				visConfig.getAttributeName());
	}

	// ----------------------------------------------------------------------------------

	/**
	 * Get feature type default data contained in the reprojected SFT and
	 * serialize to a binary stream
	 *
	 * @return byte array with binary data
	 */
	@Override
	protected byte[] defaultTypeDataToBinary() {
		// serialize the persisted/reprojected feature type by using default
		// fields and
		// data types

		final String encodedType = DataUtilities.encodeType(persistedFeatureType);
		final String axis = FeatureDataUtils.getAxis(persistedFeatureType.getCoordinateReferenceSystem());
		final String typeName = reprojectedFeatureType.getTypeName();
		final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
		final byte[] axisBytes = StringUtils.stringToBinary(axis);
		//
		final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
		userDataConfiguration.addConfigurations(
				typeName,
				new TimeDescriptorConfiguration(
						persistedFeatureType));
		userDataConfiguration.addConfigurations(
				typeName,
				new SimpleFeatureStatsConfigurationCollection(
						persistedFeatureType));
		userDataConfiguration.addConfigurations(
				typeName,
				new VisibilityConfiguration(
						persistedFeatureType));
		byte[] attrBytes = userDataConfiguration.toBinary();
		final String namespace = reprojectedFeatureType.getName().getNamespaceURI();

		byte[] namespaceBytes;
		if ((namespace != null) && (namespace.length() > 0)) {
			namespaceBytes = StringUtils.stringToBinary(namespace);
		}
		else {
			namespaceBytes = new byte[0];
		}
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
		final CoordinateReferenceSystem crs = reprojectedFeatureType.getCoordinateReferenceSystem();
		final byte[] indexCrsBytes;
		if (crs != null) {
			indexCrsBytes = StringUtils.stringToBinary(CRS.toSRS(crs));
		}
		else {
			indexCrsBytes = new byte[0];
		}
		final byte[] secondaryIndexBytes = PersistenceUtils.toBinary(secondaryIndexManager);
		// 21 bytes is the 7 four byte length fields and one byte for the
		// version
		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + indexCrsBytes.length
				+ typeNameBytes.length + namespaceBytes.length + attrBytes.length + axisBytes.length
				+ secondaryIndexBytes.length + 25);

		buf.put(VERSION);
		buf.putInt(typeNameBytes.length);
		buf.putInt(indexCrsBytes.length);
		buf.putInt(namespaceBytes.length);
		buf.putInt(attrBytes.length);
		buf.putInt(axisBytes.length);
		buf.putInt(encodedTypeBytes.length);
		buf.put(typeNameBytes);
		buf.put(indexCrsBytes);
		buf.put(namespaceBytes);
		buf.put(attrBytes);
		buf.put(axisBytes);
		buf.put(encodedTypeBytes);
		buf.put(secondaryIndexBytes);
		return buf.array();
	}

	/**
	 * Extract the feature type default data from the binary stream passed in
	 * and place in the reprojected SFT for this feature data adapter
	 *
	 * @return if successful, the reprojected feature type created from the
	 *         serialized stream
	 */
	@Override
	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		try {
			GeometryUtils.initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.warn(
					"Unable to initialize GeoTools classloader",
					e);
		}
		// deserialize the feature type
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		// for now...do a gentle migration
		final byte versionId = buf.get();
		final byte[] typeNameBytes = new byte[buf.getInt()];

		// TODO: DONTMAINTAIN! Don't maintain after 1.0. This was specifically
		// made to support GeoWave 0.9.6 - 0.9.7 data
		final byte[] indexCrsBytes;
		if (versionId < VERSION) {
			LOGGER.warn("Mismatched Feature Data Adapter version");
			indexCrsBytes = new byte[0];
		}
		else {
			indexCrsBytes = new byte[buf.getInt()];
		}
		final byte[] namespaceBytes = new byte[buf.getInt()];

		final byte[] attrBytes = new byte[buf.getInt()];
		final byte[] axisBytes = new byte[buf.getInt()];
		final byte[] encodedTypeBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(indexCrsBytes);
		buf.get(namespaceBytes);
		buf.get(attrBytes);
		buf.get(axisBytes);
		buf.get(encodedTypeBytes);

		final String typeName = StringUtils.stringFromBinary(typeNameBytes);
		String namespace = StringUtils.stringFromBinary(namespaceBytes);
		if (namespace.length() == 0) {
			namespace = null;
		}

		// 21 bytes is the 7 four byte length fields and one byte for the
		// version
		// TODO: DONTMAINTAIN! Don't maintain after 1.0. This was specifically
		// made to support GeoWave 0.9.6 - 0.9.7 data
		final byte[] secondaryIndexBytes;
		if (versionId < VERSION) {
			secondaryIndexBytes = new byte[bytes.length - axisBytes.length - typeNameBytes.length
					- indexCrsBytes.length - namespaceBytes.length - attrBytes.length - encodedTypeBytes.length - 21];
		}
		else {
			secondaryIndexBytes = new byte[bytes.length - axisBytes.length - typeNameBytes.length
					- indexCrsBytes.length - namespaceBytes.length - attrBytes.length - encodedTypeBytes.length - 25];
		}
		buf.get(secondaryIndexBytes);

		final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
		try {
			final SimpleFeatureType myType = FeatureDataUtils.decodeType(
					namespace,
					typeName,
					encodedType,
					StringUtils.stringFromBinary(axisBytes));

			final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
			userDataConfiguration.addConfigurations(
					typeName,
					new TimeDescriptorConfiguration(
							myType));
			userDataConfiguration.addConfigurations(
					typeName,
					new SimpleFeatureStatsConfigurationCollection(
							myType));
			userDataConfiguration.addConfigurations(
					typeName,
					new VisibilityConfiguration(
							myType));
			userDataConfiguration.fromBinary(attrBytes);
			userDataConfiguration.updateType(myType);
			setFeatureType(myType);
			initCRS(indexCrsBytes.length > 0 ? StringUtils.stringFromBinary(indexCrsBytes) : null);
			// advertise the reprojected type externally
			return reprojectedFeatureType;
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to deserialized feature type",
					e);
		}

		secondaryIndexManager = (SecondaryIndexManager) PersistenceUtils.fromBinary(secondaryIndexBytes);

		return null;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(persistedFeatureType.getTypeName()));
	}

	@Override
	public boolean isSupported(
			final SimpleFeature entry ) {
		return reprojectedFeatureType.getName().getURI().equals(
				entry.getType().getName().getURI());
	}

	@Override
	public ByteArrayId getDataId(
			final SimpleFeature entry ) {
		return new ByteArrayId(
				StringUtils.stringToBinary(entry.getID()));
	}

	private ThreadLocal<FeatureRowBuilder> builder = null;

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		if (builder == null) {
			builder = new ThreadLocal<FeatureRowBuilder>() {
				@Override
				protected FeatureRowBuilder initialValue() {
					return new FeatureRowBuilder(
							reprojectedFeatureType);
				}
			};
		}
		return builder.get();
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		if (reprojectedFeatureType == null) {
			return persistedFeatureType;
		}
		return reprojectedFeatureType;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {

		if (transform != null) {
			return super.encode(
					FeatureDataUtils.crsTransform(
							entry,
							reprojectedFeatureType,
							transform),
					indexModel);
		}
		return super.encode(
				entry,
				indexModel);

	}

	@Override
	public ByteArrayId[] getSupportedStatisticsTypes() {
		return statsManager.getSupportedStatisticsIds();
	}

	@Override
	public InternalDataStatistics<SimpleFeature> createDataStatistics(
			final ByteArrayId statisticsId ) {
		return statsManager.createDataStatistics(statisticsId);
	}

	@Override
	public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final CommonIndexModel indexModel,
			final DataTypeAdapter<SimpleFeature> adapter,
			final ByteArrayId statisticsId ) {
		return statsManager.getVisibilityHandler(
				indexModel,
				adapter,
				statisticsId);
	}

	@Override
	public boolean hasTemporalConstraints() {
		return getTimeDescriptors().hasTime();
	}

	public synchronized void resetTimeDescriptors() {
		timeDescriptors = inferTimeAttributeDescriptor(persistedFeatureType);
	}

	@Override
	public synchronized TimeDescriptors getTimeDescriptors() {
		if (timeDescriptors == null) {
			timeDescriptors = inferTimeAttributeDescriptor(persistedFeatureType);
		}
		return timeDescriptors;
	}

	/**
	 * Determine if a time or range descriptor is set. If so, then use it,
	 * otherwise infer.
	 *
	 * @param persistType
	 *            - FeatureType that will be scanned for TimeAttributes
	 * @return
	 */
	protected static final TimeDescriptors inferTimeAttributeDescriptor(
			final SimpleFeatureType persistType ) {

		final TimeDescriptorConfiguration config = new TimeDescriptorConfiguration(
				persistType);
		final TimeDescriptors timeDescriptors = new TimeDescriptors(
				persistType,
				config);

		// Up the meta-data so that it is clear and visible any inference that
		// has occurred here. Also, this is critical to
		// serialization/deserialization

		config.updateType(persistType);
		return timeDescriptors;
	}

	@Override
	public HadoopWritableSerializer<SimpleFeature, FeatureWritable> createWritableSerializer() {
		return new FeatureWritableSerializer(
				reprojectedFeatureType);
	}

	private static class FeatureWritableSerializer implements
			HadoopWritableSerializer<SimpleFeature, FeatureWritable>
	{
		private final FeatureWritable writable;

		FeatureWritableSerializer(
				final SimpleFeatureType type ) {
			writable = new FeatureWritable(
					type);
		}

		@Override
		public FeatureWritable toWritable(
				final SimpleFeature entry ) {
			writable.setFeature(entry);
			return writable;
		}

		@Override
		public SimpleFeature fromWritable(
				final FeatureWritable writable ) {
			return writable.getFeature();
		}

	}

	@Override
	public List<SecondaryIndexImpl<SimpleFeature>> getSupportedSecondaryIndices() {
		return secondaryIndexManager.getSupportedSecondaryIndices();
	}

	private transient final BiMap<ByteArrayId, Integer> fieldToPositionMap = HashBiMap.create();
	private transient BiMap<Integer, ByteArrayId> positionToFieldMap = null;
	private transient final Map<String, List<ByteArrayId>> modelToDimensionsMap = new ConcurrentHashMap<>();
	private transient volatile boolean positionMapsInitialized = false;

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {
		final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
		// first check CommonIndexModel dimensions
		if (dimensionFieldIds.contains(fieldId)) {
			return dimensionFieldIds.indexOf(fieldId);
		}
		if (!positionMapsInitialized) {
			synchronized (this) {
				initializePositionMaps();
			}
		}
		// next check other fields
		// dimension fields must be first, add padding
		final Integer position = fieldToPositionMap.get(fieldId);
		if (position == null) {
			return -1;
		}
		return position.intValue() + dimensionFieldIds.size();
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
		if (position >= dimensionFieldIds.size()) {
			final int adjustedPosition = position - dimensionFieldIds.size();
			if (!positionMapsInitialized) {
				synchronized (this) {
					initializePositionMaps();
				}
			}
			// check other fields
			return positionToFieldMap.get(adjustedPosition);
		}
		// otherwise check CommonIndexModel dimensions
		return dimensionFieldIds.get(position);
	}

	private void initializePositionMaps() {
		if (positionMapsInitialized) {
			return;
		}
		try {
			for (int i = 0; i < reprojectedFeatureType.getAttributeCount(); i++) {
				final AttributeDescriptor ad = reprojectedFeatureType.getDescriptor(i);
				final ByteArrayId currFieldId = new ByteArrayId(
						ad.getLocalName());
				fieldToPositionMap.forcePut(
						currFieldId,
						i);
			}
			positionToFieldMap = fieldToPositionMap.inverse();
			positionMapsInitialized = true;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to initialize position map, continuing anyways",
					e);
		}
	}

	protected List<ByteArrayId> getDimensionFieldIds(
			final CommonIndexModel model ) {
		final List<ByteArrayId> retVal = modelToDimensionsMap.get(model.getId());
		if (retVal != null) {
			return retVal;
		}
		final List<ByteArrayId> dimensionFieldIds = DataStoreUtils.getUniqueDimensionFields(model);
		modelToDimensionsMap.put(
				model.getId(),
				dimensionFieldIds);
		return dimensionFieldIds;
	}

	public static CoordinateReferenceSystem decodeCRS(
			final String crsCode ) {

		CoordinateReferenceSystem crs = null;
		try {
			crs = CRS.decode(
					crsCode,
					true);
		}
		catch (final FactoryException e) {
			LOGGER.error(
					"Unable to decode '" + crsCode + "' CRS",
					e);
			throw new RuntimeException(
					"Unable to initialize '" + crsCode + "' object",
					e);
		}

		return crs;

	}
}
