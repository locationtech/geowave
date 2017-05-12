package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import mil.nga.giat.geowave.adapter.vector.index.SecondaryIndexManager;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityConfiguration;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataAdapter;
import mil.nga.giat.geowave.mapreduce.HadoopDataAdapter;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializer;

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
 * {@link mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection}
 * . The collection maintains a set of
 * {@link mil.nga.giat.geowave.adapter.vector.stats.StatsConfig}, one for each
 * type of statistic. The default statistics for geometry and temporal
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
	final static byte VERSION = (byte) 0xa2;

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
		// If the CRS for the new FeatureType is the DEFAULT_CRS, then setup the
		// reprojected type based on the persisted type

		if (GeoWaveGTDataStore.DEFAULT_CRS.equals(featureType.getCoordinateReferenceSystem())) {
			reprojectedFeatureType = persistedFeatureType;
		}
		else {
			reprojectedFeatureType = SimpleFeatureTypeBuilder.retype(
					featureType,
					GeoWaveGTDataStore.DEFAULT_CRS);
			if (featureType.getCoordinateReferenceSystem() != null) {
				try {
					transform = CRS.findMathTransform(
							featureType.getCoordinateReferenceSystem(),
							GeoWaveGTDataStore.DEFAULT_CRS,
							true);
				}
				catch (final FactoryException e) {
					LOGGER.warn(
							"Unable to create coordinate reference system transform",
							e);
				}
			}
		}

		resetTimeDescriptors();
		statsManager = new StatsManager(
				this,
				persistedFeatureType,
				reprojectedFeatureType,
				transform);
		secondaryIndexManager = new SecondaryIndexManager(
				this,
				persistedFeatureType,
				statsManager);
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

			FeatureAttributeHandler fah_startRange = new FeatureAttributeHandler(
					timeDescriptors.getStartRange());
			FeatureAttributeHandler fah_endRange = new FeatureAttributeHandler(
					timeDescriptors.getEndRange());
			FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler = config
					.getManager()
					.createVisibilityHandler(
							timeDescriptors.getStartRange().getLocalName(),
							fieldVisiblityHandler,
							config.getAttributeName());

			FeatureTimeRangeHandler ftrh = new FeatureTimeRangeHandler(
					fah_startRange,
					fah_endRange,
					visibilityHandler);

			return (ftrh);
		}

		else if (timeDescriptors.getTime() != null) {
			// if we didn't succeed in identifying a start and end time,
			// just grab the first attribute and use it as a timestamp

			FieldVisibilityHandler<SimpleFeature, Object> visibilityHandler = config
					.getManager()
					.createVisibilityHandler(
							timeDescriptors.getTime().getLocalName(),
							fieldVisiblityHandler,
							config.getAttributeName());

			FeatureTimestampHandler fth = new FeatureTimestampHandler(
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
		byte[] attrBytes = new byte[0];

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

		try {
			attrBytes = StringUtils.stringToBinary(userDataConfiguration.asJsonString());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failure to encode simple feature user data configuration",
					e);
		}

		final String namespace = reprojectedFeatureType.getName().getNamespaceURI();

		byte[] namespaceBytes;
		if ((namespace != null) && (namespace.length() > 0)) {
			namespaceBytes = StringUtils.stringToBinary(namespace);
		}
		else {
			namespaceBytes = new byte[0];
		}
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
		final byte[] secondaryIndexBytes = PersistenceUtils.toBinary(secondaryIndexManager);
		// 21 bytes is the 7 four byte length fields and one byte for the
		// version
		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + typeNameBytes.length
				+ namespaceBytes.length + attrBytes.length + axisBytes.length + secondaryIndexBytes.length + 21);
		buf.put(VERSION);
		buf.putInt(typeNameBytes.length);
		buf.putInt(namespaceBytes.length);
		buf.putInt(attrBytes.length);
		buf.putInt(axisBytes.length);
		buf.putInt(encodedTypeBytes.length);
		buf.put(typeNameBytes);
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
			FeatureDataUtils.initClassLoader();
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
		if (versionId != VERSION) {
			LOGGER.warn("Mismatched Feature Data Adapter version");
		}
		final byte[] typeNameBytes = new byte[buf.getInt()];
		final byte[] namespaceBytes = new byte[buf.getInt()];

		final byte[] attrBytes = new byte[buf.getInt()];
		final byte[] axisBytes = new byte[buf.getInt()];
		final byte[] encodedTypeBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(namespaceBytes);
		buf.get(attrBytes);
		buf.get(axisBytes);
		buf.get(encodedTypeBytes);

		final String typeName = StringUtils.stringFromBinary(typeNameBytes);
		String namespace = StringUtils.stringFromBinary(namespaceBytes);
		if (namespace.length() == 0) {
			namespace = null;
		}

		// 24 bytes is the 6 four byte length fields and one byte for the
		// version
		final byte[] secondaryIndexBytes = new byte[bytes.length - axisBytes.length - typeNameBytes.length
				- namespaceBytes.length - attrBytes.length - encodedTypeBytes.length - 29];
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
			try {
				userDataConfiguration.fromJsonString(
						StringUtils.stringFromBinary(attrBytes),
						myType);

			}
			catch (final IOException e) {
				LOGGER.error(
						"Failure to decode simple feature user data configuration",
						e);
			}
			setFeatureType(myType);

			// advertise the reprojected type externally
			return reprojectedFeatureType;
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to deserialized feature type",
					e);
		}

		secondaryIndexManager = PersistenceUtils.fromBinary(
				secondaryIndexBytes,
				SecondaryIndexManager.class);

		return null;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(reprojectedFeatureType.getTypeName()));
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

	private FeatureRowBuilder builder;

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		if (builder == null) {
			builder = new FeatureRowBuilder(
					reprojectedFeatureType);
		}
		return builder;
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		return reprojectedFeatureType;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		return super.encode(
				FeatureDataUtils.defaultCRSTransform(
						entry,
						persistedFeatureType,
						reprojectedFeatureType,
						transform),
				indexModel);
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsTypes() {
		return statsManager.getSupportedStatisticsIds();
	}

	@Override
	public DataStatistics<SimpleFeature> createDataStatistics(
			final ByteArrayId statisticsId ) {
		return statsManager.createDataStatistics(
				this,
				statisticsId);
	}

	@Override
	public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return statsManager.getVisibilityHandler(statisticsId);
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
	public List<SecondaryIndex<SimpleFeature>> getSupportedSecondaryIndices() {
		return secondaryIndexManager.getSupportedSecondaryIndices();
	}

	private transient final BiMap<ByteArrayId, Integer> fieldToPositionMap = HashBiMap.create();
	private transient BiMap<Integer, ByteArrayId> positionToFieldMap = null;
	private transient final Map<String, List<ByteArrayId>> modelToDimensionsMap = new HashMap<>();

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {
		final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
		// first check CommonIndexModel dimensions
		if (dimensionFieldIds.contains(fieldId)) {
			return dimensionFieldIds.indexOf(fieldId);
		}
		if (fieldToPositionMap.isEmpty()) {
			initializePositionMaps();
		}
		// next check other fields
		// dimension fields must be first, add padding
		Integer position = fieldToPositionMap.get(fieldId);
		if (position == null) {
			return -1;
		}
		return position.intValue() + model.getDimensions().length;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		if (position >= model.getDimensions().length) {
			if (fieldToPositionMap.isEmpty()) {
				initializePositionMaps();
			}
			final int adjustedPosition = position - model.getDimensions().length;
			// check other fields
			return positionToFieldMap.get(adjustedPosition);
		}
		final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
		// otherwise check CommonIndexModel dimensions
		return dimensionFieldIds.get(position);
	}

	private void initializePositionMaps() {
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
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to initialize position map, continuing anyways",
					e);
		}
	}

	private List<ByteArrayId> getDimensionFieldIds(
			final CommonIndexModel model ) {
		final List<ByteArrayId> retVal = modelToDimensionsMap.get(model.getId());
		if (retVal != null) {
			return retVal;
		}
		final List<ByteArrayId> dimensionFieldIds = new ArrayList<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : model.getDimensions()) {
			dimensionFieldIds.add(dimension.getFieldId());
		}
		modelToDimensionsMap.put(
				model.getId(),
				dimensionFieldIds);
		return dimensionFieldIds;
	}
}
