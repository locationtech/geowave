package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.AdaptorProxyFieldLevelVisibilityHandler;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.HadoopDataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.HadoopWritableSerializer;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

/**
 * This data adapter will handle all reading/writing concerns for storing and
 * retrieving GeoTools SimpleFeature objects to and from a GeoWave persistent
 * store in Accumulo.
 * 
 * If the implementor needs to write rows with particular visibility, this can
 * be done by providing a FieldVisibilityHandler to a constructor or a
 * VisibilityManagement to a constructor. When using VisibilityManagement, the
 * feature attribute to contain the visibility meta-data is, by default, called
 * GEOWAVE_VISIBILITY. This can be overridden by setting the UserData property
 * 'visibility' to Boolean.TRUE for the feature attribute that describes the
 * attribute that contains the visibility meta-data.
 * persistedType.getDescriptor("someAttributeName").getUserData().put(
 * "visibility", Boolean.TRUE)
 * 
 * 
 * The adapter will use the SimpleFeature's default geometry for spatial
 * indexing.
 * 
 * The adaptor will use the first temporal attribute (a Calendar or Date object)
 * as the timestamp of a temporal index.
 * 
 * If the feature type contains a UserData property 'time' for a specific time
 * attribute with Boolean.TRUE, then the attribute is used as the timestamp of a
 * temporal index.
 * 
 * If the feature type contains UserData properties 'start' and 'end' for two
 * different time attributes with value Boolean.TRUE, then the attributes are
 * used for a range index.
 * 
 * If the feature type contains a UserData property 'time' for *all* time
 * attributes with Boolean.FALSE, then a temporal index is not used.
 * 
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
@SuppressWarnings("unchecked")
public class FeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature> implements
		StatisticalDataAdapter<SimpleFeature>,
		HadoopDataAdapter<SimpleFeature, FeatureWritable>
{
	private final static Logger LOGGER = Logger.getLogger(FeatureDataAdapter.class);
	// the original coordinate system will always be represented internally by
	// the persisted type
	private SimpleFeatureType persistedType;

	// externally the reprojected type will always be advertised because all
	// features will be reprojected to EPSG:4326 and the advertised feature type
	// from the data adapter should match in CRS
	private SimpleFeatureType reprojectedType;
	private MathTransform transform;
	private StatsManager statsManager;

	private String visibilityAttributeName = "GEOWAVE_VISIBILITY";
	private VisibilityManagement<SimpleFeature> fieldVisibilityManagement;
	private TimeDescriptors timeDescriptors = null;

	// should change this anytime the serialized image changes. Stay negative.
	// so 0xa0, 0xa1, 0xa2 etc.
	final static byte VERSION = (byte) 0xa0;

	protected FeatureDataAdapter() {}

	public FeatureDataAdapter(
			final SimpleFeatureType type ) {
		this(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>());
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final VisibilityManagement<SimpleFeature> visibilityManagement ) {
		this(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				null,
				visibilityManagement);
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this(
				type,
				customIndexHandlers,
				null,
				new JsonDefinitionColumnVisibilityManagement<SimpleFeature>());
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		this(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				fieldVisiblityHandler,
				new JsonDefinitionColumnVisibilityManagement<SimpleFeature>());
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler,
			final VisibilityManagement<SimpleFeature> visibilityManagement ) {
		super(
				customIndexHandlers,
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				type);
		setFeatureType(type);
		this.fieldVisiblityHandler = fieldVisiblityHandler;
		fieldVisibilityManagement = visibilityManagement;
	}

	private void setFeatureType(
			final SimpleFeatureType type ) {
		persistedType = type;
		determineVisibilityAttribute();
		if (!GeoWaveGTDataStore.DEFAULT_CRS.equals(type.getCoordinateReferenceSystem())) {
			reprojectedType = SimpleFeatureTypeBuilder.retype(
					type,
					GeoWaveGTDataStore.DEFAULT_CRS);
			if (type.getCoordinateReferenceSystem() != null) {
				try {
					transform = CRS.findMathTransform(
							type.getCoordinateReferenceSystem(),
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
		else {
			reprojectedType = persistedType;
		}
		resetTimeDescriptors();
		statsManager = new StatsManager(
				this,
				persistedType,
				reprojectedType,
				transform);
	}

	private static List<NativeFieldHandler<SimpleFeature, Object>> typeToFieldHandlers(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(
				type.getAttributeCount());
		for (final AttributeDescriptor attrDesc : type.getAttributeDescriptors()) {
			nativeHandlers.add(new FeatureAttributeHandler(
					attrDesc));
		}
		return nativeHandlers;
	}

	public String getVisibilityAttributeName() {
		return visibilityAttributeName;
	}

	public VisibilityManagement<SimpleFeature> getFieldVisibilityManagement() {
		return fieldVisibilityManagement;
	}

	private IndexFieldHandler<SimpleFeature, Time, Object> getTimeRangeHandler(
			final SimpleFeatureType featureType ) {
		final TimeDescriptors timeDescriptors = inferTimeAttributeDescriptor(featureType);
		if ((timeDescriptors.getStartRange() != null) && (timeDescriptors.getEndRange() != null)) {
			return (new FeatureTimeRangeHandler(
					new FeatureAttributeHandler(
							timeDescriptors.getStartRange()),
					new FeatureAttributeHandler(
							timeDescriptors.getEndRange()),
					new AdaptorProxyFieldLevelVisibilityHandler(
							timeDescriptors.getStartRange().getLocalName(),
							this)));
		}
		else if (timeDescriptors.getTime() != null) {
			// if we didn't succeed in identifying a start and end time,
			// just grab the first attribute and use it as a timestamp
			return new FeatureTimestampHandler(
					timeDescriptors.getTime(),
					new AdaptorProxyFieldLevelVisibilityHandler(
							timeDescriptors.getTime().getLocalName(),
							this));
		}
		return null;
	}

	@Override
	protected List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
			final Object typeObj ) {
		if ((typeObj != null) && (typeObj instanceof SimpleFeatureType)) {
			final SimpleFeatureType internalType = (SimpleFeatureType) typeObj;

			nativeFieldHandlers = typeToFieldHandlers((SimpleFeatureType) typeObj);
			final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> defaultHandlers = new ArrayList<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>();
			final IndexFieldHandler<SimpleFeature, Time, Object> timeHandler = getTimeRangeHandler(internalType);
			if (timeHandler != null) {
				defaultHandlers.add(timeHandler);
			}

			defaultHandlers.add(new FeatureGeometryHandler(
					internalType.getGeometryDescriptor(),
					new AdaptorProxyFieldLevelVisibilityHandler(
							internalType.getGeometryDescriptor().getLocalName(),
							this)));
			return defaultHandlers;
		}
		LOGGER.warn("Simple Feature Type could not be used for handling the indexed data");
		return super.getDefaultTypeMatchingHandlers(reprojectedType);
	}

	public void setNamespace(
			final String namespaceURI ) {
		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.init(reprojectedType);
		builder.setNamespaceURI(namespaceURI);
		reprojectedType = builder.buildFeatureType();
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		final AttributeDescriptor descriptor = reprojectedType.getDescriptor(StringUtils.stringFromBinary(fieldId.getBytes()));
		final Class<?> bindingClass = descriptor.getType().getBinding();
		return (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(bindingClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {

		final AttributeDescriptor descriptor = reprojectedType.getDescriptor(StringUtils.stringFromBinary(fieldId.getBytes()));

		final Class<?> bindingClass = descriptor.getType().getBinding();
		FieldWriter<SimpleFeature, Object> basicWriter;
		if (fieldVisiblityHandler != null) {
			basicWriter = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(
					bindingClass,
					fieldVisiblityHandler);
		}
		else {
			basicWriter = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
		}
		if (basicWriter == null) {
			LOGGER.error("BasicWriter not found for binding type:" + bindingClass.getName().toString());
		}
		return fieldVisibilityManagement.createVisibilityWriter(
				descriptor.getLocalName(),
				basicWriter,
				fieldVisiblityHandler,
				visibilityAttributeName);
	}

	@Override
	protected byte[] defaultTypeDataToBinary() {
		// serialize the feature type
		final String encodedType = DataUtilities.encodeType(persistedType);
		final String axis = FeatureDataUtils.getAxis(persistedType.getCoordinateReferenceSystem());
		final String typeName = reprojectedType.getTypeName();
		final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
		final byte[] fieldVisibilityAtributeNameBytes = StringUtils.stringToBinary(visibilityAttributeName);
		final byte[] visibilityManagementClassNameBytes = StringUtils.stringToBinary(fieldVisibilityManagement.getClass().getCanonicalName());
		final byte[] axisBytes = StringUtils.stringToBinary(axis);
		byte[] attrBytes = new byte[0];

		final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
		userDataConfiguration.addConfigurations(new TimeDescriptorConfiguration(
				persistedType));
		userDataConfiguration.addConfigurations(new SimpleFeatureStatsConfigurationCollection(
				persistedType));
		try {
			attrBytes = StringUtils.stringToBinary(userDataConfiguration.asJsonString());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failure to encode simple feature user data configuration",
					e);
		}

		final String namespace = reprojectedType.getName().getNamespaceURI();

		byte[] namespaceBytes;
		if ((namespace != null) && (namespace.length() > 0)) {
			namespaceBytes = StringUtils.stringToBinary(namespace);
		}
		else {
			namespaceBytes = new byte[0];
		}
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
		// 25 bytes is the 6 four byte length fields and one byte for the
		// version
		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + typeNameBytes.length + namespaceBytes.length + fieldVisibilityAtributeNameBytes.length + visibilityManagementClassNameBytes.length + attrBytes.length + axisBytes.length + 25);
		buf.put(VERSION);
		buf.putInt(typeNameBytes.length);
		buf.putInt(namespaceBytes.length);
		buf.putInt(fieldVisibilityAtributeNameBytes.length);
		buf.putInt(visibilityManagementClassNameBytes.length);
		buf.putInt(attrBytes.length);
		buf.putInt(axisBytes.length);
		buf.put(typeNameBytes);
		buf.put(namespaceBytes);
		buf.put(fieldVisibilityAtributeNameBytes);
		buf.put(visibilityManagementClassNameBytes);
		buf.put(attrBytes);
		buf.put(axisBytes);
		buf.put(encodedTypeBytes);

		return buf.array();
	}

	@Override
	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		// deserialize the feature type
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		// for now...do a gentle migration
		final byte versionId = buf.get();
		if (versionId != VERSION) {
			LOGGER.warn("Mismatched Feature Data Adapter version");
		}
		final byte[] typeNameBytes = new byte[buf.getInt()];
		final byte[] namespaceBytes = new byte[buf.getInt()];
		final byte[] fieldVisibilityAtributeNameBytes = new byte[buf.getInt()];
		final byte[] visibilityManagementClassNameBytes = new byte[buf.getInt()];
		final byte[] attrBytes = new byte[buf.getInt()];
		final byte[] axisBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(namespaceBytes);
		buf.get(fieldVisibilityAtributeNameBytes);
		buf.get(visibilityManagementClassNameBytes);
		buf.get(attrBytes);
		buf.get(axisBytes);

		final String typeName = StringUtils.stringFromBinary(typeNameBytes);
		String namespace = StringUtils.stringFromBinary(namespaceBytes);
		if (namespace.length() == 0) {
			namespace = null;
		}
		visibilityAttributeName = StringUtils.stringFromBinary(fieldVisibilityAtributeNameBytes);
		final String visibilityManagementClassName = StringUtils.stringFromBinary(visibilityManagementClassNameBytes);
		try {
			fieldVisibilityManagement = (VisibilityManagement<SimpleFeature>) Class.forName(
					visibilityManagementClassName).newInstance();
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot instantiate " + visibilityManagementClassName,
					ex);
		}
		// 25 bytes is the 6 four byte length fields and one byte for the
		// version
		final byte[] encodedTypeBytes = new byte[bytes.length - axisBytes.length - typeNameBytes.length - namespaceBytes.length - fieldVisibilityAtributeNameBytes.length - visibilityManagementClassNameBytes.length - attrBytes.length - 25];
		buf.get(encodedTypeBytes);

		final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
		try {
			final SimpleFeatureType myType = FeatureDataUtils.decodeType(
					namespace,
					typeName,
					encodedType,
					StringUtils.stringFromBinary(axisBytes));

			final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
			userDataConfiguration.addConfigurations(new TimeDescriptorConfiguration(
					myType));
			userDataConfiguration.addConfigurations(new SimpleFeatureStatsConfigurationCollection(
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
			if (persistedType.getDescriptor(visibilityAttributeName) != null) {
				persistedType.getDescriptor(
						visibilityAttributeName).getUserData().put(
						"visibility",
						Boolean.TRUE);
			}

			// advertise the reprojected type externally
			return reprojectedType;
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to deserialized feature type",
					e);
		}

		return null;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(reprojectedType.getTypeName()));
	}

	@Override
	public boolean isSupported(
			final SimpleFeature entry ) {
		return reprojectedType.getName().getURI().equals(
				entry.getType().getName().getURI());
	}

	@Override
	public ByteArrayId getDataId(
			final SimpleFeature entry ) {
		return new ByteArrayId(
				StringUtils.stringToBinary(entry.getID()));
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new FeatureRowBuilder(
				reprojectedType);
	}

	public SimpleFeatureType getType() {
		return reprojectedType;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {

		return super.encode(
				FeatureDataUtils.defaultCRSTransform(
						entry,
						persistedType,
						reprojectedType,
						transform),
				indexModel);
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
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
	public DataStatisticsVisibilityHandler<SimpleFeature> getVisibilityHandler(
			final ByteArrayId statisticsId ) {
		return statsManager.getVisibilityHandler(statisticsId);
	}

	public boolean hasTemporalConstraints() {
		return typeMatchingFieldHandlers.keySet().contains(
				Time.class) || typeMatchingFieldHandlers.keySet().contains(
				FeatureTimestampHandler.class) || typeMatchingFieldHandlers.keySet().contains(
				FeatureTimeRangeHandler.class);
	}

	public synchronized void resetTimeDescriptors() {
		timeDescriptors = inferTimeAttributeDescriptor(persistedType);
	}

	public synchronized TimeDescriptors getTimeDescriptors() {
		if (timeDescriptors == null) {
			timeDescriptors = inferTimeAttributeDescriptor(persistedType);
		}
		return timeDescriptors;
	}

	/**
	 * Determine if a time or range descriptor is set. If so, then user it,
	 * otherwise infer.
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

	private void determineVisibilityAttribute() {
		for (final AttributeDescriptor attrDesc : persistedType.getAttributeDescriptors()) {
			final Boolean isVisibility = (Boolean) attrDesc.getUserData().get(
					"visibility");
			if ((isVisibility != null) && isVisibility.booleanValue()) {
				visibilityAttributeName = attrDesc.getLocalName();
			}
		}
	}

	@Override
	public HadoopWritableSerializer<SimpleFeature, FeatureWritable> createWritableSerializer() {
		return new FeatureWritableSerializer(
				reprojectedType);
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
}
