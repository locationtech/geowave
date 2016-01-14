package mil.nga.giat.geowave.adapter.vector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.field.SimpleFeatureSerializationProvider;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.AdaptorProxyFieldLevelVisibilityHandler;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

// FIXME currently does not re-project (i.e. assumes EPSG:4326)
// FIXME currently does not support stats
// FIXME currently does not support secondary indexing
// FIXME currently does nto support MapReduce
public class KryoFeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature> implements
		GeotoolsFeatureDataAdapter
{
	private final static Logger LOGGER = Logger.getLogger(KryoFeatureDataAdapter.class);
	protected SimpleFeatureType featureType;
	private ByteArrayId adapterId;
	private VisibilityManagement<SimpleFeature> fieldVisibilityManagement = new JsonDefinitionColumnVisibilityManagement<SimpleFeature>();

	protected KryoFeatureDataAdapter() {}

	public KryoFeatureDataAdapter(
			final SimpleFeatureType featureType ) {
		super(
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				featureType);
		this.featureType = featureType;
		adapterId = new ByteArrayId(
				StringUtils.stringToBinary(featureType.getTypeName()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		// final Class<?> clazz = SimpleFeature.class;
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		final Class<?> clazz = SimpleFeature.class;
		return (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(clazz);
	}

	@Override
	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	@Override
	public boolean isSupported(
			final SimpleFeature entry ) {
		return entry.getName().getURI().equals(
				featureType.getName().getURI());
	}

	@Override
	public ByteArrayId getDataId(
			final SimpleFeature entry ) {
		return new ByteArrayId(
				StringUtils.stringToBinary(entry.getID()));
	}

	@Override
	public SimpleFeature decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		final RowBuilder<SimpleFeature, Object> builder = newBuilder();
		builder.setField(data.getAdapterExtendedData().getValues().get(
				0));
		return builder.buildRow(data.getDataId());
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		return super.encode(
				entry,
				indexModel);
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new WholeFeatureRowBuilder();
	}

	@Override
	protected byte[] defaultTypeDataToBinary() {
		final String encodedType = DataUtilities.encodeType(featureType);
		final String typeName = featureType.getTypeName();
		final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
		final byte[] visibilityManagementClassNameBytes = StringUtils.stringToBinary(fieldVisibilityManagement.getClass().getCanonicalName());
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + typeNameBytes.length + visibilityManagementClassNameBytes.length + adapterId.getBytes().length + 16);
		buf.putInt(typeNameBytes.length);
		buf.putInt(visibilityManagementClassNameBytes.length);
		buf.putInt(encodedTypeBytes.length);
		buf.putInt(adapterId.getBytes().length);
		buf.put(typeNameBytes);
		buf.put(visibilityManagementClassNameBytes);
		buf.put(encodedTypeBytes);
		buf.put(adapterId.getBytes());
		return buf.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] typeNameBytes = new byte[buf.getInt()];
		final byte[] visibilityManagementClassNameBytes = new byte[buf.getInt()];
		final byte[] encodedTypeBytes = new byte[buf.getInt()];
		final byte[] adapterIdBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(visibilityManagementClassNameBytes);
		buf.get(encodedTypeBytes);
		buf.get(adapterIdBytes);
		adapterId = new ByteArrayId(
				adapterIdBytes);

		final String typeName = StringUtils.stringFromBinary(typeNameBytes);
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

		final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
		try {
			final SimpleFeatureType myType = DataUtilities.createType(
					typeName,
					encodedType);

			featureType = myType;
			return featureType;
		}
		catch (final SchemaException e) {
			LOGGER.error(
					"Unable to deserialized feature type",
					e);
		}

		return null;
	}

	// FIXME copy/paste from FeatureDataAdater ... abstract!
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
		// LOGGER.warn("Simple Feature Type could not be used for handling the indexed data");
		return super.getDefaultTypeMatchingHandlers(featureType);
	}

	private static List<NativeFieldHandler<SimpleFeature, Object>> typeToFieldHandlers(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>();
		nativeHandlers.add(new WholeFeatureHandler(
				type));
		return nativeHandlers;
	}

	// FIXME copy/paste from FeatureDataAdater ... abstract!
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

	// FIXME copy/paste from FeatureDataAdater ... abstract!
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

	// FIXME copy/paste from FeatureDataAdater ... abstract!
	public String getVisibilityAttributeName() {
		return "GEOWAVE_VISIBILITY";
	}

	// FIXME copy/paste from FeatureDataAdater ... abstract!
	public VisibilityManagement<SimpleFeature> getFieldVisibilityManagement() {
		return fieldVisibilityManagement;
	}

	@Override
	public SimpleFeatureType getType() {
		return featureType;
	}

	@Override
	public synchronized TimeDescriptors getTimeDescriptors() {
		return inferTimeAttributeDescriptor(featureType);
	}

	@Override
	public ByteArrayId[] getSupportedStatisticsIds() {
		return new ByteArrayId[] {};
	}

	@Override
	public DataStatistics<SimpleFeature> createDataStatistics(
			ByteArrayId statisticsId ) {
		return null;
	}

	@Override
	public EntryVisibilityHandler<SimpleFeature> getVisibilityHandler(
			ByteArrayId statisticsId ) {
		return new EntryVisibilityHandler<SimpleFeature>() {

			@Override
			public byte[] getVisibility(
					DataStoreEntryInfo entryInfo,
					SimpleFeature entry ) {
				// TODO Auto-generated method stub
				return new byte[] {};
			}
		};
	}

}
