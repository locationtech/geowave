package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.field.SimpleFeatureSerializationProvider;
import mil.nga.giat.geowave.adapter.vector.index.SimpleFeaturePrimaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.stats.StatsManager;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Deprecated as of 0.9.1
 */
@Deprecated
public class WholeFeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature> implements
		GeotoolsFeatureDataAdapter,
		StatisticsProvider<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(WholeFeatureDataAdapter.class);
	protected SimpleFeatureType featureType;
	private ByteArrayId adapterId;
	private SimpleFeatureBuilder b;
	private StatsManager statsManager;

	protected WholeFeatureDataAdapter() {
		super();
	}

	public WholeFeatureDataAdapter(
			final SimpleFeatureType featureType ) {
		super(
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				null,
				featureType);
		this.featureType = featureType;
		adapterId = new ByteArrayId(
				StringUtils.stringToBinary(featureType.getTypeName()));
		statsManager = new StatsManager(
				this,
				featureType,
				featureType,
				null);
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
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		return (FieldReader) new SimpleFeatureSerializationProvider.WholeFeatureReader(
				featureType);
	}

	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		return (FieldWriter) new SimpleFeatureSerializationProvider.WholeFeatureWriter();
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new WholeFeatureRowBuilder();
	}

	@Override
	public boolean hasTemporalConstraints() {
		return getTimeDescriptors().hasTime();
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
					fieldVisiblityHandler));
			return defaultHandlers;
		}
		// LOGGER.warn("Simple Feature Type could not be used for handling the
		// indexed data");
		return super.getDefaultTypeMatchingHandlers(featureType);
	}

	private static List<NativeFieldHandler<SimpleFeature, Object>> typeToFieldHandlers(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>();
		nativeHandlers.add(new WholeFeatureHandler(
				type));
		return nativeHandlers;
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
					fieldVisiblityHandler));
		}
		else if (timeDescriptors.getTime() != null) {
			// if we didn't succeed in identifying a start and end time,
			// just grab the first attribute and use it as a timestamp
			return new FeatureTimestampHandler(
					timeDescriptors.getTime(),
					fieldVisiblityHandler);
		}
		return null;
	}

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
	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	@Override
	public synchronized TimeDescriptors getTimeDescriptors() {
		return inferTimeAttributeDescriptor(featureType);
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
	public SimpleFeature decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		final PersistentValue<Object> obj = data.getAdapterExtendedData().getValues().get(
				0);
		final byte[][] bytes = (byte[][]) obj.getValue();
		int i = 0;
		final SimpleFeatureBuilder bldr = getBuilder();
		for (final byte[] f : bytes) {
			if (f != null) {
				final FieldReader reader = FieldUtils.getDefaultReaderForClass(featureType.getType(
						i).getBinding());

				bldr.set(
						i,
						reader.readField(f));
			}
			i++;
		}
		return bldr.buildFeature(data.getDataId().getString());
	}

	private synchronized SimpleFeatureBuilder getBuilder() {
		if (b == null) {
			b = new SimpleFeatureBuilder(
					featureType);
		}
		return b;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		extendedData.addValue(new PersistentValue<Object>(
				new ByteArrayId(
						""),
				entry.getAttributes().toArray(
						new Object[] {})));
		final AdapterPersistenceEncoding encoding = super.encode(
				entry,
				indexModel);
		return new WholeFeatureAdapterEncoding(
				getAdapterId(),
				getDataId(entry),
				encoding.getCommonData(),
				extendedData);
	}

	@Override
	protected byte[] defaultTypeDataToBinary() {
		final String encodedType = DataUtilities.encodeType(featureType);
		final String typeName = featureType.getTypeName();
		final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);

		byte[] attrBytes = new byte[0];

		final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
		userDataConfiguration.addConfigurations(
				typeName,
				new TimeDescriptorConfiguration());
		userDataConfiguration.addConfigurations(
				typeName,
				new SimpleFeatureStatsConfigurationCollection());
		userDataConfiguration.addConfigurations(
				typeName,
				new SimpleFeaturePrimaryIndexConfiguration());
		userDataConfiguration.configureFromType(featureType);

		try {
			attrBytes = StringUtils.stringToBinary(userDataConfiguration.asJsonString());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Failure to encode simple feature user data configuration",
					e);
		}

		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + typeNameBytes.length
				+ adapterId.getBytes().length + attrBytes.length + 24);

		buf.putInt(0); // a signal for the new version
		buf.putInt(typeNameBytes.length);
		buf.putInt(0); // old visibility (backward compatibility)
		buf.putInt(attrBytes.length);
		buf.putInt(encodedTypeBytes.length);
		buf.putInt(adapterId.getBytes().length);
		buf.put(typeNameBytes);
		buf.put(attrBytes);
		buf.put(encodedTypeBytes);
		buf.put(adapterId.getBytes());
		return buf.array();
	}

	@Override
	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int initialBytes = buf.getInt();
		// temporary hack for backward compatibility
		final boolean skipConfig = (initialBytes > 0);
		final byte[] typeNameBytes = skipConfig ? new byte[initialBytes] : new byte[buf.getInt()];
		final byte[] visibilityManagementClassNameBytes = new byte[buf.getInt()];
		final byte[] attrBytes = skipConfig ? new byte[0] : new byte[buf.getInt()];
		final byte[] encodedTypeBytes = new byte[buf.getInt()];
		final byte[] adapterIdBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(visibilityManagementClassNameBytes); // ignore...old release
		buf.get(attrBytes);
		buf.get(encodedTypeBytes);
		buf.get(adapterIdBytes);
		adapterId = new ByteArrayId(
				adapterIdBytes);

		final String typeName = StringUtils.stringFromBinary(typeNameBytes);

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

		final SimpleFeatureUserDataConfigurationSet userDataConfiguration = new SimpleFeatureUserDataConfigurationSet();
		userDataConfiguration.addConfigurations(
				typeName,
				new TimeDescriptorConfiguration(
						featureType));
		userDataConfiguration.addConfigurations(
				typeName,
				new SimpleFeatureStatsConfigurationCollection(
						featureType));
		userDataConfiguration.addConfigurations(
				typeName,
				new SimpleFeaturePrimaryIndexConfiguration(
						featureType));
		try {
			userDataConfiguration.fromJsonString(
					StringUtils.stringFromBinary(attrBytes),
					featureType);

		}
		catch (final IOException e) {
			LOGGER.error(
					"Failure to decode simple feature user data configuration",
					e);
		}

		return null;
	}

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {
		return model.getDimensions().length + 1;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		return new ByteArrayId(
				"");
	}
}
