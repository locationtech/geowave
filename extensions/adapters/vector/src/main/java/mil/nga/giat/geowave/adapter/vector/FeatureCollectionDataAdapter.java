package mil.nga.giat.geowave.adapter.vector;

import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.merge.FeatureCollectionCombiner;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.util.FitToIndexDefaultFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.util.SimpleFeatureWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.SpatialArrayField;
import mil.nga.giat.geowave.core.geotime.store.dimension.SpatialField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeArrayField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.SingleTierSubStrategy;
import mil.nga.giat.geowave.core.store.TimeUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.FitToIndexPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.ModelConvertingDataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.query.ArrayToElementsIterator;
import mil.nga.giat.geowave.datastore.accumulo.query.ElementsToArrayIterator;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
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
 * retrieving GeoTools FeatureCollection objects to and from a GeoWave
 * persistent store in Accumulo. Note that if the implementor needs to write
 * rows with particular visibility, that must be done by providing a
 * FieldVisibilityHandler to this constructor. The adapter will use the
 * SimpleFeature's default geometry for spatial indexing and will use either the
 * first temporal attribute (a Calendar or Date object) as the timestamp of a
 * temporal index or if there are multiple temporal attributes and one contains
 * the term 'start' and the other contains either the term 'stop' or 'end' it
 * will interpret the combination of these attributes as a time range to index
 * on.
 * 
 */
public class FeatureCollectionDataAdapter extends
		AbstractDataAdapter<DefaultFeatureCollection> implements
		IndexDependentDataAdapter<DefaultFeatureCollection>,
		ModelConvertingDataAdapter<DefaultFeatureCollection>
{
	public static final int FEATURE_COLLECTION_COMBINER_PRIORITY = 4;
	public static final int ARRAY_TO_ELEMENTS_PRIORITY = 5;
	public static final int ELEMENTS_TO_ARRAY_PRIORITY = 15;
	public static final int DEFAULT_FEATURES_PER_ENTRY = 5000;
	private static final String FEATURE_COLLECTION_DATA_ID_KEY = "GeoWave_FeatureCollectionDataAdapter_DataId_Key";

	private final static Logger LOGGER = Logger.getLogger(FeatureCollectionDataAdapter.class);
	// the original coordinate system will always be represented internally by
	// the persisted type
	private SimpleFeatureType persistedType;

	// externally the reprojected type will always be advertised because all
	// features will be reprojected to EPSG:4326 and the advertised feature type
	// from the data adapter should match in CRS
	private SimpleFeatureType reprojectedType;
	private MathTransform transform;

	private FeatureDataAdapter featureDataAdapter;
	protected Map<Class<?>, IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> arrayTypeMatchingFieldHandlers;

	private int featuresPerEntry;

	protected FeatureCollectionDataAdapter() {}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type ) {
		this(
				type,
				DEFAULT_FEATURES_PER_ENTRY,
				new ArrayList<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>());
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final int featuresPerEntry ) {
		this(
				type,
				featuresPerEntry,
				new ArrayList<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>());
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this(
				type,
				DEFAULT_FEATURES_PER_ENTRY,
				customIndexHandlers,
				null);
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final int featuresPerEntry,
			final List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this(
				type,
				featuresPerEntry,
				customIndexHandlers,
				null);
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler ) {
		this(
				type,
				DEFAULT_FEATURES_PER_ENTRY,
				new ArrayList<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>(),
				fieldVisiblityHandler);
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final int featuresPerEntry,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler ) {
		this(
				type,
				featuresPerEntry,
				new ArrayList<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>(),
				fieldVisiblityHandler);
	}

	public FeatureCollectionDataAdapter(
			final SimpleFeatureType type,
			final int featuresPerEntry,
			final List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers,
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler ) {
		super(
				customIndexHandlers,
				new ArrayList<NativeFieldHandler<DefaultFeatureCollection, Object>>(),
				type);
		setFeatureType(type);
		this.featuresPerEntry = featuresPerEntry;
		this.fieldVisiblityHandler = fieldVisiblityHandler;
		featureDataAdapter = new FeatureDataAdapter(
				type);
	}

	private void setFeatureType(
			final SimpleFeatureType type ) {
		persistedType = type;
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
	}

	@Override
	protected void init(
			final List<? extends IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			final Object defaultIndexHandlerData ) {
		super.init(
				indexFieldHandlers,
				defaultIndexHandlerData);
		arrayTypeMatchingFieldHandlers = new HashMap<Class<?>, IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>();
		// add default handlers if the type is not already within the custom
		// handlers
		final List<IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> defaultArrayTypeMatchingHandlers = getDefaultArrayTypeMatchingHandlers(defaultIndexHandlerData);
		for (final IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object> defaultFieldHandler : defaultArrayTypeMatchingHandlers) {
			final Class<?> defaultFieldHandlerClass = (Class<?>) ((ParameterizedType) ((ParameterizedType) defaultFieldHandler.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[1]).getActualTypeArguments()[0];
			arrayTypeMatchingFieldHandlers.put(
					defaultFieldHandlerClass,
					defaultFieldHandler);
		}
	}

	private static List<NativeFieldHandler<DefaultFeatureCollection, Object>> typeToFieldHandlers(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<DefaultFeatureCollection, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<DefaultFeatureCollection, Object>>(
				type.getAttributeCount());
		for (final AttributeDescriptor attrDesc : type.getAttributeDescriptors()) {
			nativeHandlers.add(new FeatureCollectionAttributeHandler(
					attrDesc));
		}
		return nativeHandlers;
	}

	protected List<IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> getDefaultArrayTypeMatchingHandlers(
			final Object typeObj ) {
		if ((typeObj != null) && (typeObj instanceof SimpleFeatureType)) {
			nativeFieldHandlers = typeToFieldHandlers((SimpleFeatureType) typeObj);
			final List<IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> defaultHandlers = new ArrayList<IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>();
			final SimpleFeatureType internalType = (SimpleFeatureType) typeObj;
			final List<AttributeDescriptor> timeAttributes = new ArrayList<AttributeDescriptor>();
			for (final AttributeDescriptor attrDesc : internalType.getAttributeDescriptors()) {
				final Class<?> bindingClass = attrDesc.getType().getBinding();
				if (TimeUtils.isTemporal(bindingClass)) {
					timeAttributes.add(attrDesc);
				}
			}
			if (timeAttributes.size() > 1) {
				// try to find 2 attributes to account for start and end times
				AttributeDescriptor startDesc = null;
				AttributeDescriptor endDesc = null;
				// TODO is this a reasonable way to automatically try to
				// identify start and end times for a range?
				// if not, can we think of improvements?
				for (final AttributeDescriptor timeAttr : timeAttributes) {
					final String lowerCaseName = timeAttr.getLocalName().toLowerCase();
					if (lowerCaseName.contains("end")) {
						endDesc = timeAttr;
					}
					else if (lowerCaseName.contains("stop")) {
						endDesc = timeAttr;
					}
					else if (lowerCaseName.contains("start")) {
						startDesc = timeAttr;
					}
				}
				if ((startDesc == null) || (endDesc == null)) {
					// if we didn't succeed in identifying a start and end time,
					// just grab the first attribute and use it as a timestamp
					defaultHandlers.add(new FeatureCollectionTimestampHandler(
							timeAttributes.get(0),
							fieldVisiblityHandler));
				}
				else {
					defaultHandlers.add(new FeatureCollectionTimeRangeHandler(
							new FeatureCollectionAttributeHandler(
									startDesc),
							new FeatureCollectionAttributeHandler(
									endDesc),
							fieldVisiblityHandler));
				}
			}
			else if (timeAttributes.size() == 1) {
				defaultHandlers.add(new FeatureCollectionTimestampHandler(
						timeAttributes.get(0),
						fieldVisiblityHandler));
			}
			defaultHandlers.add(new FeatureCollectionGeometryHandler(
					internalType.getGeometryDescriptor(),
					fieldVisiblityHandler));
			return defaultHandlers;
		}
		LOGGER.warn("Simple Feature Type could not be used for handling the indexed data");
		return super.getDefaultTypeMatchingHandlers(reprojectedType);
	}

	@Override
	protected IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object> getFieldHandler(
			final DimensionField<? extends CommonIndexValue> dimension ) {
		IndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object> fieldHandler = super.getFieldHandler(dimension);
		if (fieldHandler == null) {
			// if that fails, go for array type matching
			final Class<?> type = (Class<?>) ((ParameterizedType) dimension.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
			fieldHandler = FieldUtils.getAssignableValueFromClassMap(
					type,
					arrayTypeMatchingFieldHandlers);
		}
		return fieldHandler;
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
		Class<?> bindingClass = null;
		if (!fieldId.equals(new ByteArrayId(
				FEATURE_COLLECTION_DATA_ID_KEY))) {
			final AttributeDescriptor descriptor = reprojectedType.getDescriptor(StringUtils.stringFromBinary(fieldId.getBytes()));
			try {
				bindingClass = Class.forName("[L" + descriptor.getType().getBinding().getName() + ";");
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Could not create array binding for Simple Feature Type",
						e);
			}
		}
		else {
			bindingClass = String[].class;
		}
		return (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(bindingClass);
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<DefaultFeatureCollection, Object> getWriter(
			final ByteArrayId fieldId ) {
		Class<?> bindingClass = null;
		if (!fieldId.equals(new ByteArrayId(
				FEATURE_COLLECTION_DATA_ID_KEY))) {
			final AttributeDescriptor descriptor = reprojectedType.getDescriptor(StringUtils.stringFromBinary(fieldId.getBytes()));
			try {
				bindingClass = Class.forName("[L" + descriptor.getType().getBinding().getName() + ";");
			}
			catch (final ClassNotFoundException e) {
				LOGGER.error(
						"Could not create array binding for Simple Feature Type",
						e);
			}
		}
		else {
			bindingClass = String[].class;
		}
		FieldWriter<DefaultFeatureCollection, Object> retVal;
		if (fieldVisiblityHandler != null) {
			retVal = (FieldWriter<DefaultFeatureCollection, Object>) FieldUtils.getDefaultWriterForClass(
					bindingClass,
					fieldVisiblityHandler);
		}
		else {
			retVal = (FieldWriter<DefaultFeatureCollection, Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
		}
		return retVal;
	}

	@Override
	protected byte[] defaultTypeDataToBinary() {
		// serialize the feature type
		final String encodedType = DataUtilities.encodeType(persistedType);
		final String typeName = persistedType.getTypeName();
		final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
		final String namespace = persistedType.getName().getNamespaceURI();
		byte[] namespaceBytes;
		if (namespace != null) {
			namespaceBytes = StringUtils.stringToBinary(namespace);
		}
		else {
			namespaceBytes = new byte[0];
		}
		final byte[] encodedTypeBytes = StringUtils.stringToBinary(encodedType);
		final ByteBuffer buf = ByteBuffer.allocate(encodedTypeBytes.length + typeNameBytes.length + namespaceBytes.length + 8);
		buf.putInt(typeNameBytes.length);
		buf.putInt(namespaceBytes.length);
		buf.put(typeNameBytes);
		buf.put(namespaceBytes);
		buf.put(encodedTypeBytes);

		return buf.array();
	}

	@Override
	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		// deserialize the feature type
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] typeNameBytes = new byte[buf.getInt()];
		final byte[] namespaceBytes = new byte[buf.getInt()];
		buf.get(typeNameBytes);
		buf.get(namespaceBytes);
		final String typeName = StringUtils.stringFromBinary(typeNameBytes);
		final String namespace = StringUtils.stringFromBinary(namespaceBytes);
		final byte[] encodedTypeBytes = new byte[bytes.length - typeNameBytes.length - namespaceBytes.length - 8];
		buf.get(encodedTypeBytes);

		final String encodedType = StringUtils.stringFromBinary(encodedTypeBytes);
		try {
			setFeatureType(DataUtilities.createType(
					namespace,
					typeName,
					encodedType));
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
				StringUtils.stringToBinary(featuresPerEntry + reprojectedType.getTypeName()));
	}

	@Override
	public boolean isSupported(
			final DefaultFeatureCollection entry ) {
		return reprojectedType.getName().getURI().equals(
				entry.getSchema().getName().getURI());
	}

	@Override
	public ByteArrayId getDataId(
			final DefaultFeatureCollection entry ) {
		return new ByteArrayId(
				new byte[] {});
	}

	@Override
	protected RowBuilder<DefaultFeatureCollection, Object> newBuilder() {
		return new FeatureCollectionRowBuilder(
				reprojectedType);
	}

	public SimpleFeatureType getType() {
		return reprojectedType;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final DefaultFeatureCollection entry,
			final CommonIndexModel indexModel ) {

		final AdapterPersistenceEncoding encoding = super.encode(
				entry,
				indexModel);

		final ArrayList<String> dataIds = new ArrayList<String>();
		final Iterator<SimpleFeature> itr = entry.iterator();
		while (itr.hasNext()) {
			dataIds.add(itr.next().getID());
		}

		encoding.getAdapterExtendedData().addValue(
				new PersistentValue<Object>(
						new ByteArrayId(
								FEATURE_COLLECTION_DATA_ID_KEY),
						dataIds.toArray(new String[dataIds.size()])));

		if (entry instanceof FitToIndexDefaultFeatureCollection) {
			return new FitToIndexPersistenceEncoding(
					getAdapterId(),
					encoding.getDataId(),
					encoding.getCommonData(),
					encoding.getAdapterExtendedData(),
					((FitToIndexDefaultFeatureCollection) entry).getIndexId());
		}
		else {
			// this shouldn't happen
			LOGGER.warn("SimpleFeatureCollection is not fit to the index");
			return new AdapterPersistenceEncoding(
					getAdapterId(),
					encoding.getDataId(),
					encoding.getCommonData(),
					encoding.getAdapterExtendedData());
		}
	}

	@Override
	public DefaultFeatureCollection decode(
			final IndexedAdapterPersistenceEncoding data,
			final Index index ) {

		final Index convertedIndex = new Index(
				index.getIndexStrategy(),
				convertModel(index.getIndexModel()));

		final FieldWriter<?, String[]> arrayWriter = FieldUtils.getDefaultWriterForClass(String[].class);

		ByteArrayId dataId = null;
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		for (final PersistentValue<Object> value : data.getAdapterExtendedData().getValues()) {
			if (!value.getId().equals(
					new ByteArrayId(
							FEATURE_COLLECTION_DATA_ID_KEY))) {
				extendedData.addValue(value);
			}
			else {
				dataId = new ByteArrayId(
						arrayWriter.writeField((String[]) value.getValue()));
			}
		}

		return super.decode(
				new IndexedAdapterPersistenceEncoding(
						data.getAdapterId(),
						dataId,
						data.getIndexInsertionId(),
						data.getDuplicateCount(),
						data.getCommonData(),
						extendedData),
				convertedIndex);
	}

	@Override
	public Iterator<DefaultFeatureCollection> convertToIndex(
			final Index index,
			final DefaultFeatureCollection originalEntry ) {

		final DefaultFeatureCollection defaultCRSEntry;

		if (originalEntry instanceof FitToIndexDefaultFeatureCollection) {
			defaultCRSEntry = originalEntry;
		}
		else {
			defaultCRSEntry = new DefaultFeatureCollection(
					originalEntry.getID());

			final SimpleFeatureIterator itr = originalEntry.features();

			while (itr.hasNext()) {
				defaultCRSEntry.add(FeatureDataUtils.defaultCRSTransform(
						itr.next(),
						persistedType,
						reprojectedType,
						transform));
			}

			itr.close();
		}

		final SubStrategy[] subStrategies;
		if (index.getIndexStrategy() instanceof HierarchicalNumericIndexStrategy) {

			final HierarchicalNumericIndexStrategy indexStrategy = (HierarchicalNumericIndexStrategy) index.getIndexStrategy();
			subStrategies = indexStrategy.getSubStrategies();
		}
		else if (index.getIndexStrategy() instanceof SingleTierSubStrategy) {
			final SingleTierSubStrategy indexStrategy = (SingleTierSubStrategy) index.getIndexStrategy();
			subStrategies = new SubStrategy[] {
				new SubStrategy(
						indexStrategy,
						new byte[] {})
			};
		}
		else {
			LOGGER.warn("Could not determine index strategy type.");
			return Collections.<DefaultFeatureCollection> emptyList().iterator();
		}

		// this is a mapping of simple features to index ids
		final Map<ByteArrayId, ArrayList<SimpleFeatureWrapper>> indexedFeaturesMap = new HashMap<ByteArrayId, ArrayList<SimpleFeatureWrapper>>();

		// this is a list of insertions that need to be moved down to the
		// next level in the hierarchy
		final List<SimpleFeatureWrapper> reprocessQueue = new ArrayList<SimpleFeatureWrapper>();

		final Iterator<SimpleFeature> featItr = defaultCRSEntry.iterator();

		// process each feature and continue processing until all features
		// are placed
		while (featItr.hasNext() || (reprocessQueue.size() > 0)) {
			SimpleFeature feature;
			int subStratIdx;
			ByteArrayId prevId;

			// reprocess data before inserting new features
			if (featItr.hasNext() && (reprocessQueue.size() == 0)) {
				// this is a special case used during global optimization
				if (defaultCRSEntry instanceof FitToIndexDefaultFeatureCollection) {
					feature = featItr.next();
					subStratIdx = ((FitToIndexDefaultFeatureCollection) defaultCRSEntry).getSubStratIdx() + 1;
					prevId = ((FitToIndexDefaultFeatureCollection) defaultCRSEntry).getIndexId();
				}
				else {
					feature = featItr.next();
					subStratIdx = 0;
					prevId = null;
				}
			}
			else {
				final SimpleFeatureWrapper featWrapper = reprocessQueue.remove(reprocessQueue.size() - 1);
				feature = featWrapper.getSimpleFeature();
				subStratIdx = featWrapper.getSubStratIdx() + 1;
				prevId = featWrapper.getInsertionId();
			}

			final MultiDimensionalNumericData bounds = featureDataAdapter.encode(
					feature,
					index.getIndexModel()).getNumericData(
					index.getIndexModel().getDimensions());

			final List<ByteArrayId> ids = subStrategies[subStratIdx].getIndexStrategy().getInsertionIds(
					bounds);

			// determine whether the feature can be inserted within this
			// substrategy
			for (final ByteArrayId id : ids) {

				// in the case of reprocessed data, only process the
				// insertion ids that map to the parent id
				if (prevId != null) {

					// bounds of parent insertion id
					final MultiDimensionalNumericData prevBounds = subStrategies[subStratIdx - 1].getIndexStrategy().getRangeForId(
							prevId);

					// bounds of current insertion id
					final MultiDimensionalNumericData subBounds = subStrategies[subStratIdx].getIndexStrategy().getRangeForId(
							id);

					boolean include = true;

					final double[] boundMax = prevBounds.getMaxValuesPerDimension();
					final double[] boundMin = prevBounds.getMinValuesPerDimension();

					final double[] subBoundCentroid = subBounds.getCentroidPerDimension();

					// if the centroid of the sub index is outside the
					// range of the parent index, exclude it from the
					// list
					for (int i = 0; i < prevBounds.getDimensionCount(); i++) {
						if ((subBoundCentroid[i] < boundMin[i]) || (subBoundCentroid[i] >= boundMax[i])) {
							include = false;
							break;
						}
					}

					if (!include) {
						continue;
					}
				}

				// if the insertion id is already present
				if (indexedFeaturesMap.containsKey(id)) {

					final ArrayList<SimpleFeatureWrapper> entry = indexedFeaturesMap.get(id);

					// if the entry is null, that means we have already
					// disabled this insertion id
					if (entry == null) {
						reprocessQueue.add(new SimpleFeatureWrapper(
								feature,
								id,
								subStratIdx));
					}
					// if the entry is full and we are not at the lowest
					// tier, we need to add all the entries to the
					// reprocess queue and mark the insertion id as null
					// (i.e. disabled)
					else if ((entry.size() == featuresPerEntry) && ((subStratIdx + 1) < subStrategies.length)) {
						entry.add(new SimpleFeatureWrapper(
								feature,
								id,
								subStratIdx));

						reprocessQueue.addAll(entry);

						indexedFeaturesMap.put(
								id,
								null);
					}
					// if the entry is non-null, and the size hasn't
					// reached the limit (or we have reached the lowest
					// tier), insert our feature
					else {
						entry.add(new SimpleFeatureWrapper(
								feature,
								id,
								subStratIdx));
					}
				}
				// if this is a new insertion id, create a new list and
				// add it to the indexed features map
				else {
					final ArrayList<SimpleFeatureWrapper> entry = new ArrayList<SimpleFeatureWrapper>();
					entry.add(new SimpleFeatureWrapper(
							feature,
							id,
							subStratIdx));
					indexedFeaturesMap.put(
							id,
							entry);
				}
			}

		}

		final List<DefaultFeatureCollection> featureCollections = new ArrayList<DefaultFeatureCollection>();

		// create the feature collections and return
		// final Set<ByteArrayId> keys = indexedFeaturesMap.keySet();
		// for (final ByteArrayId key : keys) {
		for (final Map.Entry<ByteArrayId, ArrayList<SimpleFeatureWrapper>> kvp : indexedFeaturesMap.entrySet()) {

			final ArrayList<SimpleFeatureWrapper> wrappedFeatures = kvp.getValue();

			if (wrappedFeatures != null) {
				final DefaultFeatureCollection collection = new FitToIndexDefaultFeatureCollection(
						new DefaultFeatureCollection(),
						kvp.getKey());

				for (final SimpleFeatureWrapper wrappedFeature : wrappedFeatures) {
					collection.add(wrappedFeature.getSimpleFeature());
				}

				featureCollections.add(collection);
			}
		}
		return featureCollections.iterator();
	}

	@Override
	public IteratorConfig[] getAttachedIteratorConfig(
			final Index index ) {

		final IteratorSetting combinerSetting = new IteratorSetting(
				FEATURE_COLLECTION_COMBINER_PRIORITY,
				FeatureCollectionCombiner.class);

		final IteratorConfig combinerConfig = new IteratorConfig(
				combinerSetting,
				EnumSet.allOf(IteratorScope.class));

		final List<Column> columns = new ArrayList<Column>();
		columns.add(new Column(
				getAdapterId().getString()));
		Combiner.setColumns(
				combinerConfig.getIteratorSettings(),
				columns);

		final IteratorSetting decompSetting = new IteratorSetting(
				ARRAY_TO_ELEMENTS_PRIORITY,
				ArrayToElementsIterator.class);

		final String modelString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index.getIndexModel()));

		decompSetting.addOption(
				ArrayToElementsIterator.MODEL,
				modelString);

		decompSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));

		final IteratorConfig decompConfig = new IteratorConfig(
				decompSetting,
				EnumSet.of(IteratorScope.scan));

		final IteratorSetting builderSetting = new IteratorSetting(
				ELEMENTS_TO_ARRAY_PRIORITY,
				ElementsToArrayIterator.class);

		builderSetting.addOption(
				ElementsToArrayIterator.MODEL,
				modelString);

		builderSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));

		final IteratorConfig builderConfig = new IteratorConfig(
				builderSetting,
				EnumSet.of(IteratorScope.scan));

		return new IteratorConfig[] {
			combinerConfig,
			decompConfig,
			builderConfig
		};
	}

	@Override
	public CommonIndexModel convertModel(
			final CommonIndexModel indexModel ) {

		final DimensionField<?>[] dimensions = new DimensionField<?>[indexModel.getDimensions().length];

		for (int i = 0; i < indexModel.getDimensions().length; i++) {
			final DimensionField<?> dimension = indexModel.getDimensions()[i];
			if (dimension instanceof TimeField) {
				dimensions[i] = new TimeArrayField(
						(TimeField) dimension);
			}
			else if (dimension instanceof SpatialField) {
				dimensions[i] = new SpatialArrayField(
						(SpatialField) dimension);
			}
		}
		return new BasicIndexModel(
				dimensions);
	}
}
