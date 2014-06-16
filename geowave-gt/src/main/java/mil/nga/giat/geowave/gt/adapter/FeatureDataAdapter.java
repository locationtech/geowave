package mil.nga.giat.geowave.gt.adapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.gt.datastore.GeoWaveGTDataStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.TimeUtils;
import mil.nga.giat.geowave.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.store.data.field.FieldReader;
import mil.nga.giat.geowave.store.data.field.FieldUtils;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.field.FieldWriter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This data adapter will handle all reading/writing concerns for storing and
 * retrieving GeoTools SimpleFeature objects to and from a GeoWave persistent
 * store in Accumulo. Note that if the implementor needs to write rows with
 * particular visibility, that must be done by providing a
 * FieldVisibilityHandler to this constructor. The adapter will use the
 * SimpleFeature's default geometry for spatial indexing and will use either the
 * first temporal attribute (a Calendar or Date object) as the timestamp of a
 * temporal index or if there are multiple temporal attributes and one contains
 * the term 'start' and the other contains either the term 'stop' or 'end' it
 * will interpret the combination of these attributes as a time range to index
 * on.
 * 
 */
public class FeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature>
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

	protected FeatureDataAdapter() {}

	public FeatureDataAdapter(
			final SimpleFeatureType type ) {
		this(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>());
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this(
				type,
				customIndexHandlers,
				null);
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		this(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				fieldVisiblityHandler);
	}

	public FeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		super(
				customIndexHandlers,
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				type);
		setFeatureType(type);
		this.fieldVisiblityHandler = fieldVisiblityHandler;
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

	@Override
	protected List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
			final Object typeObj ) {
		if ((typeObj != null) && (typeObj instanceof SimpleFeatureType)) {
			nativeFieldHandlers = typeToFieldHandlers((SimpleFeatureType) typeObj);
			final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> defaultHandlers = new ArrayList<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>();
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
					defaultHandlers.add(new FeatureTimestampHandler(
							timeAttributes.get(0),
							fieldVisiblityHandler));
				}
				else {
					defaultHandlers.add(new FeatureTimeRangeHandler(
							new FeatureAttributeHandler(
									startDesc),
							new FeatureAttributeHandler(
									endDesc),
							fieldVisiblityHandler));
				}
			}
			else if (timeAttributes.size() == 1) {
				defaultHandlers.add(new FeatureTimestampHandler(
						timeAttributes.get(0),
						fieldVisiblityHandler));
			}
			defaultHandlers.add(new FeatureGeometryHandler(
					internalType.getGeometryDescriptor(),
					fieldVisiblityHandler));
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
		FieldWriter<SimpleFeature, Object> retVal;
		if (fieldVisiblityHandler != null) {
			retVal = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(
					bindingClass,
					fieldVisiblityHandler);
		}
		else {
			retVal = (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(bindingClass);
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
		// if the feature is in a different coordinate reference system than
		// EPSG:4326, transform the geometry
		final CoordinateReferenceSystem crs = entry.getFeatureType().getCoordinateReferenceSystem();
		SimpleFeature defaultCRSEntry = entry;

		if (!GeoWaveGTDataStore.DEFAULT_CRS.equals(crs)) {
			MathTransform featureTransform = null;
			if ((persistedType.getCoordinateReferenceSystem() != null) && persistedType.getCoordinateReferenceSystem().equals(
					crs) && (transform != null)) {
				// we can use the transform we have already calculated for this
				// feature
				featureTransform = transform;
			}
			else if (crs != null) {
				// this feature differs from the persisted type in CRS,
				// calculate the transform
				try {
					featureTransform = CRS.findMathTransform(
							crs,
							GeoWaveGTDataStore.DEFAULT_CRS,
							true);
				}
				catch (final FactoryException e) {
					LOGGER.warn(
							"Unable to find transform to EPSG:4326, the feature geometry will remain in its original CRS",
							e);
				}
			}
			if (featureTransform != null) {
				try {
					// what should we do besides log a message when an entry
					// can't be transformed to EPSG:4326 for some reason?
					// this will clone the feature and retype it to EPSG:4326
					defaultCRSEntry = SimpleFeatureBuilder.retype(
							entry,
							reprojectedType);
					// this will transform the geometry
					defaultCRSEntry.setDefaultGeometry(JTS.transform(
							(Geometry) entry.getDefaultGeometry(),
							featureTransform));
				}
				catch (MismatchedDimensionException | TransformException e) {
					LOGGER.warn(
							"Unable to perform transform to EPSG:4326, the feature geometry will remain in its original CRS",
							e);
				}
			}
		}

		return super.encode(
				defaultCRSEntry,
				indexModel);
	}
}
