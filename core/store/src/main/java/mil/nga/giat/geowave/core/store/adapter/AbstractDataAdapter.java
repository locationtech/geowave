package mil.nga.giat.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class generically supports most of the operations necessary to implement
 * a Data Adapter and can be easily extended to support specific data types.<br>
 * Many of the details are handled by mapping IndexFieldHandler's based on
 * either types or exact dimensions. These handler mappings can be supplied in
 * the constructor. The dimension matching handlers are used first when trying
 * to decode a persistence encoded value. This can be done specifically to match
 * a field (for example if there are multiple ways of encoding/decoding the same
 * type). Otherwise the type matching handlers will simply match any field with
 * the same type as its generic field type.
 * 
 * @param <T>
 *            The type for the entries handled by this adapter
 */
abstract public class AbstractDataAdapter<T> implements
		WritableDataAdapter<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AbstractDataAdapter.class);
	protected Map<Class<?>, IndexFieldHandler<T, ? extends CommonIndexValue, Object>> typeMatchingFieldHandlers;
	protected Map<ByteArrayId, IndexFieldHandler<T, ? extends CommonIndexValue, Object>> dimensionMatchingFieldHandlers;
	protected List<NativeFieldHandler<T, Object>> nativeFieldHandlers;
	protected FieldVisibilityHandler<T, Object> fieldVisiblityHandler;

	protected AbstractDataAdapter() {}

	public AbstractDataAdapter(
			final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			final List<NativeFieldHandler<T, Object>> nativeFieldHandlers,
			final FieldVisibilityHandler<T, Object> fieldVisiblityHandler ) {
		this(
				indexFieldHandlers,
				nativeFieldHandlers,
				fieldVisiblityHandler,
				null);
	}

	public AbstractDataAdapter(
			final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			final List<NativeFieldHandler<T, Object>> nativeFieldHandlers ) {
		this(
				indexFieldHandlers,
				nativeFieldHandlers,
				null,
				null);
	}

	protected AbstractDataAdapter(
			final List<PersistentIndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			final List<NativeFieldHandler<T, Object>> nativeFieldHandlers,
			final FieldVisibilityHandler<T, Object> fieldVisiblityHandler,
			final Object defaultTypeData ) {
		this.nativeFieldHandlers = nativeFieldHandlers;
		this.fieldVisiblityHandler = fieldVisiblityHandler;
		init(
				indexFieldHandlers,
				defaultTypeData);
	}

	protected void init(
			final List<? extends IndexFieldHandler<T, ? extends CommonIndexValue, Object>> indexFieldHandlers,
			final Object defaultIndexHandlerData ) {
		dimensionMatchingFieldHandlers = new HashMap<ByteArrayId, IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();
		typeMatchingFieldHandlers = new HashMap<Class<?>, IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();

		// --------------------------------------------------------------------
		// split out the dimension-matching index handlers from the
		// type-matching index handlers

		for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : indexFieldHandlers) {
			if (indexHandler instanceof DimensionMatchingIndexFieldHandler) {
				final ByteArrayId[] matchedDimensionFieldIds = ((DimensionMatchingIndexFieldHandler<T, ? extends CommonIndexValue, Object>) indexHandler)
						.getSupportedIndexFieldIds();

				for (final ByteArrayId matchedDimensionId : matchedDimensionFieldIds) {
					dimensionMatchingFieldHandlers.put(
							matchedDimensionId,
							indexHandler);
				}

			}
			else {
				// alternatively we could put make the dimension-matching field
				// handlers match types as a last resort rather than this else,
				// but they shouldn't conflict with an existing type matching
				// class

				typeMatchingFieldHandlers.put(
						GenericTypeResolver.resolveTypeArguments(
								indexHandler.getClass(),
								IndexFieldHandler.class)[1],
						indexHandler);
			}
		}

		// --------------------------------------------------------------------
		// add default handlers if the type is not already within the custom
		// handlers

		final List<IndexFieldHandler<T, ? extends CommonIndexValue, Object>> defaultTypeMatchingHandlers = getDefaultTypeMatchingHandlers(defaultIndexHandlerData);

		for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> defaultFieldHandler : defaultTypeMatchingHandlers) {
			final Class<?> defaultFieldHandlerClass = GenericTypeResolver.resolveTypeArguments(
					defaultFieldHandler.getClass(),
					IndexFieldHandler.class)[1];

			// if the type matching handlers can already handle this class,
			// don't overload it, otherwise, use this as a default handler

			final IndexFieldHandler<T, ? extends CommonIndexValue, Object> existingTypeHandler = FieldUtils
					.getAssignableValueFromClassMap(
							defaultFieldHandlerClass,
							typeMatchingFieldHandlers);
			if (existingTypeHandler == null) {
				typeMatchingFieldHandlers.put(
						defaultFieldHandlerClass,
						defaultFieldHandler);
			}
		}
	}

	/**
	 * Returns an empty list of IndexFieldHandlers as default.
	 * 
	 * @param defaultIndexHandlerData
	 *            - object parameter
	 * @return Empty list of IndexFieldHandlers.
	 */
	protected List<IndexFieldHandler<T, ? extends CommonIndexValue, Object>> getDefaultTypeMatchingHandlers(
			final Object defaultIndexHandlerData ) {
		return new ArrayList<IndexFieldHandler<T, ? extends CommonIndexValue, Object>>();
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final T entry,
			final CommonIndexModel indexModel ) {
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final Set<ByteArrayId> nativeFieldsInIndex = new HashSet<ByteArrayId>();

		for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {

			final IndexFieldHandler<T, ? extends CommonIndexValue, Object> fieldHandler = getFieldHandler(dimension);

			if (fieldHandler == null) {
				if (LOGGER.isInfoEnabled()) {
					// Don't waste time converting IDs to String if "info" level
					// is not enabled
					LOGGER.info("Unable to find field handler for data adapter '"
							+ StringUtils.stringFromBinary(getAdapterId().getBytes()) + "' and indexed field '"
							+ StringUtils.stringFromBinary(dimension.getFieldId().getBytes()));
				}
				continue;
			}

			final CommonIndexValue value = fieldHandler.toIndexValue(entry);
			indexData.addValue(new PersistentValue<CommonIndexValue>(
					dimension.getFieldId(),
					value));
			nativeFieldsInIndex.addAll(Arrays.asList(fieldHandler.getNativeFieldIds()));
		}

		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();

		// now for the other data
		if (nativeFieldHandlers != null) {
			for (final NativeFieldHandler<T, Object> fieldHandler : nativeFieldHandlers) {
				final ByteArrayId fieldId = fieldHandler.getFieldId();
				if (nativeFieldsInIndex.contains(fieldId)) {
					continue;
				}
				extendedData.addValue(new PersistentValue<Object>(
						fieldId,
						fieldHandler.getFieldValue(entry)));
			}
		}

		return new AdapterPersistenceEncoding(
				getAdapterId(),
				getDataId(entry),
				indexData,
				extendedData);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		final RowBuilder<T, Object> builder = newBuilder();
		if (index != null) {
			final CommonIndexModel indexModel = index.getIndexModel();
			for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {
				final IndexFieldHandler<T, CommonIndexValue, Object> fieldHandler = (IndexFieldHandler<T, CommonIndexValue, Object>) getFieldHandler(dimension);
				if (fieldHandler == null) {
					if (LOGGER.isInfoEnabled()) {
						// dont waste time converting IDs to String if info is
						// not
						// enabled
						LOGGER.info("Unable to find field handler for data adapter '"
								+ StringUtils.stringFromBinary(getAdapterId().getBytes()) + "' and indexed field '"
								+ StringUtils.stringFromBinary(dimension.getFieldId().getBytes()));
					}
					continue;
				}
				final CommonIndexValue value = data.getCommonData().getValue(
						dimension.getFieldId());
				if (value == null) {
					continue;
				}
				final PersistentValue<Object>[] values = fieldHandler.toNativeValues(value);
				if ((values != null) && (values.length > 0)) {
					for (final PersistentValue<Object> v : values) {
						builder.setField(v);
					}
				}
			}
		}
		for (final PersistentValue<Object> fieldValue : data.getAdapterExtendedData().getValues()) {
			builder.setField(fieldValue);
		}
		return builder.buildRow(data.getDataId());
	}

	abstract protected RowBuilder<T, Object> newBuilder();

	/**
	 * Get index field handler for the provided dimension.
	 * 
	 * @param dimension
	 * @return field handler
	 */
	private IndexFieldHandler<T, ? extends CommonIndexValue, Object> getFieldHandler(
			final NumericDimensionField<? extends CommonIndexValue> dimension ) {
		// first try explicit dimension matching
		IndexFieldHandler<T, ? extends CommonIndexValue, Object> fieldHandler = dimensionMatchingFieldHandlers
				.get(dimension.getFieldId());
		if (fieldHandler == null) {
			// if that fails, go for type matching
			fieldHandler = FieldUtils.getAssignableValueFromClassMap(
					GenericTypeResolver.resolveTypeArgument(
							dimension.getClass(),
							NumericDimensionField.class),
					typeMatchingFieldHandlers);
			dimensionMatchingFieldHandlers.put(
					dimension.getFieldId(),
					fieldHandler);
		}
		return fieldHandler;
	}

	@Override
	public byte[] toBinary() {
		// run through the list of field handlers and persist whatever is
		// persistable, if the field handler is not persistable the assumption
		// is that the data adapter can re-create it by some other means

		// use a linked hashset to maintain order and ensure no duplication
		final Set<Persistable> persistables = new LinkedHashSet<Persistable>();
		for (final NativeFieldHandler<T, Object> nativeHandler : nativeFieldHandlers) {
			if (nativeHandler instanceof Persistable) {
				persistables.add((Persistable) nativeHandler);
			}
		}
		for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : typeMatchingFieldHandlers
				.values()) {
			if (indexHandler instanceof Persistable) {
				persistables.add((Persistable) indexHandler);
			}
		}
		for (final IndexFieldHandler<T, ? extends CommonIndexValue, Object> indexHandler : dimensionMatchingFieldHandlers
				.values()) {
			if (indexHandler instanceof Persistable) {
				persistables.add((Persistable) indexHandler);
			}
		}
		if (fieldVisiblityHandler instanceof Persistable) {
			persistables.add((Persistable) fieldVisiblityHandler);
		}

		final byte[] defaultTypeDataBinary = defaultTypeDataToBinary();
		final byte[] persistablesBytes = PersistenceUtils.toBinary(persistables);
		final ByteBuffer buf = ByteBuffer.allocate(defaultTypeDataBinary.length + persistablesBytes.length + 4);
		buf.putInt(defaultTypeDataBinary.length);
		buf.put(defaultTypeDataBinary);
		buf.put(persistablesBytes);
		return buf.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes == null) || (bytes.length < 4)) {
			LOGGER.warn("Unable to deserialize data adapter.  Binary is incomplete.");
			return;
		}
		final List<IndexFieldHandler<T, CommonIndexValue, Object>> indexFieldHandlers = new ArrayList<IndexFieldHandler<T, CommonIndexValue, Object>>();
		final List<NativeFieldHandler<T, Object>> nativeFieldHandlers = new ArrayList<NativeFieldHandler<T, Object>>();
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] defaultTypeDataBinary = new byte[buf.getInt()];
		Object defaultTypeData = null;
		if (defaultTypeDataBinary.length > 0) {
			buf.get(defaultTypeDataBinary);
			defaultTypeData = defaultTypeDataFromBinary(defaultTypeDataBinary);
		}

		final byte[] persistablesBytes = new byte[bytes.length - defaultTypeDataBinary.length - 4];
		if (persistablesBytes.length > 0) {
			buf.get(persistablesBytes);
			final List<Persistable> persistables = PersistenceUtils.fromBinary(persistablesBytes);
			for (final Persistable persistable : persistables) {
				if (persistable instanceof IndexFieldHandler) {
					indexFieldHandlers.add((IndexFieldHandler<T, CommonIndexValue, Object>) persistable);
				}
				// in case persistable is polymorphic and multi-purpose, check
				// both
				// handler types and visibility handler
				if (persistable instanceof NativeFieldHandler) {
					nativeFieldHandlers.add((NativeFieldHandler<T, Object>) persistable);
				}
				if (persistable instanceof FieldVisibilityHandler) {
					fieldVisiblityHandler = (FieldVisibilityHandler<T, Object>) persistable;
				}
			}
		}

		this.nativeFieldHandlers = nativeFieldHandlers;
		init(
				indexFieldHandlers,
				defaultTypeData);
	}

	public FieldVisibilityHandler<T, Object> getFieldVisiblityHandler() {
		return fieldVisiblityHandler;
	}

	protected byte[] defaultTypeDataToBinary() {
		return new byte[] {};
	}

	protected Object defaultTypeDataFromBinary(
			final byte[] bytes ) {
		return null;
	}
}
