package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.encoding.HBaseCommonIndexedPersistenceEncoding;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

/**
 * This class wraps our Distributable filters in an HBase filter so that a
 * coprocessor can use them.
 * 
 * @author kent
 *
 */
public class HBaseDistributableFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(HBaseDistributableFilter.class);

	private List<DistributableQueryFilter> filterList;
	protected CommonIndexModel model;
	private final List<ByteArrayId> commonIndexFieldIds = new ArrayList<>();
	private PersistentDataset<Object> adapterExtendedValues;

	// CACHED decoded data:
	private PersistentDataset<CommonIndexValue> commonData;
	private FlattenedUnreadData unreadData;
	private CommonIndexedPersistenceEncoding persistenceEncoding;
	private IndexedAdapterPersistenceEncoding adapterEncoding;

	public HBaseDistributableFilter() {
		filterList = new ArrayList<DistributableQueryFilter>();
	}

	public static HBaseDistributableFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

		final int modelLength = buf.getInt();
		final byte[] modelBytes = new byte[modelLength];
		buf.get(modelBytes);

		final byte[] filterBytes = new byte[pbBytes.length - modelLength - 4];
		buf.get(filterBytes);

		HBaseDistributableFilter newInstance = new HBaseDistributableFilter();
		newInstance.init(
				filterBytes,
				modelBytes);

		return newInstance;
	}

	public byte[] toByteArray()
			throws IOException {
		final byte[] modelBinary = PersistenceUtils.toBinary(model);
		final byte[] filterListBinary = PersistenceUtils.toBinary(filterList);

		final ByteBuffer buf = ByteBuffer.allocate(filterListBinary.length + modelBinary.length + 4);

		buf.putInt(modelBinary.length);
		buf.put(modelBinary);
		buf.put(filterListBinary);

		return buf.array();
	}

	public boolean init(
			final byte[] filterBytes,
			final byte[] modelBytes ) {
		List<Persistable> decodedFilterList = PersistenceUtils.fromBinary(filterBytes);

		if (decodedFilterList == null) {
			System.out.println("Failed to decode filter list");
			return false;
		}

		filterList.clear();

		for (Persistable decodedFilter : decodedFilterList) {
			if (decodedFilter instanceof DistributableQueryFilter) {
				filterList.add((DistributableQueryFilter) decodedFilter);
			}
			else {
				System.out.println("Unrecognized type for decoded filter!" + decodedFilter.getClass().getName());
			}
		}

		model = PersistenceUtils.fromBinary(
				modelBytes,
				CommonIndexModel.class);

		if (model == null) {
			System.out.println("Failed to decode index model");
			return false;
		}

		commonIndexFieldIds.clear();
		for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
			commonIndexFieldIds.add(numericDimension.getFieldId());
		}

		return true;
	}

	public boolean init(
			List<DistributableQueryFilter> filterList,
			CommonIndexModel model ) {
		this.filterList.clear();
		this.filterList.addAll(filterList);

		this.model = model;
		for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
			commonIndexFieldIds.add(numericDimension.getFieldId());
		}

		commonIndexFieldIds.clear();
		for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
			commonIndexFieldIds.add(numericDimension.getFieldId());
		}

		return true;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell cell )
			throws IOException {
		commonData = new PersistentDataset<CommonIndexValue>();

		unreadData = aggregateFieldData(
				cell,
				commonData);

		return applyRowFilter(
				cell,
				commonData,
				unreadData);
	}

	protected ReturnCode applyRowFilter(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		ReturnCode returnCode = ReturnCode.SKIP;
		persistenceEncoding = null;

		try {
			persistenceEncoding = getPersistenceEncoding(
					cell,
					commonData,
					unreadData);

			if (applyRowFilter(persistenceEncoding)) {
				returnCode = ReturnCode.INCLUDE;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return returnCode;
	}

	protected static CommonIndexedPersistenceEncoding getPersistenceEncoding(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		final GeowaveRowId rowId = new GeowaveRowId(
				CellUtil.cloneRow(cell));

		return new HBaseCommonIndexedPersistenceEncoding(
				new ByteArrayId(
						rowId.getAdapterId()),
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				commonData,
				unreadData);
	}

	public IndexedAdapterPersistenceEncoding getAdapterEncoding(
			DataAdapter dataAdapter ) {
		final PersistentDataset<Object> adapterExtendedValues = new PersistentDataset<Object>();
		if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
			((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
					dataAdapter,
					model);
			final PersistentDataset<Object> existingExtValues = ((AbstractAdapterPersistenceEncoding) persistenceEncoding)
					.getAdapterExtendedData();
			if (existingExtValues != null) {
				for (final PersistentValue<Object> val : existingExtValues.getValues()) {
					adapterExtendedValues.addValue(val);
				}
			}
		}

		adapterEncoding = new IndexedAdapterPersistenceEncoding(
				persistenceEncoding.getAdapterId(),
				persistenceEncoding.getDataId(),
				persistenceEncoding.getIndexInsertionId(),
				persistenceEncoding.getDuplicateCount(),
				persistenceEncoding.getCommonData(),
				new PersistentDataset<byte[]>(),
				adapterExtendedValues);

		return adapterEncoding;
	}

	// Called by the aggregation endpoint, after filtering the current row
	public Object decodeRow(
			DataAdapter dataAdapter ) {
		return dataAdapter.decode(
				getAdapterEncoding(dataAdapter),
				new PrimaryIndex(
						null,
						model));
	}

	protected boolean applyRowFilter(
			final CommonIndexedPersistenceEncoding encoding ) {
		if (filterList == null) {
			System.err.println("FILTER IS NULL");
			return false;
		}

		if (model == null) {
			System.err.println("MODEL IS NULL");
			return false;
		}

		if (encoding == null) {
			System.err.println("ENCODING IS NULL");
			return false;
		}

		for (DistributableQueryFilter filter : filterList) {
			if (!filter.accept(
					model,
					encoding)) {
				return false;
			}
		}

		return true;
	}

	protected FlattenedUnreadData aggregateFieldData(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData ) {
		final ByteArrayId colQual = new ByteArrayId(
				CellUtil.cloneQualifier(cell));

		final byte[] valueBytes = CellUtil.cloneValue(cell);

		final FlattenedDataSet dataSet = DataStoreUtils.decomposeFlattenedFields(
				colQual.getBytes(),
				valueBytes,
				null,
				-1);

		final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final int ordinal = fieldInfo.getFieldPosition();

			if (ordinal < model.getDimensions().length) {
				final ByteArrayId commonIndexFieldId = commonIndexFieldIds.get(ordinal);
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(commonIndexFieldId);
				if (reader != null) {
					final CommonIndexValue fieldValue = reader.readField(fieldInfo.getValue());
					// TODO: handle visibility
					// fieldValue.setVisibility(key.getColumnVisibility().getBytes());
					commonData.addValue(new PersistentValue<CommonIndexValue>(
							commonIndexFieldId,
							fieldValue));
				}
				else {
					LOGGER.error("Could not find reader for common index field: " + commonIndexFieldId.getString());
				}
			}
		}

		return dataSet.getFieldsDeferred();
	}
}
