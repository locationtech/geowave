/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.DeferredReadCommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.URLClassloaderUtils;

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
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseDistributableFilter.class);

	private boolean wholeRowFilter = false;
	private final List<DistributableQueryFilter> filterList;
	protected CommonIndexModel model;
	private List<ByteArrayId> commonIndexFieldIds = new ArrayList<>();

	// CACHED decoded data:
	private PersistentDataset<CommonIndexValue> commonData;
	private FlattenedUnreadData unreadData;
	private CommonIndexedPersistenceEncoding persistenceEncoding;
	private IndexedAdapterPersistenceEncoding adapterEncoding;
	private int partitionKeyLength;

	public HBaseDistributableFilter() {
		filterList = new ArrayList<DistributableQueryFilter>();
	}

	public static HBaseDistributableFilter parseFrom(
			final byte[] pbBytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(pbBytes);

		boolean wholeRow = buf.get() == (byte) 1 ? true : false;

		final int partitionKeyLength = buf.getInt();

		final int modelLength = buf.getInt();

		final byte[] modelBytes = new byte[modelLength];
		buf.get(modelBytes);

		final byte[] filterBytes = new byte[pbBytes.length - modelLength - 9];
		buf.get(filterBytes);

		final HBaseDistributableFilter newInstance = new HBaseDistributableFilter();
		newInstance.setWholeRowFilter(wholeRow);
		newInstance.setPartitionKeyLength(partitionKeyLength);
		newInstance.init(
				filterBytes,
				modelBytes);

		return newInstance;
	}

	@Override
	public byte[] toByteArray()
			throws IOException {
		final byte[] modelBinary = URLClassloaderUtils.toBinary(model);
		final byte[] filterListBinary = URLClassloaderUtils.toBinary(filterList);

		final ByteBuffer buf = ByteBuffer.allocate(modelBinary.length + filterListBinary.length + 9);

		buf.put(wholeRowFilter ? (byte) 1 : (byte) 0);
		buf.putInt(partitionKeyLength);
		buf.putInt(modelBinary.length);
		buf.put(modelBinary);
		buf.put(filterListBinary);
		return buf.array();
	}

	public boolean init(
			final byte[] filterBytes,
			final byte[] modelBytes ) {
		filterList.clear();
		if ((filterBytes != null) && (filterBytes.length > 0)) {
			final List<Persistable> decodedFilterList = URLClassloaderUtils.fromBinaryAsList(filterBytes);

			if (decodedFilterList == null) {
				LOGGER.error("Failed to decode filter list");
				return false;
			}

			for (final Persistable decodedFilter : decodedFilterList) {
				if (decodedFilter instanceof DistributableQueryFilter) {
					filterList.add((DistributableQueryFilter) decodedFilter);
				}
				else {
					LOGGER.warn("Unrecognized type for decoded filter!" + decodedFilter.getClass().getName());
				}
			}
		}

		model = (CommonIndexModel) URLClassloaderUtils.fromBinary(modelBytes);

		if (model == null) {
			LOGGER.error("Failed to decode index model");
			return false;
		}

		commonIndexFieldIds = DataStoreUtils.getUniqueDimensionFields(model);

		return true;
	}

	public boolean init(
			final List<DistributableQueryFilter> filterList,
			final CommonIndexModel model,
			final String[] visList ) {
		this.filterList.clear();
		this.filterList.addAll(filterList);

		this.model = model;

		commonIndexFieldIds = DataStoreUtils.getUniqueDimensionFields(model);

		return true;
	}

	public void setWholeRowFilter(
			boolean wholeRowFilter ) {
		this.wholeRowFilter = wholeRowFilter;
	}

	/**
	 * If true (wholeRowFilter == true), then the filter will use the
	 * filterRowCells method instead of filterKeyValue
	 */
	@Override
	public boolean hasFilterRow() {
		return wholeRowFilter;
	}

	/**
	 * Handle the entire row at one time
	 */
	@Override
	public void filterRowCells(
			List<Cell> rowCells )
			throws IOException {
		if (!rowCells.isEmpty()) {
			Iterator<Cell> it = rowCells.iterator();

			GeoWaveKeyImpl rowKey = null;
			commonData = new PersistentDataset<CommonIndexValue>();

			while (it.hasNext()) {
				Cell cell = it.next();

				// Grab rowkey from first cell
				if (rowKey == null) {
					rowKey = new GeoWaveKeyImpl(
							cell.getRowArray(),
							partitionKeyLength,
							cell.getRowOffset(),
							cell.getRowLength());
				}

				unreadData = aggregateFieldData(
						cell,
						commonData);
			}

			ReturnCode code = applyFilter(rowKey);

			if (code == ReturnCode.SKIP) {
				rowCells.clear();
			}
		}
	}

	/**
	 * filterKeyValue is executed second
	 */
	@Override
	public ReturnCode filterKeyValue(
			final Cell cell )
			throws IOException {
		if (wholeRowFilter) {
			// let filterRowCells do the work
			return ReturnCode.INCLUDE_AND_NEXT_COL;
		}

		commonData = new PersistentDataset<CommonIndexValue>();

		unreadData = aggregateFieldData(
				cell,
				commonData);

		return applyFilter(cell);
	}

	protected ReturnCode applyFilter(
			final Cell cell ) {
		final GeoWaveKeyImpl rowKey = new GeoWaveKeyImpl(
				cell.getRowArray(),
				partitionKeyLength,
				cell.getRowOffset(),
				cell.getRowLength());

		return applyFilter(rowKey);
	}

	protected ReturnCode applyFilter(
			final GeoWaveKeyImpl rowKey ) {

		persistenceEncoding = null;

		try {
			persistenceEncoding = getPersistenceEncoding(
					rowKey,
					commonData,
					unreadData);

			if (filterInternal(persistenceEncoding)) {
				return ReturnCode.INCLUDE_AND_NEXT_COL;
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error applying distributed filter.",
					e);
		}

		return ReturnCode.SKIP;
	}

	protected static CommonIndexedPersistenceEncoding getPersistenceEncoding(
			final GeoWaveKeyImpl rowKey,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {

		return new DeferredReadCommonIndexedPersistenceEncoding(
				rowKey.getInternalAdapterId(),
				new ByteArrayId(
						rowKey.getDataId()),
				new ByteArrayId(
						rowKey.getPartitionKey()),
				new ByteArrayId(
						rowKey.getSortKey()),
				rowKey.getNumberOfDuplicates(),
				commonData,
				unreadData);

	}

	public CommonIndexedPersistenceEncoding getPersistenceEncoding() {
		return persistenceEncoding;
	}

	public IndexedAdapterPersistenceEncoding getAdapterEncoding(
			final DataAdapter dataAdapter ) {
		final PersistentDataset<Object> adapterExtendedValues = new PersistentDataset<Object>();
		if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
			((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
					dataAdapter,
					model);
			final PersistentDataset<Object> existingExtValues = ((AbstractAdapterPersistenceEncoding) persistenceEncoding)
					.getAdapterExtendedData();
			if (existingExtValues != null) {
				adapterExtendedValues.addValues(existingExtValues.getValues());
			}
		}

		adapterEncoding = new IndexedAdapterPersistenceEncoding(
				persistenceEncoding.getInternalAdapterId(),
				persistenceEncoding.getDataId(),
				persistenceEncoding.getInsertionPartitionKey(),
				persistenceEncoding.getInsertionSortKey(),
				persistenceEncoding.getDuplicateCount(),
				persistenceEncoding.getCommonData(),
				new PersistentDataset<byte[]>(),
				adapterExtendedValues);

		return adapterEncoding;
	}

	// Called by the aggregation endpoint, after filtering the current row
	public Object decodeRow(
			final DataAdapter dataAdapter ) {
		return dataAdapter.decode(
				getAdapterEncoding(dataAdapter),
				new PrimaryIndex(
						null,
						model));
	}

	protected boolean filterInternal(
			final CommonIndexedPersistenceEncoding encoding ) {
		if (filterList == null) {
			LOGGER.error("FILTER IS NULL");
			return false;
		}

		if (model == null) {
			LOGGER.error("MODEL IS NULL");
			return false;
		}

		if (encoding == null) {
			LOGGER.error("ENCODING IS NULL");
			return false;
		}

		for (final DistributableQueryFilter filter : filterList) {
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
		final byte[] qualBuf = CellUtil.cloneQualifier(cell);
		final byte[] valBuf = CellUtil.cloneValue(cell);

		final FlattenedDataSet dataSet = DataStoreUtils.decomposeFlattenedFields(
				qualBuf,
				valBuf,
				null,
				commonIndexFieldIds.size() - 1);

		final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final int ordinal = fieldInfo.getFieldPosition();

			if (ordinal < commonIndexFieldIds.size()) {
				final ByteArrayId commonIndexFieldId = commonIndexFieldIds.get(ordinal);
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(commonIndexFieldId);
				if (reader != null) {
					final CommonIndexValue fieldValue = reader.readField(fieldInfo.getValue());
					commonData.addValue(
							commonIndexFieldId,
							fieldValue);
				}
				else {
					LOGGER.error("Could not find reader for common index field: " + commonIndexFieldId.getString());
				}
			}
		}

		return dataSet.getFieldsDeferred();
	}

	public int getPartitionKeyLength() {
		return partitionKeyLength;
	}

	public void setPartitionKeyLength(
			int partitionKeyLength ) {
		this.partitionKeyLength = partitionKeyLength;
	}
}
