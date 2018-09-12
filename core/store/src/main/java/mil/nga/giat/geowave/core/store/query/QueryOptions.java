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
package mil.nga.giat.geowave.core.store.query;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

/**
 * Directs a query to restrict searches to specific adapters, indices, etc.. For
 * example, if a set of adapter IDs are provided, all data in the data store
 * that matches the query parameter with the matching adapters are returned.
 * Without providing a specific value for adapters and indices, a query searches
 * all persisted indices and adapters. Since some data stores may not be
 * configured to persist indices or adapters, it is advised to always provide
 * adapters and indices to a QueryOptions. This maximizes the reuse of the code
 * making the query.
 *
 * If no index is provided, all indices are checked. The data store is expected
 * to use statistics to determine which the indices that index data for the any
 * given adapter.
 *
 * If queries are made across multiple indices, the default is to de-duplicate.
 *
 * Container object that encapsulates additional options to be applied to a
 * {@link Query}
 *
 * @since 0.8.7
 */

// TODO: Allow secondary index requests to bypass CBO.

public class QueryOptions implements
		Persistable,
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 544085046847603371L;

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"SE_TRANSIENT_FIELD_NOT_RESTORED"
	})
	private transient List<DataAdapter<Object>> adapters = null;

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"SE_TRANSIENT_FIELD_NOT_RESTORED"
	})
	private List<ByteArrayId> adapterIds = null;
	private ByteArrayId indexId = null;
	private transient PrimaryIndex index = null;
	private Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregationAdapterPair;
	private Integer maxRangeDecomposition = null;
	private Integer limit = -1;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private String[] authorizations = new String[0];
	private Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair;

	public QueryOptions(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		adapters = null;
		adapterIds = adapterId == null ? Collections.<ByteArrayId> emptyList() : Collections.singletonList(adapterId);
		this.indexId = indexId;
	}

	public QueryOptions(
			final DataAdapter<?> adapter ) {
		setAdapter(adapter);
	}

	public QueryOptions(
			final PrimaryIndex index ) {
		setIndex(index);
	}

	public QueryOptions(
			final DataAdapter<?> adapter,
			final PrimaryIndex index ) {
		setAdapter(adapter);
		setIndex(index);
	}

	public QueryOptions(
			final List<DataAdapter<?>> adapters ) {
		setAdapters(adapters);
	}

	public QueryOptions(
			final DataAdapter<?> adapter,
			final String[] authorizations ) {
		setAdapter(adapter);
		this.authorizations = authorizations;
	}

	public QueryOptions(
			final DataAdapter<?> adapter,
			final PrimaryIndex index,
			final String[] authorizations ) {
		setAdapter(adapter);
		setIndex(index);
		this.authorizations = authorizations;
	}

	public QueryOptions(
			final QueryOptions options ) {
		indexId = options.indexId;
		adapterIds = options.adapterIds;
		adapters = options.adapters;
		maxRangeDecomposition = options.maxRangeDecomposition;
		limit = options.limit;
		authorizations = options.authorizations;
		adapters = options.adapters;
		index = options.index;
		aggregationAdapterPair = options.aggregationAdapterPair;
	}

	/**
	 *
	 * @param adapter
	 * @param index
	 * @param limit
	 *            null or -1 implies no limit. Otherwise, constrain the number
	 *            of results to the provided limit.
	 * @param scanCallback
	 * @param authorizations
	 */
	public QueryOptions(
			final DataAdapter<?> adapter,
			final PrimaryIndex index,
			final Integer limit,
			final ScanCallback<?, ?> scanCallback,
			final String[] authorizations ) {
		super();
		setAdapter(adapter);
		setIndex(index);
		setLimit(limit);
		this.authorizations = authorizations;
	}

	/**
	 * @param fieldIds
	 *            the subset of fieldIds to be included with each query result
	 * @param adapter
	 *            the associated data adapter
	 */
	public QueryOptions(
			final List<String> fieldIds,
			final DataAdapter<?> adapter ) {
		super();
		fieldIdsAdapterPair = new ImmutablePair<List<String>, DataAdapter<?>>(
				fieldIds,
				adapter);
	}

	public QueryOptions() {}

	public List<ByteArrayId> getAdapterIds() {
		return adapterIds;
	}

	public List<DataAdapter<Object>> getAdapters() {
		return adapters;
	}

	public void setAdapters(
			final List<DataAdapter<?>> adapters ) {
		this.adapters = Lists.transform(
				adapters,
				new Function<DataAdapter<?>, DataAdapter<Object>>() {
					@Override
					public DataAdapter<Object> apply(
							final DataAdapter<?> input ) {
						return (DataAdapter<Object>) input;
					}
				});
		adapterIds = Lists.transform(
				adapters,
				new Function<DataAdapter<?>, ByteArrayId>() {

					@Override
					public ByteArrayId apply(
							final DataAdapter<?> input ) {
						return input.getAdapterId();
					}
				});
	}

	public void setAdapter(
			final DataAdapter<?> adapter ) {
		if (adapter != null) {
			adapters = Collections.<DataAdapter<Object>> singletonList((DataAdapter<Object>) adapter);
			adapterIds = Collections.singletonList(adapter.getAdapterId());
		}
		else {
			adapterIds = Collections.emptyList();
			adapters = null;
		}

	}

	public void setAdapterId(
			final ByteArrayId adapterId ) {
		adapterIds = Arrays.asList(adapterId);
	}

	public void setAdapterIds(
			final List<ByteArrayId> adapterIds ) {
		if (adapterIds != null) {
			adapters = null;
			this.adapterIds = adapterIds;
		}
		else {
			this.adapterIds = Collections.emptyList();
			adapters = null;
		}
	}

	public void setMaxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}

	/**
	 * @param index
	 */
	public void setIndex(
			final PrimaryIndex index ) {
		if (index != null) {
			indexId = index.getId();
			this.index = index;
		}
		else {
			indexId = null;
			this.index = null;
		}
	}

	/**
	 * @param index
	 */
	public void setIndexId(
			final ByteArrayId indexId ) {
		if (indexId != null) {
			this.indexId = indexId;
			index = null;
		}
		else {
			this.indexId = null;
			index = null;
		}
	}

	/**
	 *
	 * @return the max range decomposition to use when computing query ranges
	 */
	public Integer getMaxRangeDecomposition() {
		return maxRangeDecomposition;
	}

	/**
	 * a value of null indicates to use the data store configured default
	 * 
	 * @param maxRangeDecomposition
	 */
	public void setMaxRangeDecomposition(
			Integer maxRangeDecomposition ) {
		this.maxRangeDecomposition = maxRangeDecomposition;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	/**
	 *
	 * @return Limit the number of data items to return
	 */
	public Integer getLimit() {
		return limit;
	}

	/**
	 * a value <= 0 or null indicates no limits
	 *
	 * @param limit
	 */
	public void setLimit(
			Integer limit ) {
		if ((limit == null) || (limit == 0)) {
			limit = -1;
		}
		this.limit = limit;
	}

	public boolean isAllAdapters() {
		return ((adapterIds == null) || adapterIds.isEmpty());
	}

	/**
	 *
	 * @return authorizations to apply to the query in addition to the
	 *         authorizations assigned to the data store as a whole.
	 */
	public String[] getAuthorizations() {
		return authorizations == null ? new String[0] : authorizations;
	}

	public void setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
	}

	/**
	 *
	 * @return a paring of fieldIds and their associated data adapter >>>>>>>
	 *         wip: bitmask approach
	 */
	public Pair<List<String>, DataAdapter<?>> getFieldIdsAdapterPair() {
		return fieldIdsAdapterPair;
	}

	/**
	 *
	 * @param fieldIds
	 *            the subset of fieldIds to be included with each query result
	 * @param adapter
	 *            the associated data adapter
	 */
	public void setFieldIds(
			final List<String> fieldIds,
			final DataAdapter<?> adapter ) {
		fieldIdsAdapterPair = new ImmutablePair<List<String>, DataAdapter<?>>(
				fieldIds,
				adapter);
	}

	@Override
	public byte[] toBinary() {

		final byte[] authBytes = StringUtils.stringsToBinary(getAuthorizations());
		int iSize = 4;
		if (indexId != null) {
			iSize += indexId.getBytes().length;
		}

		int aSize = 4;
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId id : adapterIds) {
				aSize += id.getBytes().length + 4;
			}
		}

		byte[] adapterBytes = new byte[0];
		if ((fieldIdsAdapterPair != null) && (fieldIdsAdapterPair.getRight() != null)) {
			adapterBytes = PersistenceUtils.toBinary(fieldIdsAdapterPair.getRight());
		}

		byte[] fieldIdsBytes = new byte[0];
		if ((fieldIdsAdapterPair != null) && (fieldIdsAdapterPair.getLeft() != null)
				&& (fieldIdsAdapterPair.getLeft().size() > 0)) {
			final String fieldIdsString = org.apache.commons.lang3.StringUtils.join(
					fieldIdsAdapterPair.getLeft(),
					',');
			fieldIdsBytes = StringUtils.stringToBinary(fieldIdsString.toString());
		}

		final ByteBuffer buf = ByteBuffer.allocate(24 + authBytes.length + aSize + iSize + adapterBytes.length
				+ fieldIdsBytes.length);
		buf.putInt(adapterBytes.length);
		if (adapterBytes.length > 0) {
			buf.put(adapterBytes);
		}
		buf.putInt(fieldIdsBytes.length);
		if (fieldIdsBytes.length > 0) {
			buf.put(fieldIdsBytes);
		}

		buf.putInt(authBytes.length);
		buf.put(authBytes);

		if (indexId != null) {
			buf.putInt(indexId.getBytes().length);
			buf.put(indexId.getBytes());
		}
		else {
			buf.putInt(0);
		}

		buf.putInt(adapterIds == null ? 0 : adapterIds.size());
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId id : adapterIds) {
				final byte[] idBytes = id.getBytes();
				buf.putInt(idBytes.length);
				buf.put(idBytes);
			}
		}

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {

		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int adapterBytesLength = buf.getInt();
		AbstractDataAdapter<?> dataAdapter = null;
		if (adapterBytesLength > 0) {
			final byte[] adapterBytes = new byte[adapterBytesLength];
			buf.get(adapterBytes);
			dataAdapter = (AbstractDataAdapter<?>) PersistenceUtils.fromBinary(adapterBytes);
		}
		final int fieldIdsLength = buf.getInt();
		List<String> fieldIds = null;
		if (fieldIdsLength > 0) {
			final byte[] fieldIdsBytes = new byte[fieldIdsLength];
			buf.get(fieldIdsBytes);
			fieldIds = Arrays.asList(StringUtils.stringFromBinary(
					fieldIdsBytes).split(
					","));
		}
		if ((dataAdapter != null) && (fieldIds != null)) {
			setFieldIds(
					fieldIds,
					dataAdapter);
		}

		final byte[] authBytes = new byte[buf.getInt()];
		buf.get(authBytes);

		authorizations = StringUtils.stringsFromBinary(authBytes);

		indexId = null;
		final int size = buf.getInt();

		if (size > 0) {
			final byte[] idBytes = new byte[size];
			buf.get(idBytes);
			indexId = new ByteArrayId(
					idBytes);
		}

		int count = buf.getInt();
		adapterIds = new ArrayList<ByteArrayId>();
		while (count > 0) {
			final int l = buf.getInt();
			final byte[] idBytes = new byte[l];
			buf.get(idBytes);
			adapterIds.add(new ByteArrayId(
					idBytes));
			count--;
		}
	}

	public Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregationAdapterPair;
	}

	public void setAggregation(
			final Aggregation<?, ?, ?> aggregation,
			final DataAdapter<?> adapter ) {
		aggregationAdapterPair = new ImmutablePair<DataAdapter<?>, Aggregation<?, ?, ?>>(
				adapter,
				aggregation);
	}

	@Override
	public String toString() {
		return "QueryOptions [adapterId=" + adapterIds + ", limit=" + limit + ", authorizations="
				+ Arrays.toString(authorizations) + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterIds == null) ? 0 : adapterIds.hashCode());
		result = (prime * result) + Arrays.hashCode(authorizations);
		result = (prime * result) + ((indexId == null) ? 0 : indexId.hashCode());
		result = (prime * result) + ((limit == null) ? 0 : limit.hashCode());
		result = (prime * result) + Arrays.hashCode(maxResolutionSubsamplingPerDimension);
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final QueryOptions other = (QueryOptions) obj;
		if (adapterIds == null) {
			if (other.adapterIds != null) {
				return false;
			}
		}
		else if (!adapterIds.equals(other.adapterIds)) {
			return false;
		}
		if (!Arrays.equals(
				authorizations,
				other.authorizations)) {
			return false;
		}
		if (indexId == null) {
			if (other.indexId != null) {
				return false;
			}
		}
		else if (!indexId.equals(other.indexId)) {
			return false;
		}
		if (limit == null) {
			if (other.limit != null) {
				return false;
			}
		}
		else if (!limit.equals(other.limit)) {
			return false;
		}
		if (!Arrays.equals(
				maxResolutionSubsamplingPerDimension,
				other.maxResolutionSubsamplingPerDimension)) {
			return false;
		}
		return true;
	}
}
