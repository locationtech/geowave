package mil.nga.giat.geowave.core.store.query;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
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

	private static ScanCallback<Object> DEFAULT_CALLBACK = new ScanCallback<Object>() {
		@Override
		public void entryScanned(
				final DataStoreEntryInfo entryInfo,
				final Object entry ) {}
	};

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
	private Pair<DataAdapter<?>, Aggregation<?>> aggregationAdapterPair;
	private Integer limit = -1;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private transient ScanCallback<?> scanCallback = DEFAULT_CALLBACK;
	private String[] authorizations = new String[0];
	private List<String> fieldIds = Collections.emptyList();

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
			final List<ByteArrayId> adapterIds ) {
		setAdapterIds(adapterIds);
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
		limit = options.limit;
		scanCallback = options.scanCallback;
		authorizations = options.authorizations;
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
			final ScanCallback<?> scanCallback,
			final String[] authorizations ) {
		super();
		setAdapter(adapter);
		setIndex(index);
		setLimit(limit);
		this.scanCallback = scanCallback;
		this.authorizations = authorizations;
	}

	public QueryOptions() {}

	public void setAdapterIds(
			final List<ByteArrayId> adapterIds ) {
		adapters = null;
		this.adapterIds = adapterIds == null ? Collections.<ByteArrayId> emptyList() : adapterIds;
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

	public void setMaxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
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

	public ScanCallback<?> getScanCallback() {
		return scanCallback == null ? DEFAULT_CALLBACK : scanCallback;
	}

	/**
	 * @param scanCallback
	 *            a function called for each item discovered per the query
	 *            constraints
	 */
	public void setScanCallback(
			final ScanCallback<?> scanCallback ) {
		this.scanCallback = scanCallback;
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
	 * Return the set of adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them.
	 * 
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 * 
	 * @param adapterStore
	 * @param
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */

	public List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> getIndicesForAdapters(
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return combineByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	/**
	 * Return a set list adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them. The full set of adapter/index associations is
	 * reduced so that a single index is queried per adapter and the number
	 * indices queried is minimized.
	 * 
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 * 
	 * @param adapterStore
	 * @param adapterIndexMappingStore
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */
	public List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> getAdaptersWithMinimalSetOfIndices(
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return reduceIndicesAndGroupByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	private List<Pair<PrimaryIndex, DataAdapter<Object>>> compileIndicesForAdapters(
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<DataAdapter<Object>>();
				for (final ByteArrayId id : adapterIds) {
					final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
		}
		else {
			adapters = new ArrayList<DataAdapter<Object>>();
			try (CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext())
					this.adapters.add((DataAdapter<Object>) it.next());
			}
		}
		List<Pair<PrimaryIndex, DataAdapter<Object>>> result = new ArrayList<Pair<PrimaryIndex, DataAdapter<Object>>>();
		for (DataAdapter<Object> adapter : adapters) {
			final AdapterToIndexMapping indices = adapterIndexMappingStore.getIndicesForAdapter(adapter.getAdapterId());
			if (indexId != null && indices.contains(indexId)) {
				if (index == null) {
					this.index = (PrimaryIndex) indexStore.getIndex(indexId);
				}
				if (index != null) {
					result.add(Pair.of(
							index,
							adapter));
				}
			}
			else if (indices.isNotEmpty()) {
				for (ByteArrayId id : indices.getIndexIds()) {
					final PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(id);
					// this could happen if persistent was turned off
					if (index != null) {
						result.add(Pair.of(
								index,
								adapter));
					}
				}
			}
		}
		return result;
	}

	public CloseableIterator<DataAdapter<?>> getAdapters(
			final AdapterStore adapterStore ) {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<DataAdapter<Object>>();
				for (final ByteArrayId id : adapterIds) {
					final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
			return new CloseableIterator.Wrapper(
					adapters.iterator());
		}
		return adapterStore.getAdapters();
	}

	public DataAdapter[] getAdaptersArray(
			final AdapterStore adapterStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<DataAdapter<Object>>();
				for (final ByteArrayId id : adapterIds) {
					final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
			return adapters.toArray(new DataAdapter[adapters.size()]);

		}
		final List<DataAdapter> list = new ArrayList<DataAdapter>();
		try (CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				list.add(it.next());
			}
		}
		return list.toArray(new DataAdapter[list.size()]);
	}

	public List<ByteArrayId> getAdapterIds(
			final AdapterStore adapterStore )
			throws IOException {
		final List<ByteArrayId> ids = new ArrayList<ByteArrayId>();
		if ((adapterIds == null) || adapterIds.isEmpty()) {
			try (CloseableIterator<DataAdapter<?>> it = getAdapters(adapterStore)) {
				while (it.hasNext()) {
					ids.add(it.next().getAdapterId());
				}
			}
		}
		else {
			ids.addAll(adapterIds);
		}
		return ids;
	}

	/**
	 * 
	 * @return the fieldIds
	 */
	public List<String> getFieldIds() {
		return fieldIds;
	}

	/**
	 * 
	 * @param fieldIds
	 *            the desired subset of fieldIds to be included in query results
	 */
	public void setFieldIds(
			final List<String> fieldIds ) {
		if (fieldIds != null) {
			this.fieldIds = fieldIds;
		}
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

		byte[] fieldIdsBytes = new byte[0];
		if (fieldIds != null && fieldIds.size() > 0) {
			final String fieldIdsString = org.apache.commons.lang3.StringUtils.join(
					fieldIds,
					',');
			fieldIdsBytes = StringUtils.stringToBinary(fieldIdsString.toString());
		}

		final ByteBuffer buf = ByteBuffer.allocate(20 + authBytes.length + aSize + iSize + fieldIdsBytes.length);
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
		else
			buf.putInt(0);

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
		final int fieldIdsLength = buf.getInt();
		if (fieldIdsLength > 0) {
			final byte[] fieldIdsBytes = new byte[fieldIdsLength];
			buf.get(fieldIdsBytes);
			fieldIds = Arrays.asList(StringUtils.stringFromBinary(
					fieldIdsBytes).split(
					","));
		}
		final byte[] authBytes = new byte[buf.getInt()];
		buf.get(authBytes);

		authorizations = StringUtils.stringsFromBinary(authBytes);

		indexId = null;
		int size = buf.getInt();

		if (size > 0) {
			final byte[] idBytes = new byte[size];
			buf.get(idBytes);
			this.indexId = new ByteArrayId(
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

	public Pair<DataAdapter<?>, Aggregation<?>> getAggregation() {
		return aggregationAdapterPair;
	}

	public void setAggregation(
			final Aggregation<?> aggregation,
			final DataAdapter<?> adapter ) {
		aggregationAdapterPair = new ImmutablePair<DataAdapter<?>, Aggregation<?>>(
				adapter,
				aggregation);
	}

	@Override
	public String toString() {
		return "QueryOptions [adapterId=" + adapterIds + ", limit=" + limit + ", authorizations=" + Arrays.toString(authorizations) + "]";
	}

	private void sortInPlace(
			List<Pair<PrimaryIndex, DataAdapter<Object>>> input ) {
		Collections.sort(
				input,
				new Comparator<Pair<PrimaryIndex, DataAdapter<Object>>>() {

					@Override
					public int compare(
							Pair<PrimaryIndex, DataAdapter<Object>> o1,
							Pair<PrimaryIndex, DataAdapter<Object>> o2 ) {

						return o1.getKey().getId().compareTo(
								o1.getKey().getId());
					}
				});
	}

	private List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> combineByIndex(
			List<Pair<PrimaryIndex, DataAdapter<Object>>> input ) {
		List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> result = new ArrayList<Pair<PrimaryIndex, List<DataAdapter<Object>>>>();
		sortInPlace(input);
		List<DataAdapter<Object>> adapterSet = new ArrayList<DataAdapter<Object>>();
		Pair<PrimaryIndex, DataAdapter<Object>> last = null;
		for (Pair<PrimaryIndex, DataAdapter<Object>> item : input) {
			if (last != null && !last.getKey().getId().equals(
					item.getKey().getId())) {
				result.add(Pair.of(
						last.getLeft(),
						adapterSet));
				adapterSet = new ArrayList<DataAdapter<Object>>();

			}
			adapterSet.add(item.getValue());
			last = item;
		}
		if (last != null) result.add(Pair.of(
				last.getLeft(),
				adapterSet));
		return result;
	}

	private List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> reduceIndicesAndGroupByIndex(
			List<Pair<PrimaryIndex, DataAdapter<Object>>> input ) {
		final List<Pair<PrimaryIndex, DataAdapter<Object>>> result = new ArrayList<Pair<PrimaryIndex, DataAdapter<Object>>>();
		// sort by index to eliminate the amount of indices returned
		sortInPlace(input);
		final Set<DataAdapter<Object>> adapterSet = new HashSet<DataAdapter<Object>>();
		for (Pair<PrimaryIndex, DataAdapter<Object>> item : input) {
			if (adapterSet.add(item.getRight())) {
				result.add(item);
			}
		}
		return combineByIndex(result);
	}
}
