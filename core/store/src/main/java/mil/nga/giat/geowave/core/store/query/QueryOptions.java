package mil.nga.giat.geowave.core.store.query;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

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
	private static final long serialVersionUID = 544085046847603372L;

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
	private transient List<Index<?, ?>> indices = null;

	private List<ByteArrayId> adapterIds = null;
	private List<ByteArrayId> indexIds = null;
	private Integer limit = -1;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private transient ScanCallback<?> scanCallback = DEFAULT_CALLBACK;
	private String[] authorizations = new String[0];
	private Boolean dedupAcrossIndices = null;

	public QueryOptions(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		adapters = null;
		indices = null;
		adapterIds = adapterId == null ? Collections.<ByteArrayId> emptyList() : Collections.singletonList(adapterId);
		indexIds = indexId == null ? Collections.<ByteArrayId> emptyList() : Collections.singletonList(indexId);
	}

	public QueryOptions(
			final PrimaryIndex index ) {
		setIndex(index);
	}

	public QueryOptions(
			final DataAdapter<?> adapter ) {
		setAdapter(adapter);
	}

	public QueryOptions(
			final DataAdapter<?> adapter,
			final PrimaryIndex index ) {
		setAdapter(adapter);
		setIndex(index);
	}

	public QueryOptions(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index ) {
		setAdapterIds(adapterIds);
		setIndex(index);
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
		indexIds = options.indexIds;
		adapterIds = options.adapterIds;
		adapters = options.adapters;
		indices = options.indices;
		limit = options.limit;
		scanCallback = options.scanCallback;
		authorizations = options.authorizations;
		dedupAcrossIndices = options.dedupAcrossIndices;
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

	public boolean isDedupAcrossIndices() {
		return dedupAcrossIndices == null ? ((indexIds == null) || (indexIds.size() > 0)) : dedupAcrossIndices;
	}

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
			indices = Collections.<Index<?, ?>> singletonList(index);
			indexIds = Collections.singletonList(index.getId());
		}
		else {
			indices = Collections.emptyList();
			indexIds = null;
		}
	}

	public void setDedupAcrosssIndices(
			final boolean dedupAcrossIndices ) {
		this.dedupAcrossIndices = dedupAcrossIndices;
	}

	public void setIndices(
			final Index<?, ?>[] indices ) {
		this.indices = indices == null ? Collections.<Index<?, ?>> emptyList() : new ArrayList<Index<?, ?>>();
		indexIds = new ArrayList<ByteArrayId>();
		if (indices != null) {
			for (final Index<?, ?> index : indices) {
				indexIds.add(index.getId());
				this.indices.add(index);
			}
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

	public boolean isAllAdaptersAndIndices() {
		return ((indexIds == null) || indexIds.isEmpty()) && ((adapterIds == null) || adapterIds.isEmpty());
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

	public CloseableIterator<Index<?, ?>> getIndices(
			final IndexStore indexStore ) {
		if ((indexIds != null) && !indexIds.isEmpty()) {
			if ((indices == null) || indices.isEmpty()) {
				indices = new ArrayList<Index<?, ?>>();
				for (final ByteArrayId id : indexIds) {
					final PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(id);
					if (index != null) {
						indices.add(index);
					}
				}
			}
			return new CloseableIterator.Wrapper<Index<?, ?>>(
					indices.iterator());
		}
		return indexStore.getIndices();
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

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
	public byte[] toBinary() {

		final byte[] authBytes = StringUtils.stringsToBinary(getAuthorizations());
		int iSize = 4;
		if ((indexIds != null) && !indexIds.isEmpty()) {
			for (final ByteArrayId id : indexIds) {
				iSize += id.getBytes().length + 4;
			}
		}

		int aSize = 4;
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			for (final ByteArrayId id : adapterIds) {
				aSize += id.getBytes().length + 4;
			}
		}

		final ByteBuffer buf = ByteBuffer.allocate(16 + authBytes.length + aSize + iSize);

		buf.putInt(dedupAcrossIndices == null ? -1 : (dedupAcrossIndices ? 1 : 0));
		buf.putInt(authBytes.length);
		buf.put(authBytes);

		buf.putInt(indexIds == null ? 0 : indexIds.size());
		if ((indexIds != null) && !indexIds.isEmpty()) {
			for (final ByteArrayId id : indexIds) {
				final byte[] idBytes = id.getBytes();
				buf.putInt(idBytes.length);
				buf.put(idBytes);
			}
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
		final int dedupCode = buf.getInt();

		dedupAcrossIndices = null;
		if (dedupCode >= 0) {
			dedupAcrossIndices = dedupCode > 0 ? Boolean.TRUE : Boolean.FALSE;
		}
		final byte[] authBytes = new byte[buf.getInt()];
		buf.get(authBytes);

		authorizations = StringUtils.stringsFromBinary(authBytes);

		int count = buf.getInt();
		indexIds = new ArrayList<ByteArrayId>();
		while (count > 0) {
			final int l = buf.getInt();
			final byte[] idBytes = new byte[l];
			buf.get(idBytes);
			indexIds.add(new ByteArrayId(
					idBytes));
			count--;
		}

		count = buf.getInt();
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

	@Override
	public String toString() {
		return "QueryOptions [adapterId=" + adapterIds + ", indexIds=" + indexIds + ", limit=" + limit + ", authorizations=" + Arrays.toString(authorizations) + "]";
	}
}
