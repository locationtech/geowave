package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class BaseQueryOptions
{
	private static Logger LOGGER = LoggerFactory.getLogger(BaseQueryOptions.class);
	private static ScanCallback<Object, GeoWaveRow> DEFAULT_CALLBACK = new ScanCallback<Object, GeoWaveRow>() {
		@Override
		public void entryScanned(
				final Object entry,
				final GeoWaveRow row ) {}
	};

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"SE_TRANSIENT_FIELD_NOT_RESTORED"
	})
	private Collection<InternalDataAdapter<?>> adapters = null;
	private Collection<Short> adapterIds = null;
	private ByteArrayId indexId = null;
	private transient PrimaryIndex index = null;
	private Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregationAdapterPair;
	private Integer limit = -1;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private transient ScanCallback<?, ?> scanCallback = DEFAULT_CALLBACK;
	private String[] authorizations = new String[0];
	private Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair;
	private boolean nullId = false;

	public BaseQueryOptions(
			final QueryOptions options,
			final InternalAdapterStore internalAdapterStore ) {
		super();
		indexId = options.getIndexId();
		index = options.getIndex();
		limit = options.getLimit();
		maxResolutionSubsamplingPerDimension = options.getMaxResolutionSubsamplingPerDimension();
		authorizations = options.getAuthorizations();

		if (options.getAggregation() != null) {
			final DataAdapter<?> adapter = options.getAggregation().getLeft();
			final short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			aggregationAdapterPair = new ImmutablePair<>(
					new InternalDataAdapterWrapper(
							(WritableDataAdapter) adapter,
							internalAdapterId),
					options.getAggregation().getRight());
		}

		if (options.getFieldIdsAdapterPair() != null) {
			final DataAdapter<?> adapter = options.getFieldIdsAdapterPair().getRight();
			final short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			fieldIdsAdapterPair = new ImmutablePair<>(
					options.getFieldIdsAdapterPair().getLeft(),
					new InternalDataAdapterWrapper(
							(WritableDataAdapter) adapter,
							internalAdapterId));
		}

		if (options.getAdapterIds() != null) {
			adapterIds = Collections2.filter(
					Lists.transform(
							options.getAdapterIds(),
							new Function<ByteArrayId, Short>() {
								@Override
								public Short apply(
										final ByteArrayId input ) {
									return internalAdapterStore.getInternalAdapterId(input);
								}
							}),
					new Predicate<Short>() {
						@Override
						public boolean apply(
								final Short input ) {
							if (input == null) {
								nullId = true;
								return false;
							}
							return true;
						}
					});
		}
		if (options.getAdapters() != null) {
			adapters = Collections2.filter(
					Lists.transform(
							options.getAdapters(),
							new Function<DataAdapter<?>, InternalDataAdapter<?>>() {
								@Override
								public InternalDataAdapter<?> apply(
										final DataAdapter<?> adapter ) {
									final Short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter
											.getAdapterId());
									if (internalAdapterId == null) {
										LOGGER.warn("Unable to get internal Adapter ID from store.");
										nullId = true;
										return null;
									}
									return new InternalDataAdapterWrapper(
											(WritableDataAdapter) adapter,
											internalAdapterId);
								}
							}),
					new Predicate<InternalDataAdapter<?>>() {
						@Override
						public boolean apply(
								final InternalDataAdapter<?> input ) {
							return input != null;
						}
					});
		}
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

	public List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> getIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return BaseDataStoreUtils.combineByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	public CloseableIterator<InternalDataAdapter<?>> getAdapters(
			final PersistentAdapterStore adapterStore ) {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<>();
				for (final short id : adapterIds) {
					final InternalDataAdapter adapter = adapterStore.getAdapter(id);
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

	public InternalDataAdapter<?>[] getAdaptersArray(
			final PersistentAdapterStore adapterStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<>();
				for (final Short id : adapterIds) {
					if (id == null) {
						nullId = true;
						continue;
					}
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
					else {
						nullId = true;
					}
				}
			}
			return adapters.toArray(new InternalDataAdapter[adapters.size()]);
		}
		if (nullId) {
			return new InternalDataAdapter[] {};
		}
		final List<InternalDataAdapter<?>> list = new ArrayList<>();
		if ((adapterStore != null) && (adapterStore.getAdapters() != null)) {
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					list.add(it.next());
				}
			}
		}
		return list.toArray(new InternalDataAdapter[list.size()]);
	}

	public void setInternalAdapterId(
			final short internalAdapterId ) {
		adapterIds = Arrays.asList(internalAdapterId);
	}

	public Collection<Short> getAdapterIds() {
		return adapterIds;
	}

	private List<Pair<PrimaryIndex, InternalDataAdapter<?>>> compileIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<>();
				for (final Short id : adapterIds) {
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
		}
		else if (!nullId && ((adapters == null) || adapters.isEmpty())) {
			adapters = new ArrayList<>();
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					adapters.add(it.next());
				}
			}
		}
		else if (adapters == null) {
			adapters = Collections.emptyList();
		}
		final List<Pair<PrimaryIndex, InternalDataAdapter<?>>> result = new ArrayList<>();
		for (final InternalDataAdapter<?> adapter : adapters) {
			final AdapterToIndexMapping indices = adapterIndexMappingStore.getIndicesForAdapter(adapter
					.getInternalAdapterId());
			if (index != null) {
				result.add(Pair.of(
						index,
						adapter));
			}
			else if ((indexId != null) && indices.contains(indexId)) {
				if (index == null) {
					index = (PrimaryIndex) indexStore.getIndex(indexId);
					result.add(Pair.of(
							index,
							adapter));
				}
			}
			else if (indices.isNotEmpty()) {
				for (final ByteArrayId id : indices.getIndexIds()) {
					final PrimaryIndex pIndex = (PrimaryIndex) indexStore.getIndex(id);
					// this could happen if persistent was turned off
					if (pIndex != null) {
						result.add(Pair.of(
								pIndex,
								adapter));
					}
				}
			}
		}
		return result;
	}

	public ScanCallback<?, ?> getScanCallback() {
		return scanCallback == null ? DEFAULT_CALLBACK : scanCallback;
	}

	/**
	 * @param scanCallback
	 *            a function called for each item discovered per the query
	 *            constraints
	 */
	public void setScanCallback(
			final ScanCallback<?, ?> scanCallback ) {
		this.scanCallback = scanCallback;
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

	public void setMaxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregationAdapterPair;
	}

	public void setAggregation(
			final Aggregation<?, ?, ?> aggregation,
			final InternalDataAdapter<?> adapter ) {
		aggregationAdapterPair = new ImmutablePair<>(
				adapter,
				aggregation);
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
	public List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> getAdaptersWithMinimalSetOfIndices(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		// TODO this probably doesn't have to use PrimaryIndex and should be
		// sufficient to use index IDs
		return BaseDataStoreUtils.reduceIndicesAndGroupByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	/**
	 *
	 * @return a paring of fieldIds and their associated data adapter >>>>>>>
	 *         wip: bitmask approach
	 */
	public Pair<List<String>, InternalDataAdapter<?>> getFieldIdsAdapterPair() {
		return fieldIdsAdapterPair;
	}

}
