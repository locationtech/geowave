package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;

public abstract class BaseReaderParamsBuilder<T, R extends BaseReaderParamsBuilder<T, R>>
{
	protected final Index index;
	protected final PersistentAdapterStore adapterStore;
	protected final InternalAdapterStore internalAdapterStore;
	protected final GeoWaveRowIteratorTransformer<T> rowTransformer;
	protected short[] adapterIds = null;
	protected double[] maxResolutionSubsamplingPerDimension = null;
	protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation = null;
	protected Pair<String[], InternalDataAdapter<?>> fieldSubsets = null;
	protected boolean isMixedVisibility = false;
	protected boolean isAuthorizationsLimiting = true;
	protected Integer limit = null;
	protected Integer maxRangeDecomposition = null;
	protected String[] additionalAuthorizations = null;

	public BaseReaderParamsBuilder(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final GeoWaveRowIteratorTransformer<T> rowTransformer ) {
		this.index = index;
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
		this.rowTransformer = rowTransformer;
	}

	protected abstract R builder();

	public R adapterIds(
			final short[] adapterIds ) {
		this.adapterIds = adapterIds;
		return builder();
	}

	public R maxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		return builder();
	}

	public R aggregation(
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation ) {
		this.aggregation = aggregation;
		return builder();
	}

	public R fieldSubsets(
			final Pair<String[], InternalDataAdapter<?>> fieldSubsets ) {
		this.fieldSubsets = fieldSubsets;
		return builder();
	}

	public R isMixedVisibility(
			final boolean isMixedVisibility ) {
		this.isMixedVisibility = isMixedVisibility;
		return builder();
	}

	public R isAuthorizationsLimiting(
			final boolean isAuthorizationsLimiting ) {
		this.isAuthorizationsLimiting = isAuthorizationsLimiting;
		return builder();
	}

	public R limit(
			final Integer limit ) {
		this.limit = limit;
		return builder();
	}

	public R maxRangeDecomposition(
			final Integer maxRangeDecomposition ) {
		this.maxRangeDecomposition = maxRangeDecomposition;
		return builder();
	}

	public R additionalAuthorizations(
			String... authorizations ) {
		this.additionalAuthorizations = authorizations;
		return builder();
	}
}
