package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.persist.Persistable;

public class CommonQueryOptions implements
		Persistable
{
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Integer maxRangeDecomposition;
	private final Integer limit;
	private final String[] authorizations;

	public CommonQueryOptions(
			final Integer limit,
			final String... authorizations ) {
		this(
				null,
				null,
				limit,
				authorizations);
	}

	public CommonQueryOptions(
			final double[] maxResolutionSubsamplingPerDimension,
			final String... authorizations ) {
		this(
				maxResolutionSubsamplingPerDimension,
				null,
				null,
				authorizations);

	}

	public CommonQueryOptions(
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer maxRangeDecomposition,
			final Integer limit,
			final String... authorizations ) {
		super();
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.maxRangeDecomposition = maxRangeDecomposition;
		this.limit = limit;
		this.authorizations = authorizations;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Integer getMaxRangeDecomposition() {
		return maxRangeDecomposition;
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

}