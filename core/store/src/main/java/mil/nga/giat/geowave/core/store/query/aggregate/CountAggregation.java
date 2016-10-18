package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Persistable;

public class CountAggregation<T> implements
		Aggregation<Persistable, CountResult, T>
{
	private CountResult countResult = new CountResult();

	public CountAggregation() {}

	@Override
	public String toString() {
		return countResult.toString();
	}

	@Override
	public void aggregate(
			final T entry ) {
		if (!countResult.isSet()) {
			countResult.count = 0;
		}
		countResult.count += 1;
	}

	@Override
	public Persistable getParameters() {
		return null;
	}

	@Override
	public CountResult getResult() {
		if (!countResult.isSet()) {
			return null;
		}
		return countResult;
	}

	@Override
	public void setParameters(
			final Persistable parameters ) {}

	@Override
	public void clearResult() {
		countResult.count = 0;
	}
}
