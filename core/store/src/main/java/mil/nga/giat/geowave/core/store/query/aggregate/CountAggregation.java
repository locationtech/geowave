package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Persistable;

public class CountAggregation<T> implements
		Aggregation<Persistable, CountResult, T>
{
	private long count = Long.MIN_VALUE;

	public CountAggregation() {}
	
	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public void aggregate(
			final T entry ) {
		if (!isSet()) {
			count = 0;
		}
		
		count += 1;
	}

	@Override
	public Persistable getParameters() {
		return null;
	}

	@Override
	public CountResult getResult() {
		if (!isSet()) {
			return null;
		}
		
		return new CountResult(count);
	}

	@Override
	public void setParameters(
			final Persistable parameters ) {}

	@Override
	public void clearResult() {
		count = 0;
	}
}
