package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class CountAggregation implements
		CommonIndexAggregation<Persistable, CountResult>
{
	private CountResult countResult = new CountResult();

	public CountAggregation() {}

	@Override
	public String toString() {
		return countResult.toString();
	}

	@Override
	public void aggregate(
			final CommonIndexedPersistenceEncoding entry ) {
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
		countResult = new CountResult();
	}
}
