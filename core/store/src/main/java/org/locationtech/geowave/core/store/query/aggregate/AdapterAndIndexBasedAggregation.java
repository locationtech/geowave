package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;

public interface AdapterAndIndexBasedAggregation<P extends Persistable, R, T> extends
		Aggregation<P, R, T>
{
	Aggregation<P, R, T> createAggregation(
			DataTypeAdapter<T> adapter,
			Index index );

	@Override
	default byte[] toBinary() {
		return new byte[0];
	}

	@Override
	default void fromBinary(
			final byte[] bytes ) {}

	@Override
	default P getParameters() {
		return null;
	}

	@Override
	default void setParameters(
			final P parameters ) {

	}

	@Override
	default R getResult() {
		return null;
	}

	@Override
	default R merge(
			final R result1,
			final R result2 ) {
		return null;
	}

	@Override
	default byte[] resultToBinary(
			final R result ) {
		return new byte[0];
	}

	@Override
	default R resultFromBinary(
			final byte[] binary ) {
		return null;
	}

	@Override
	default void clearResult() {}

	@Override
	default void aggregate(
			final T entry ) {}
}
