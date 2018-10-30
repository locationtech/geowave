package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.aggregate.AdapterAndIndexBasedAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class BaseOptimalVectorAggregation<P extends Persistable, R, T> implements
		AdapterAndIndexBasedAggregation<P, R, T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseOptimalVectorAggregation.class);

	protected FieldNameParam fieldNameParam;

	public BaseOptimalVectorAggregation() {}

	public BaseOptimalVectorAggregation(
			final FieldNameParam fieldNameParam ) {
		this.fieldNameParam = fieldNameParam;
	}

	@Override
	public P getParameters() {
		return (P) fieldNameParam;
	}

	@Override
	public void setParameters(
			final P parameters ) {
		if (parameters instanceof FieldNameParam) {
			fieldNameParam = (FieldNameParam) parameters;
		}
	}

	@Override
	public Aggregation<P, R, T> createAggregation(
			final DataTypeAdapter<T> adapter,
			final Index index ) {
		GeotoolsFeatureDataAdapter gtAdapter;
		if (adapter instanceof GeotoolsFeatureDataAdapter) {
			gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
		}
		else if ((adapter instanceof InternalDataAdapter)
				&& (((InternalDataAdapter) adapter).getAdapter() instanceof GeotoolsFeatureDataAdapter)) {
			gtAdapter = (GeotoolsFeatureDataAdapter) ((InternalDataAdapter) adapter).getAdapter();
		}
		else {
			LOGGER.error("Unable to perform aggregation on non-geotools feature adapter '" + adapter.getTypeName()
					+ "'");
			return null;
		}
		if ((fieldNameParam == null) || isCommonIndex(
				index,
				gtAdapter)) {
			return createCommonIndexAggregation();
		}

		return createAggregation();
	}

	abstract protected boolean isCommonIndex(
			Index index,
			GeotoolsFeatureDataAdapter adapter );

	abstract protected Aggregation<P, R, T> createCommonIndexAggregation();

	abstract protected Aggregation<P, R, T> createAggregation();
}
