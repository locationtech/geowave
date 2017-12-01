package mil.nga.giat.geowave.analytic.spark.spatial;

import mil.nga.giat.geowave.analytic.spark.sparksql.udf.GeomFunction;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

public class SpatialJoinOptions
{

	private DataStorePluginOptions leftStore = null;
	private DataStorePluginOptions rightStore = null;

	// TODO: Experiment with collecting + broadcasting rdds when one side can
	// fit into memory
	private boolean leftBroadcast = false;
	private boolean rightBroadcast = false;

	private GeomFunction predicate = null;
	private NumericIndexStrategy indexStrategy = null;

	public SpatialJoinOptions() {}

	public DataStorePluginOptions getLeftStore() {
		return leftStore;
	}

	public void setLeftStore(
			DataStorePluginOptions leftStore ) {
		this.leftStore = leftStore;
	}

	public DataStorePluginOptions getRightStore() {
		return rightStore;
	}

	public void setRightStore(
			DataStorePluginOptions rightStore ) {
		this.rightStore = rightStore;
	}

	public GeomFunction getPredicate() {
		return predicate;
	}

	public void setPredicate(
			GeomFunction predicate ) {
		this.predicate = predicate;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public void setIndexStrategy(
			NumericIndexStrategy indexStrategy ) {
		this.indexStrategy = indexStrategy;
	}

}
