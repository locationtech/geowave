package mil.nga.giat.geowave.analytic.spark.spatial;

import org.apache.spark.api.java.JavaPairRDD;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public abstract class JoinStrategy implements
		SpatialJoin
{
	// Final joined pair RDDs
	protected JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined = null;
	protected JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined = null;

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getLeftResults() {
		return leftJoined;
	}

	public void setLeftResults(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> leftJoined ) {
		this.leftJoined = leftJoined;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRightResults() {
		return rightJoined;
	}

	public void setRightResults(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rightJoined ) {
		this.rightJoined = rightJoined;
	}

}
