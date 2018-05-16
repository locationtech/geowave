package mil.nga.giat.geowave.analytic.spark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class GeoWaveRDD implements
		Serializable
{
	private JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD = null;

	public GeoWaveRDD() {}

	public GeoWaveRDD(
			final JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD ) {
		this.rawRDD = rawRDD;
	}

	public JavaPairRDD<GeoWaveInputKey, SimpleFeature> getRawRDD() {
		return rawRDD;
	}

	public void setRawRDD(
			JavaPairRDD<GeoWaveInputKey, SimpleFeature> rawRDD ) {
		this.rawRDD = rawRDD;
	}

	public boolean isLoaded() {
		return (getRawRDD() != null);
	}

}
