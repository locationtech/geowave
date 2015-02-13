package mil.nga.giat.geowave.analytics.tools.kryo;

import org.apache.spark.SparkConf;
import org.geotools.feature.simple.SimpleFeatureImpl;

import com.esotericsoftware.kryo.Kryo;

public class GeoWaveKryoRegistrator
{
	public static final void setupSerializers(
			SparkConf sparkConf ) {
		sparkConf.set(
				"spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		sparkConf.set(
				"spark.kryo.registrator",
				GeoWaveKryoRegistrator.class.getName());
	}

	public void registerClasses(
			Kryo kryo ) {
		kryo.register(
				SimpleFeatureImpl.class,
				new FeatureSerializer());
	}
	
}
