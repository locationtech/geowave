package mil.nga.giat.geowave.analytics.tools.kryo;

import org.geotools.feature.simple.SimpleFeatureImpl;

import com.esotericsoftware.kryo.Kryo;

/**
 * 
 * @formatter:off
 * 
 *                SparkConf sparkConf;
 * 
 *                sparkConf.set( "spark.serializer",
 *                "org.apache.spark.serializer.KryoSerializer");
 * 
 *                sparkConf.set( "spark.kryo.registrator",
 *                GeoWaveKryoRegistrator.class.getName());
 * 
 * @formatter:on
 */
public class GeoWaveKryoRegistrator
{

	public void registerClasses(
			final Kryo kryo ) {
		kryo.register(
				SimpleFeatureImpl.class,
				new FeatureSerializer());
	}

}
