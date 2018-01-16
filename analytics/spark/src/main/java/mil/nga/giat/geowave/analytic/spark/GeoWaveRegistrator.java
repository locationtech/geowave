package mil.nga.giat.geowave.analytic.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;

import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer;

public class GeoWaveRegistrator implements
		KryoRegistrator
{
	public void registerClasses(
			Kryo kryo ) {
		// Use existing FeatureSerializer code to serialize SimpleFeature
		// classes
		FeatureSerializer simpleSerializer = new FeatureSerializer();

		kryo.register(
				SimpleFeature.class,
				simpleSerializer);
		kryo.register(
				SimpleFeatureImpl.class,
				simpleSerializer);
		kryo.register(
				AvroSimpleFeature.class,
				simpleSerializer);
	}
}