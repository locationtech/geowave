package mil.nga.giat.geowave.analytic.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;

import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeature;
import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer;
import mil.nga.giat.geowave.analytic.kryo.PersistableSerializer;
import mil.nga.giat.geowave.core.index.persist.PersistableFactory;

public class GeoWaveRegistrator implements
		KryoRegistrator
{
	public void registerClasses(
			Kryo kryo ) {
		// Use existing FeatureSerializer code to serialize SimpleFeature
		// classes
		FeatureSerializer simpleSerializer = new FeatureSerializer();
		PersistableSerializer persistSerializer = new PersistableSerializer();
		
		PersistableFactory.getInstance().getClassIdMapping().entrySet().forEach(e -> kryo.register(e.getKey(), persistSerializer, e.getValue()));

		kryo.register(
				SimpleFeatureImpl.class,
				simpleSerializer);
	}
}