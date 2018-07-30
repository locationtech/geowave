package mil.nga.giat.geowave.analytic.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;

import com.esotericsoftware.kryo.Kryo;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;

import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer;
import mil.nga.giat.geowave.analytic.kryo.PersistableSerializer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.PersistableFactory;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

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

		kryo.register(GeoWaveRDD.class);
		kryo.register(GeoWaveIndexedRDD.class);
		kryo.register(Geometry.class);
		kryo.register(PreparedGeometry.class);
		kryo.register(ByteArrayId.class);
		kryo.register(GeoWaveInputKey.class);
		kryo.register(
				SimpleFeatureImpl.class,
				simpleSerializer);
	}
}