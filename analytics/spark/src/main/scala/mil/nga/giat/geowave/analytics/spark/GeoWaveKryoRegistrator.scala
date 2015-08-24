package mil.nga.giat.geowave.analytics.spark

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer
import org.opengis.feature.simple.SimpleFeature
import org.geotools.feature.simple.SimpleFeatureImpl

class GeoWaveKryoRegistrator extends KryoRegistrator {
	  override def registerClasses(kryo: Kryo) {
	    kryo.register(classOf[SimpleFeatureImpl], new FeatureSerializer )
	  }
}