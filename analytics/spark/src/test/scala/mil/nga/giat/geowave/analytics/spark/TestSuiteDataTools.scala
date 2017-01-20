package mil.nga.giat.geowave.analytics.spark

import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import com.vividsolutions.jts.geom.GeometryFactory
import scala.util.Random
import com.vividsolutions.jts.geom.Coordinate
import org.opengis.feature.`type`.GeometryType
import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.util.Date
import java.util.UUID
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.`type`.AttributeType
import mil.nga.giat.geowave.core.index.ByteArrayId
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey
import mil.nga.giat.geowave.analytic.distance.CoordinateCircleDistanceFn

class TestSuiteDataTools(val name: String, val definition: String) {
  val featureType = DataUtilities.createType(name, definition)
  val builder = new SimpleFeatureBuilder(featureType);
  val factory = new GeometryFactory();
  val random = new Random(23231);
  val adapterId = new ByteArrayId(name);
  val distanceFn = new FeatureCentroidDistanceFn(new CoordinateCircleDistanceFn)
  
  private def createForType(x: AttributeType): Object = x match {
    case gt: GeometryType => factory.createPoint(new Coordinate(random.nextDouble * 180, random.nextDouble * 90))
    case tt: Timestamp => new Timestamp(random.nextLong)
    case dt: Date => new Date(random.nextLong)
    case _ => UUID.randomUUID().toString()
  }

  private def createOne = {
    val values = builder.getFeatureType().getAttributeDescriptors().asScala.map(d =>
      createForType(d.getType))
    builder.buildFeature(UUID.randomUUID().toString(), values.toArray)
  }

  def create(count: Int): Seq[(GeoWaveInputKey, SimpleFeature)] = {
    (1 to count by 1).map {
      case num => {
        val feature = createOne
        (new GeoWaveInputKey(adapterId, new ByteArrayId(feature.getID)), feature)
      }
    }
  }
}
object TestSuiteDataTools {

}