package org.locationtech.geowave.analytic.spark;

import java.io.Serializable;
import org.apache.spark.api.java.JavaPairRDD;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveRasterRDD  implements Serializable {
  /**
  *
  */
 private static final long serialVersionUID = 1L;
 private JavaPairRDD<GeoWaveInputKey, GridCoverage> rawRDD = null;

 public GeoWaveRasterRDD() {}

 public GeoWaveRasterRDD(final JavaPairRDD<GeoWaveInputKey, GridCoverage> rawRDD) {
   this.rawRDD = rawRDD;
 }

 public JavaPairRDD<GeoWaveInputKey, GridCoverage> getRawRDD() {
   return rawRDD;
 }

 public void setRawRDD(final JavaPairRDD<GeoWaveInputKey, GridCoverage> rawRDD) {
   this.rawRDD = rawRDD;
 }

 public boolean isLoaded() {
   return (getRawRDD() != null);
 }
}
