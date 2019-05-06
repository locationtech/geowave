package org.locationtech.geowave.analytic.spark.kde;

import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.analytic.mapreduce.kde.CellCounter;
import org.locationtech.geowave.analytic.mapreduce.kde.GaussianFilter;
import org.locationtech.geowave.analytic.mapreduce.kde.GaussianFilter.ValueRange;
import org.locationtech.geowave.analytic.mapreduce.kde.KDEReducer;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveRasterRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.analytic.spark.kmeans.KMeansRunner;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.Tuple2;

public class KDERunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(KDERunner.class);
  private static final double WEIGHT_EPSILON = 2.22E-14;

  public static final int NUM_BANDS = 3;
  protected static final String[] NAME_PER_BAND =
      new String[] {"Weight", "Normalized", "Percentile"};

  protected static final double[] MINS_PER_BAND = new double[] {0, 0, 0};
  protected static final double[] MAXES_PER_BAND = new double[] {Double.MAX_VALUE, 1, 1};
  private String appName = "KDERunner";
  private String master = "yarn";
  private String host = "localhost";

  private JavaSparkContext jsc = null;
  private SparkSession session = null;
  private DataStorePluginOptions inputDataStore = null;

  private DataStorePluginOptions outputDataStore = null;

  private String cqlFilter = null;
  private String typeName = null;
  private String indexName = null;
  private int minLevel = 5;
  private int maxLevel = 20;
  private int tileSize = 1;
  private String coverageName = "kde";

  private int minSplits = -1;
  private int maxSplits = -1;

  public KDERunner() {}

  private void initContext() {
    if (session == null) {
      String jar = "";
      try {
        jar =
            KMeansRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
      } catch (final URISyntaxException e) {
        LOGGER.error("Unable to set jar location in spark configuration", e);
      }

      session = GeoWaveSparkConf.createSessionFromParams(appName, master, host, jar);

      jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
    }
  }

  public void close() {
    if (session != null) {
      session.close();
      session = null;
    }
  }

  public void setTileSize(final int tileSize) {
    this.tileSize = tileSize;
  }

  public void run() throws IOException {
    initContext();

    // Validate inputs
    if (inputDataStore == null) {
      LOGGER.error("You must supply an input datastore!");
      throw new IOException("You must supply an input datastore!");
    }


    // Retrieve the feature adapters
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    List<String> featureTypeNames;

    // If provided, just use the one
    if (typeName != null) {
      featureTypeNames = new ArrayList<>();
      featureTypeNames.add(typeName);
    } else { // otherwise, grab all the feature adapters
      featureTypeNames = FeatureDataUtils.getFeatureTypeNames(inputDataStore);
    }
    bldr.setTypeNames(featureTypeNames.toArray(new String[0]));

    // This is required due to some funkiness in GeoWaveInputFormat
    final PersistentAdapterStore adapterStore = inputDataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = inputDataStore.createInternalAdapterStore();

    // TODO remove this, but in case there is trouble this is here for
    // reference temporarily
    // queryOptions.getAdaptersArray(adapterStore);

    // Add a spatial filter if requested
    try {
      if (cqlFilter != null) {
        Geometry bbox = null;
        String cqlTypeName;
        if (typeName == null) {
          cqlTypeName = featureTypeNames.get(0);
        } else {
          cqlTypeName = typeName;
        }

        final short adapterId = internalAdapterStore.getAdapterId(cqlTypeName);

        final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId).getAdapter();

        if (adapter instanceof FeatureDataAdapter) {
          final String geometryAttribute =
              ((FeatureDataAdapter) adapter).getFeatureType().getGeometryDescriptor().getLocalName();
          Filter filter;
          filter = ECQL.toFilter(cqlFilter);

          final ExtractGeometryFilterVisitorResult geoAndCompareOpData =
              (ExtractGeometryFilterVisitorResult) filter.accept(
                  new ExtractGeometryFilterVisitor(
                      GeometryUtils.getDefaultCRS(),
                      geometryAttribute),
                  null);
          bbox = geoAndCompareOpData.getGeometry();
        }

        if ((bbox != null) && !bbox.equals(GeometryUtils.infinity())) {
          bldr.constraints(
              bldr.constraintsFactory().spatialTemporalConstraints().spatialConstraints(
                  bbox).build());
        }
      }
    } catch (final CQLException e) {
      LOGGER.error("Unable to parse CQL: " + cqlFilter);
    }

    // Load RDD from datastore
    final RDDOptions kdeOpts = new RDDOptions();
    kdeOpts.setMinSplits(minSplits);
    kdeOpts.setMaxSplits(maxSplits);
    kdeOpts.setQuery(bldr.build());
    final Function<Double, Double> identity = x -> x;

    final Function2<Double, Double, Double> sum = (final Double x, final Double y) -> {
      return x + y;
    };

    final SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
    final SpatialOptions so = sdp.createOptions();

    final Index outputPrimaryIndex = sdp.createIndex(so);

    final DataTypeAdapter<?> adapter =
        RasterUtils.createDataAdapterTypeDouble(
            coverageName,
            KDEReducer.NUM_BANDS,
            tileSize,
            MINS_PER_BAND,
            MAXES_PER_BAND,
            NAME_PER_BAND,
            null);
    outputDataStore.createDataStore().addType(adapter, outputPrimaryIndex);
    for (int level = minLevel; level < maxLevel; level++) {

      final int numXTiles = (int) Math.pow(2, level + 1);
      final int numYTiles = (int) Math.pow(2, level);
      final int numXPosts = numXTiles * tileSize;
      final int numYPosts = numYTiles * tileSize;
      final GeoWaveRDD kdeRDD =
          GeoWaveRDDLoader.loadRDD(session.sparkContext(), inputDataStore, kdeOpts);
      // kdeRDD.getRawRDD().cache().aggregate(new Envelope(), (e, p) -> {
      // if (e.isNull()) {
      //
      // }
      // return e;
      // }, (e1, e2) -> {
      // return e1;
      // });
      final JavaPairRDD<Double, Long> cells =
          kdeRDD.getRawRDD().flatMapToPair(
              new GeoWaveCellMapper(numXPosts, numYPosts)).combineByKey(
                  identity,
                  sum,
                  sum).mapToPair(new MyFunction()).sortByKey(false).repartition(1).cache();
      final double max = cells.first()._1;
      final long count = cells.count();
      final JavaPairRDD<GeoWaveInputKey, GridCoverage> rdd =
          cells.zipWithIndex().mapToPair(
              new MyFunction2(
                  max,
                  count,
                  numXPosts,
                  numYPosts,
                  numXTiles,
                  numYTiles,
                  numYTiles,
                  coverageName,
                  indexName));
      LOGGER.debug("Writing results to output store...");
      RDDUtils.writeRasterRDDToGeoWave(
          jsc.sc(),
          outputPrimaryIndex,
          outputDataStore,
          adapter,
          new GeoWaveRasterRDD(rdd));

      LOGGER.debug("Results successfully written!");
    }

  }

  private static Envelope getEnvelope(final SimpleFeature entry) {
    final Object o = entry.getDefaultGeometry();

    if ((o != null) && (o instanceof Geometry)) {
      final Geometry geometry = (Geometry) o;
      if (!geometry.isEmpty()) {
        return geometry.getEnvelopeInternal();
      }
    }
    return null;
  }


  public DataStorePluginOptions getInputDataStore() {
    return inputDataStore;
  }

  public void setInputDataStore(final DataStorePluginOptions inputDataStore) {
    this.inputDataStore = inputDataStore;
  }

  public DataStorePluginOptions getOutputDataStore() {
    return outputDataStore;
  }

  public void setOutputDataStore(final DataStorePluginOptions outputDataStore) {
    this.outputDataStore = outputDataStore;
  }

  public void setSparkSession(final SparkSession ss) {
    session = ss;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public void setMinLevel(final int minLevel) {
    this.minLevel = minLevel;
  }

  public void setMaxLevel(final int maxLevel) {
    this.maxLevel = maxLevel;
  }

  public void setMaster(final String master) {
    this.master = master;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public void setCqlFilter(final String cqlFilter) {
    this.cqlFilter = cqlFilter;
  }

  public void setTypeName(final String typeName) {
    this.typeName = typeName;
  }

  public void setCoverageName(final String coverageName) {
    this.coverageName = coverageName;
  }

  public void setSplits(final int min, final int max) {
    minSplits = min;
    maxSplits = max;
  }

  protected static class GeoWaveCellMapper implements
      org.apache.spark.api.java.function.PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, Long, Double> {
    /**
    *
    */
    private static final long serialVersionUID = 1L;
    private final int numXPosts;
    private final int numYPosts;

    public GeoWaveCellMapper(final int numXPosts, final int numYPosts) {
      this.numXPosts = numXPosts;
      this.numYPosts = numYPosts;
    }

    @Override
    public Iterator<Tuple2<Long, Double>> call(final Tuple2<GeoWaveInputKey, SimpleFeature> t)
        throws Exception {
      return getCells(t._2, numXPosts, numYPosts);
    }
  }

  public static Iterator<Tuple2<Long, Double>> getCells(
      final SimpleFeature s,
      final int numXPosts,
      final int numYPosts) {

    final List<Tuple2<Long, Double>> cells = new ArrayList<>();

    Point pt = null;
    if (s != null) {
      final Object geomObj = s.getDefaultGeometry();
      if ((geomObj != null) && (geomObj instanceof Geometry)) {
        pt = ((Geometry) geomObj).getCentroid();
        GaussianFilter.incrementPtFast(pt.getY(), pt.getX(), new CellCounter() {

          @Override
          public void increment(final long cellId, final double weight) {
            cells.add(new Tuple2<>(cellId, weight));

          }
        }, numXPosts, numYPosts);
      }
    }
    return cells.iterator();
  }

  private static final class TileInfo {
    private final double tileWestLon;
    private final double tileEastLon;
    private final double tileSouthLat;
    private final double tileNorthLat;
    private final int x;
    private final int y;

    public TileInfo(
        final double tileWestLon,
        final double tileEastLon,
        final double tileSouthLat,
        final double tileNorthLat,
        final int x,
        final int y) {
      this.tileWestLon = tileWestLon;
      this.tileEastLon = tileEastLon;
      this.tileSouthLat = tileSouthLat;
      this.tileNorthLat = tileNorthLat;
      this.x = x;
      this.y = y;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      long temp;
      temp = Double.doubleToLongBits(tileEastLon);
      result = (prime * result) + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(tileNorthLat);
      result = (prime * result) + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(tileSouthLat);
      result = (prime * result) + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(tileWestLon);
      result = (prime * result) + (int) (temp ^ (temp >>> 32));
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final TileInfo other = (TileInfo) obj;
      if (Double.doubleToLongBits(tileEastLon) != Double.doubleToLongBits(other.tileEastLon)) {
        return false;
      }
      if (Double.doubleToLongBits(tileNorthLat) != Double.doubleToLongBits(other.tileNorthLat)) {
        return false;
      }
      if (Double.doubleToLongBits(tileSouthLat) != Double.doubleToLongBits(other.tileSouthLat)) {
        return false;
      }
      if (Double.doubleToLongBits(tileWestLon) != Double.doubleToLongBits(other.tileWestLon)) {
        return false;
      }
      return true;
    }
  }

  private static class MyFunction implements
      PairFunction<Tuple2<Long, Double>, Double, Long>,
      Serializable {
    /**
    *
    */
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Double, Long> call(final Tuple2<Long, Double> item) throws Exception {
      return item.swap();
    }

  }
  private static class MyFunction2 implements
      PairFunction<Tuple2<Tuple2<Double, Long>, Long>, GeoWaveInputKey, GridCoverage>,
      Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final double max;
    private final long count;
    private final int numXPosts;
    private final int numYPosts;
    private final int numXTiles;
    private final int numYTiles;
    private final String coverageName;
    private final String indexName;
    private final int tileSize;

    public MyFunction2(
        final double max,
        final long count,
        final int numXPosts,
        final int numYPosts,
        final int numXTiles,
        final int numYTiles,
        final int tileSize,
        final String coverageName,
        final String indexName) {
      super();
      this.max = max;
      this.count = count;
      this.numXPosts = numXPosts;
      this.numYPosts = numYPosts;
      this.numXTiles = numXTiles;
      this.numYTiles = numYTiles;
      this.tileSize = tileSize;
      this.coverageName = coverageName;
      this.indexName = indexName;
    }

    private static final ValueRange[] valueRangePerDimension =
        new ValueRange[] {new ValueRange(-180, 180), new ValueRange(-90, 90)};

    @SuppressFBWarnings(
        value = "INT_BAD_REM_BY_1",
        justification = "The calculation is appropriate if we ever want to vary to tile size.")
    private TileInfo fromCellIndexToTileInfo(
        final long index,
        final int numXPosts,
        final int numYPosts,
        final int numXTiles,
        final int numYTiles,
        final int tileSize) {
      final int xPost = (int) (index / numYPosts);
      final int yPost = (int) (index % numYPosts);
      final int xTile = xPost / tileSize;
      final int yTile = yPost / tileSize;
      final int x = (xPost % tileSize);
      final int y = (yPost % tileSize);
      final double xMin = valueRangePerDimension[0].getMin();
      final double xMax = valueRangePerDimension[0].getMax();
      final double yMin = valueRangePerDimension[1].getMin();
      final double yMax = valueRangePerDimension[1].getMax();
      final double crsWidth = xMax - xMin;
      final double crsHeight = yMax - yMin;
      final double tileWestLon = ((xTile * crsWidth) / numXTiles) + xMin;
      final double tileSouthLat = ((yTile * crsHeight) / numYTiles) + yMin;
      final double tileEastLon = tileWestLon + (crsWidth / numXTiles);
      final double tileNorthLat = tileSouthLat + (crsHeight / numYTiles);
      return new TileInfo(
          tileWestLon,
          tileEastLon,
          tileSouthLat,
          tileNorthLat,
          x,
          tileSize - y - 1); // remember
                             // java
                             // rasters
                             // go
      // from 0 at the
      // top
      // to (height-1) at the bottom, so we have
      // to
      // inverse the y here which goes from bottom
      // to top
    }

    @Override
    public Tuple2<GeoWaveInputKey, GridCoverage> call(final Tuple2<Tuple2<Double, Long>, Long> t)
        throws Exception {
      final TileInfo tileInfo =
          fromCellIndexToTileInfo(t._1._2, numXPosts, numYPosts, numXTiles, numYTiles, tileSize);
      final WritableRaster raster = RasterUtils.createRasterTypeDouble(NUM_BANDS, tileSize);

      final double normalizedValue = t._1._1 / max;
      // for consistency give all cells with matching weight the same
      // percentile
      // because we are using a DoubleWritable as the key, the ordering
      // isn't always completely reproducible as Double equals does not
      // take into account an epsilon, but we can make it reproducible by
      // doing a comparison with the previous value using an appropriate
      // epsilon
      final double percentile = (count - 1 - t._2) / ((double) count - 1);
      raster.setSample(tileInfo.x, tileInfo.y, 0, t._1._1);
      raster.setSample(tileInfo.x, tileInfo.y, 1, normalizedValue);

      raster.setSample(tileInfo.x, tileInfo.y, 2, percentile);
      return new Tuple2(
          new GeoWaveOutputKey(coverageName, indexName),
          RasterUtils.createCoverageTypeDouble(
              coverageName,
              tileInfo.tileWestLon,
              tileInfo.tileEastLon,
              tileInfo.tileSouthLat,
              tileInfo.tileNorthLat,
              MINS_PER_BAND,
              MAXES_PER_BAND,
              NAME_PER_BAND,
              raster,
              GeometryUtils.DEFAULT_CRS_STR));
    }

  }
}
