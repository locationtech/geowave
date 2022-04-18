/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kde;

import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.adapter.ClientMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.GridCoverageWritable;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.analytic.mapreduce.kde.CellCounter;
import org.locationtech.geowave.analytic.mapreduce.kde.GaussianFilter;
import org.locationtech.geowave.analytic.mapreduce.kde.KDEReducer;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializer;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.cs.CoordinateSystemAxis;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.Tuple2;

public class KDERunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(KDERunner.class);

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
  private Index outputIndex;

  private int minSplits = -1;
  private int maxSplits = -1;

  public KDERunner() {}

  private void initContext() {
    if (session == null) {
      String jar = "";
      try {
        jar = KDERunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        if (!FilenameUtils.isExtension(jar.toLowerCase(), "jar")) {
          jar = "";
        }
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
    if (indexName != null) {
      bldr.indexName(indexName);
    }
    Index inputPrimaryIndex = null;
    final Index[] idxArray = inputDataStore.createDataStore().getIndices();
    for (final Index idx : idxArray) {
      if ((idx != null) && ((indexName == null) || indexName.equals(idx.getName()))) {
        inputPrimaryIndex = idx;
        break;
      }
    }
    final CoordinateReferenceSystem inputIndexCrs = GeometryUtils.getIndexCrs(inputPrimaryIndex);
    final String inputCrsCode = GeometryUtils.getCrsCode(inputIndexCrs);

    Index outputPrimaryIndex = outputIndex;
    CoordinateReferenceSystem outputIndexCrs = null;
    final String outputCrsCode;

    if (outputPrimaryIndex != null) {
      outputIndexCrs = GeometryUtils.getIndexCrs(outputPrimaryIndex);
      outputCrsCode = GeometryUtils.getCrsCode(outputIndexCrs);
    } else {
      final SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
      final SpatialOptions so = sdp.createOptions();
      so.setCrs(inputCrsCode);
      outputPrimaryIndex = SpatialDimensionalityTypeProvider.createIndexFromOptions(so);
      outputIndexCrs = inputIndexCrs;
      outputCrsCode = inputCrsCode;
    }

    final CoordinateSystem cs = outputIndexCrs.getCoordinateSystem();
    final CoordinateSystemAxis csx = cs.getAxis(0);
    final CoordinateSystemAxis csy = cs.getAxis(1);
    final double xMax = csx.getMaximumValue();
    final double xMin = csx.getMinimumValue();
    final double yMax = csy.getMaximumValue();
    final double yMin = csy.getMinimumValue();

    if ((xMax == Double.POSITIVE_INFINITY)
        || (xMin == Double.NEGATIVE_INFINITY)
        || (yMax == Double.POSITIVE_INFINITY)
        || (yMin == Double.NEGATIVE_INFINITY)) {
      LOGGER.error(
          "Raster KDE resize with raster primary index CRS dimensions min/max equal to positive infinity or negative infinity is not supported");
      throw new RuntimeException(
          "Raster KDE resize with raster primary index CRS dimensions min/max equal to positive infinity or negative infinity is not supported");
    }

    if (cqlFilter != null) {
      bldr.constraints(bldr.constraintsFactory().cqlConstraints(cqlFilter));
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

    final RasterDataAdapter adapter =
        RasterUtils.createDataAdapterTypeDouble(
            coverageName,
            KDEReducer.NUM_BANDS,
            tileSize,
            MINS_PER_BAND,
            MAXES_PER_BAND,
            NAME_PER_BAND,
            new NoDataMergeStrategy());
    outputDataStore.createDataStore().addType(adapter, outputPrimaryIndex);

    // The following "inner" variables are created to give access to member
    // variables within lambda
    // expressions
    final int innerTileSize = 1;// tileSize;
    final String innerCoverageName = coverageName;
    for (int level = minLevel; level <= maxLevel; level++) {
      final int numXTiles = (int) Math.pow(2, level + 1);
      final int numYTiles = (int) Math.pow(2, level);
      final int numXPosts = numXTiles; // * tileSize;
      final int numYPosts = numYTiles; // * tileSize;
      final GeoWaveRDD kdeRDD =
          GeoWaveRDDLoader.loadRDD(session.sparkContext(), inputDataStore, kdeOpts);
      JavaPairRDD<Double, Long> cells =
          kdeRDD.getRawRDD().flatMapToPair(
              new GeoWaveCellMapper(
                  numXPosts,
                  numYPosts,
                  xMin,
                  xMax,
                  yMin,
                  yMax,
                  inputCrsCode,
                  outputCrsCode)).combineByKey(identity, sum, sum).mapToPair(item -> item.swap());
      cells =
          cells.partitionBy(
              new RangePartitioner(
                  cells.getNumPartitions(),
                  cells.rdd(),
                  true,
                  scala.math.Ordering.Double$.MODULE$,
                  scala.reflect.ClassTag$.MODULE$.apply(Double.class))).sortByKey(false).cache();
      final long count = cells.count();
      if (count == 0) {
        LOGGER.warn("No cells produced by KDE");
        continue;
      }
      final double max = cells.first()._1;

      JavaRDD<GridCoverage> rdd = cells.zipWithIndex().map(t -> {
        final TileInfo tileInfo =
            fromCellIndexToTileInfo(
                t._1._2,
                numXPosts,
                numYPosts,
                numXTiles,
                numYTiles,
                xMin,
                xMax,
                yMin,
                yMax,
                innerTileSize);
        final WritableRaster raster = RasterUtils.createRasterTypeDouble(NUM_BANDS, innerTileSize);

        final double normalizedValue = t._1._1 / max;
        // because we are using a Double as the key, the ordering
        // isn't always completely reproducible as Double equals does not
        // take into account an epsilon

        final double percentile = (count - t._2) / ((double) count);
        raster.setSample(tileInfo.x, tileInfo.y, 0, t._1._1);
        raster.setSample(tileInfo.x, tileInfo.y, 1, normalizedValue);

        raster.setSample(tileInfo.x, tileInfo.y, 2, percentile);
        return RasterUtils.createCoverageTypeDouble(
            innerCoverageName,
            tileInfo.tileWestLon,
            tileInfo.tileEastLon,
            tileInfo.tileSouthLat,
            tileInfo.tileNorthLat,
            MINS_PER_BAND,
            MAXES_PER_BAND,
            NAME_PER_BAND,
            raster,
            GeometryUtils.DEFAULT_CRS_STR);
      });
      LOGGER.debug("Writing results to output store...");
      if (tileSize > 1) {
        // byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
        // byte[] indexBytes = PersistenceUtils.toBinary(outputPrimaryIndex);
        rdd =
            rdd.flatMapToPair(new TransformTileSize(adapter, outputPrimaryIndex)).groupByKey().map(
                new MergeOverlappingTiles(adapter, outputPrimaryIndex));
      }
      RDDUtils.writeRasterToGeoWave(jsc.sc(), outputPrimaryIndex, outputDataStore, adapter, rdd);

      LOGGER.debug("Results successfully written!");
    }

  }

  private static class PartitionAndSortKey implements Serializable {
    private static final long serialVersionUID = 1L;
    byte[] partitionKey;
    byte[] sortKey;

    public PartitionAndSortKey(final byte[] partitionKey, final byte[] sortKey) {
      super();
      this.partitionKey = partitionKey;
      this.sortKey = sortKey;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + Arrays.hashCode(partitionKey);
      result = (prime * result) + Arrays.hashCode(sortKey);
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
      final PartitionAndSortKey other = (PartitionAndSortKey) obj;
      if (!Arrays.equals(partitionKey, other.partitionKey)) {
        return false;
      }
      if (!Arrays.equals(sortKey, other.sortKey)) {
        return false;
      }
      return true;
    }
  }

  @SuppressFBWarnings(
      value = "INT_BAD_REM_BY_1",
      justification = "The calculation is appropriate if we ever want to vary to tile size.")
  private static TileInfo fromCellIndexToTileInfo(
      final long index,
      final int numXPosts,
      final int numYPosts,
      final int numXTiles,
      final int numYTiles,
      final double xMin,
      final double xMax,
      final double yMin,
      final double yMax,
      final int tileSize) {
    final int xPost = (int) (index / numYPosts);
    final int yPost = (int) (index % numYPosts);
    final int xTile = xPost / tileSize;
    final int yTile = yPost / tileSize;
    final int x = (xPost % tileSize);
    final int y = (yPost % tileSize);
    final double crsWidth = xMax - xMin;
    final double crsHeight = yMax - yMin;
    final double tileWestLon = ((xTile * crsWidth) / numXTiles) + xMin;
    final double tileSouthLat = ((yTile * crsHeight) / numYTiles) + yMin;
    final double tileEastLon = tileWestLon + (crsWidth / numXTiles);
    final double tileNorthLat = tileSouthLat + (crsHeight / numYTiles);
    // remember java rasters go from 0 at the top to (height-1) at the bottom, so we
    // have to inverse
    // the y here which goes from bottom to top
    return new TileInfo(tileWestLon, tileEastLon, tileSouthLat, tileNorthLat, x, tileSize - y - 1);
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

  public void setOutputIndex(final Index outputIndex) {
    this.outputIndex = outputIndex;
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
      PairFlatMapFunction<Tuple2<GeoWaveInputKey, SimpleFeature>, Long, Double> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final int numXPosts;
    private final int numYPosts;
    private final double minX;
    private final double maxX;
    private final double minY;
    private final double maxY;
    private final String inputCrsCode;
    private final String outputCrsCode;
    private MathTransform transform = null;

    protected GeoWaveCellMapper(
        final int numXPosts,
        final int numYPosts,
        final double minX,
        final double maxX,
        final double minY,
        final double maxY,
        final String inputCrsCode,
        final String outputCrsCode) {
      this.numXPosts = numXPosts;
      this.numYPosts = numYPosts;
      this.minX = minX;
      this.maxX = maxX;
      this.minY = minY;
      this.maxY = maxY;
      this.inputCrsCode = inputCrsCode;
      this.outputCrsCode = outputCrsCode;
    }

    @Override
    public Iterator<Tuple2<Long, Double>> call(final Tuple2<GeoWaveInputKey, SimpleFeature> t)
        throws Exception {
      final List<Tuple2<Long, Double>> cells = new ArrayList<>();

      Point pt = null;
      if ((t != null) && (t._2 != null)) {
        final Object geomObj = t._2.getDefaultGeometry();
        if ((geomObj != null) && (geomObj instanceof Geometry)) {
          if (inputCrsCode.equals(outputCrsCode)) {
            pt = ((Geometry) geomObj).getCentroid();
          } else {
            if (transform == null) {

              try {
                transform =
                    CRS.findMathTransform(
                        CRS.decode(inputCrsCode, true),
                        CRS.decode(outputCrsCode, true),
                        true);
              } catch (final FactoryException e) {
                LOGGER.error("Unable to decode " + inputCrsCode + " CRS", e);
                throw new RuntimeException("Unable to initialize " + inputCrsCode + " object", e);
              }
            }

            try {
              final Geometry transformedGeometry = JTS.transform((Geometry) geomObj, transform);
              pt = transformedGeometry.getCentroid();
            } catch (MismatchedDimensionException | TransformException e) {
              LOGGER.warn(
                  "Unable to perform transform to specified CRS of the index, the feature geometry will remain in its original CRS",
                  e);
            }
          }
          GaussianFilter.incrementPtFast(
              pt.getX(),
              pt.getY(),
              minX,
              maxX,
              minY,
              maxY,
              new CellCounter() {
                @Override
                public void increment(final long cellId, final double weight) {
                  cells.add(new Tuple2<>(cellId, weight));

                }
              },
              numXPosts,
              numYPosts);
        }
      }
      return cells.iterator();
    }
  }

  private static class MergeOverlappingTiles implements
      Function<Tuple2<PartitionAndSortKey, Iterable<GridCoverageWritable>>, GridCoverage> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private Index index;
    private RasterDataAdapter newAdapter;
    private HadoopWritableSerializer<GridCoverage, GridCoverageWritable> writableSerializer;

    public MergeOverlappingTiles(final RasterDataAdapter newAdapter, final Index index) {
      super();
      this.index = index;
      this.newAdapter = newAdapter;
      writableSerializer = newAdapter.createWritableSerializer();
    }

    private void readObject(final ObjectInputStream aInputStream)
        throws ClassNotFoundException, IOException {
      final byte[] adapterBytes = new byte[aInputStream.readShort()];
      aInputStream.readFully(adapterBytes);
      final byte[] indexBytes = new byte[aInputStream.readShort()];
      aInputStream.readFully(indexBytes);
      newAdapter = (RasterDataAdapter) PersistenceUtils.fromBinary(adapterBytes);
      index = (Index) PersistenceUtils.fromBinary(indexBytes);
      writableSerializer = newAdapter.createWritableSerializer();
    }

    private void writeObject(final ObjectOutputStream aOutputStream) throws IOException {
      final byte[] adapterBytes = PersistenceUtils.toBinary(newAdapter);
      final byte[] indexBytes = PersistenceUtils.toBinary(index);
      aOutputStream.writeShort(adapterBytes.length);
      aOutputStream.write(adapterBytes);
      aOutputStream.writeShort(indexBytes.length);
      aOutputStream.write(indexBytes);
    }

    @Override
    public GridCoverage call(final Tuple2<PartitionAndSortKey, Iterable<GridCoverageWritable>> v)
        throws Exception {
      GridCoverage mergedCoverage = null;
      ClientMergeableRasterTile<?> mergedTile = null;
      boolean needsMerge = false;
      final Iterator<GridCoverageWritable> it = v._2.iterator();
      while (it.hasNext()) {
        final GridCoverageWritable value = it.next();
        if (mergedCoverage == null) {
          mergedCoverage = writableSerializer.fromWritable(value);
        } else {
          if (!needsMerge) {
            mergedTile = newAdapter.getRasterTileFromCoverage(mergedCoverage);
            needsMerge = true;
          }
          final ClientMergeableRasterTile thisTile =
              newAdapter.getRasterTileFromCoverage(writableSerializer.fromWritable(value));
          if (mergedTile != null) {
            mergedTile.merge(thisTile);
          }
        }
      }
      if (needsMerge) {
        mergedCoverage =
            newAdapter.getCoverageFromRasterTile(
                mergedTile,
                v._1.partitionKey,
                v._1.sortKey,
                index);
      }
      return mergedCoverage;
    }

  }

  private static class TransformTileSize implements
      PairFlatMapFunction<GridCoverage, PartitionAndSortKey, GridCoverageWritable> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private RasterDataAdapter newAdapter;
    private Index index;
    private HadoopWritableSerializer<GridCoverage, GridCoverageWritable> writableSerializer;

    public TransformTileSize(final RasterDataAdapter newAdapter, final Index index) {
      super();
      this.newAdapter = newAdapter;
      this.index = index;
      writableSerializer = newAdapter.createWritableSerializer();
    }

    private void readObject(final ObjectInputStream aInputStream)
        throws ClassNotFoundException, IOException {
      final byte[] adapterBytes = new byte[aInputStream.readShort()];
      aInputStream.readFully(adapterBytes);
      final byte[] indexBytes = new byte[aInputStream.readShort()];
      aInputStream.readFully(indexBytes);
      newAdapter = (RasterDataAdapter) PersistenceUtils.fromBinary(adapterBytes);
      index = (Index) PersistenceUtils.fromBinary(indexBytes);
      writableSerializer = newAdapter.createWritableSerializer();
    }

    private void writeObject(final ObjectOutputStream aOutputStream) throws IOException {
      final byte[] adapterBytes = PersistenceUtils.toBinary(newAdapter);
      final byte[] indexBytes = PersistenceUtils.toBinary(index);
      aOutputStream.writeShort(adapterBytes.length);
      aOutputStream.write(adapterBytes);
      aOutputStream.writeShort(indexBytes.length);
      aOutputStream.write(indexBytes);
    }

    @Override
    public Iterator<Tuple2<PartitionAndSortKey, GridCoverageWritable>> call(
        final GridCoverage existingCoverage) throws Exception {
      final Iterator<GridCoverage> it = newAdapter.convertToIndex(index, existingCoverage);
      return Iterators.transform(
          it,
          g -> new Tuple2<>(
              new PartitionAndSortKey(
                  ((FitToIndexGridCoverage) g).getPartitionKey(),
                  ((FitToIndexGridCoverage) g).getSortKey()),
              writableSerializer.toWritable(((FitToIndexGridCoverage) g).getOriginalCoverage())));
    }

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
}
