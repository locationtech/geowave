/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark.kmeans;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ScaledTemporalRange;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.jts.geom.Geometry;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;

public class KMeansRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansRunner.class);

  private String appName = "KMeansRunner";
  private String master = "yarn";
  private String host = "localhost";

  private JavaSparkContext jsc = null;
  private SparkSession session = null;
  private DataStorePluginOptions inputDataStore = null;

  private DataStorePluginOptions outputDataStore = null;
  private String centroidTypeName = "kmeans_centroids";
  private String hullTypeName = "kmeans_hulls";

  private JavaRDD<Vector> centroidVectors;
  private KMeansModel outputModel;

  private int numClusters = 8;
  private int numIterations = 20;
  private double epsilon = -1.0;
  private String cqlFilter = null;
  private String typeName = null;
  private String timeField = null;
  private ScaledTemporalRange scaledTimeRange = null;
  private ScaledTemporalRange scaledRange = null;
  private int minSplits = -1;
  private int maxSplits = -1;
  private Boolean useTime = false;
  private Boolean generateHulls = false;
  private Boolean computeHullData = false;

  public KMeansRunner() {}

  private void initContext() {
    if (session == null) {
      String jar = "";
      try {
        jar =
            KMeansRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
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

  public void run() throws IOException {
    initContext();

    // Validate inputs
    if (inputDataStore == null) {
      LOGGER.error("You must supply an input datastore!");
      throw new IOException("You must supply an input datastore!");
    }

    if (isUseTime()) {

      scaledRange = KMeansUtils.setRunnerTimeParams(this, inputDataStore, typeName);

      if (scaledRange == null) {
        LOGGER.error("Failed to set time params for kmeans. Please specify a valid feature type.");
        throw new ParameterException("--useTime option: Failed to set time params");
      }
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

        if (adapter instanceof GeotoolsFeatureDataAdapter) {
          final String geometryAttribute =
              ((GeotoolsFeatureDataAdapter) adapter).getFeatureType().getGeometryDescriptor().getLocalName();
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
    final RDDOptions kmeansOpts = new RDDOptions();
    kmeansOpts.setMinSplits(minSplits);
    kmeansOpts.setMaxSplits(maxSplits);
    kmeansOpts.setQuery(bldr.build());
    final GeoWaveRDD kmeansRDD =
        GeoWaveRDDLoader.loadRDD(session.sparkContext(), inputDataStore, kmeansOpts);

    // Retrieve the input centroids
    LOGGER.debug("Retrieving input centroids from RDD...");
    centroidVectors = RDDUtils.rddFeatureVectors(kmeansRDD, timeField, scaledTimeRange);
    centroidVectors.cache();

    // Init the algorithm
    final KMeans kmeans = new KMeans();
    kmeans.setInitializationMode("kmeans||");
    kmeans.setK(numClusters);
    kmeans.setMaxIterations(numIterations);

    if (epsilon > -1.0) {
      kmeans.setEpsilon(epsilon);
    }

    // Run KMeans
    LOGGER.debug("Running KMeans algorithm...");
    outputModel = kmeans.run(centroidVectors.rdd());

    LOGGER.debug("Writing results to output store...");
    writeToOutputStore();
    LOGGER.debug("Results successfully written!");
  }

  public void writeToOutputStore() {
    if (outputDataStore != null) {
      // output cluster centroids (and hulls) to output datastore
      KMeansUtils.writeClusterCentroids(
          outputModel,
          outputDataStore,
          centroidTypeName,
          scaledRange);

      if (isGenerateHulls()) {
        KMeansUtils.writeClusterHulls(
            centroidVectors,
            outputModel,
            outputDataStore,
            hullTypeName,
            isComputeHullData());
      }
    }
  }

  public Boolean isUseTime() {
    return useTime;
  }

  public void setUseTime(final Boolean useTime) {
    this.useTime = useTime;
  }

  public String getCentroidTypeName() {
    return centroidTypeName;
  }

  public void setCentroidTypeName(final String centroidTypeName) {
    this.centroidTypeName = centroidTypeName;
  }

  public String getHullTypeName() {
    return hullTypeName;
  }

  public void setHullTypeName(final String hullTypeName) {
    this.hullTypeName = hullTypeName;
  }

  public Boolean isGenerateHulls() {
    return generateHulls;
  }

  public void setGenerateHulls(final Boolean generateHulls) {
    this.generateHulls = generateHulls;
  }

  public Boolean isComputeHullData() {
    return computeHullData;
  }

  public void setComputeHullData(final Boolean computeHullData) {
    this.computeHullData = computeHullData;
  }

  public JavaRDD<Vector> getInputCentroids() {
    return centroidVectors;
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

  public void setNumClusters(final int numClusters) {
    this.numClusters = numClusters;
  }

  public void setNumIterations(final int numIterations) {
    this.numIterations = numIterations;
  }

  public void setEpsilon(final Double epsilon) {
    this.epsilon = epsilon;
  }

  public KMeansModel getOutputModel() {
    return outputModel;
  }

  public void setAppName(final String appName) {
    this.appName = appName;
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

  public void setTimeParams(final String timeField, final ScaledTemporalRange timeRange) {
    this.timeField = timeField;
    scaledTimeRange = timeRange;
  }

  public void setSplits(final int min, final int max) {
    minSplits = min;
    maxSplits = max;
  }
}
