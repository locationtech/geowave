/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.locationtech.geowave.service.AnalyticService;

public class AnalyticServiceClient {
  private final AnalyticService analyticService;

  public AnalyticServiceClient(final String baseUrl) {
    this(baseUrl, null, null);
  }

  public AnalyticServiceClient(final String baseUrl, final String user, final String password) {

    analyticService =
        WebResourceFactory.newResource(
            AnalyticService.class,
            ClientBuilder.newClient().register(MultiPartFeature.class).target(baseUrl));
  }

  public Response dbScan(
      final String storeName,
      final String mapReduceHdfsBaseDir,
      final String extractMaxInputSplit,
      final String extractMinInputSplit,
      final String adapterIds, // Array
      // of
      // strings
      final String clusteringMaxIterations,
      final String clusteringMinimumSize,
      final String partitionMaxDistance,
      final String mapReduceConfigFile,
      final String mapReduceHdfsHostPort,
      final String mapReduceJobtrackerHostPort,
      final String mapReduceYarnResourceManager,
      final String commonDistanceFunctionClass,
      final String extractQuery,
      final String outputOutputFormat,
      final String inputFormatClass,
      final String inputHdfsPath,
      final String outputReducerCount,
      final String authorizations, // Array
      // of
      // strings
      final String indexId,
      final String outputHdfsOutputPath,
      final String partitioningDistanceThresholds,
      final String partitioningGeometricDistanceUnit,
      final String globalBatchId,
      final String hullDataTypeId,
      final String hullProjectionClass,
      final String outputDataNamespaceUri,
      final String outputDataTypeId,
      final String outputIndexId,
      final String partitionMaxMemberSelection,
      final String partitionPartitionerClass,
      final String partitionPartitionDecreaseRate,
      final String partitionPartitionPrecision,
      final String partitionSecondaryPartitionerClass) {

    final Response resp =
        analyticService.dbScan(
            storeName,
            mapReduceHdfsBaseDir,
            extractMaxInputSplit,
            extractMinInputSplit,
            adapterIds, // Array
                        // of
                        // strings
            clusteringMaxIterations,
            clusteringMinimumSize,
            partitionMaxDistance,
            mapReduceConfigFile,
            mapReduceHdfsHostPort,
            mapReduceJobtrackerHostPort,
            mapReduceYarnResourceManager,
            commonDistanceFunctionClass,
            extractQuery,
            outputOutputFormat,
            inputFormatClass,
            inputHdfsPath,
            outputReducerCount,
            authorizations, // Array
                            // of
                            // strings
            indexId,
            outputHdfsOutputPath,
            partitioningDistanceThresholds,
            partitioningGeometricDistanceUnit,
            globalBatchId,
            hullDataTypeId,
            hullProjectionClass,
            outputDataNamespaceUri,
            outputDataTypeId,
            outputIndexId,
            partitionMaxMemberSelection,
            partitionPartitionerClass,
            partitionPartitionDecreaseRate,
            partitionPartitionPrecision,
            partitionSecondaryPartitionerClass);
    return resp;
  }

  public Response dbScan(
      final String storeName,
      final String mapReduceHdfsBaseDir,
      final String extractMaxInputSplit,
      final String extractMinInputSplit,
      final String adapterIds, // Array
      // of
      // strings
      final String clusteringMaxIterations,
      final String clusteringMinimumSize,
      final String partitionMaxDistance) {

    return dbScan(
        storeName,
        mapReduceHdfsBaseDir,
        extractMaxInputSplit,
        extractMinInputSplit,
        adapterIds,
        clusteringMaxIterations,
        clusteringMinimumSize,
        partitionMaxDistance,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public Response kde(
      final String inputStoreName,
      final String outputStoreName,
      final String featuretype,
      final Integer minLevel,
      final Integer maxLevel,
      final String coverageName,
      final String jobTrackerOrResourceManHostPort,
      final String indexId,
      final Integer minSplits,
      final Integer maxSplits,
      final String hdfsHostPort,
      final Integer tileSize,
      final String cqlFilter) {

    final Response resp =
        analyticService.kde(
            inputStoreName,
            outputStoreName,
            featuretype,
            minLevel,
            maxLevel,
            coverageName,
            jobTrackerOrResourceManHostPort,
            indexId,
            minSplits,
            maxSplits,
            hdfsHostPort,
            tileSize,
            cqlFilter);
    return resp;
  }

  public Response kde(
      final String inputStoreName,
      final String outputStoreName,
      final String featuretype,
      final Integer minLevel,
      final Integer maxLevel,
      final String coverageName,
      final String jobTrackerOrResourceManHostPort) {

    return kde(
        inputStoreName,
        outputStoreName,
        featuretype,
        minLevel,
        maxLevel,
        coverageName,
        jobTrackerOrResourceManHostPort,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public Response kmeansSpark(
      final String inputStoreName,
      final String outputStoreName,
      final String appName,
      final String host,
      final String master,
      final Integer numClusters,
      final Integer numIterations,
      final String epsilon,
      final Boolean useTime,
      final Boolean generateHulls,
      final Boolean computeHullData,
      final String cqlFilter,
      final String adapterId,
      final Integer minSplits,
      final Integer maxSplits,
      final String centroidTypeName,
      final String hullTypeName) {

    final Response resp =
        analyticService.kmeansSpark(
            inputStoreName,
            outputStoreName,
            appName,
            host,
            master,
            numClusters,
            numIterations,
            epsilon,
            useTime,
            generateHulls,
            computeHullData,
            cqlFilter,
            adapterId,
            minSplits,
            maxSplits,
            centroidTypeName,
            hullTypeName);
    return resp;
  }

  public Response kmeansSpark(final String inputStoreName, final String outputStoreName) {

    return kmeansSpark(
        inputStoreName,
        outputStoreName,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public Response nearestNeighbor(
      final String storeName,
      final String mapReduceHdfsBaseDir,
      final String extractMaxInputSplit,
      final String extractMinInputSplit,
      final String adapterIds, // Array
      // of
      // strings
      final String outputHdfsOutputPath,
      final String partitionMaxDistance,
      final String mapReduceConfigFile,
      final String mapReduceHdfsHostPort,
      final String mapReduceJobtrackerHostPort,
      final String mapReduceYarnResourceManager,
      final String commonDistanceFunctionClass,
      final String extractQuery,
      final String outputOutputFormat,
      final String inputFormatClass,
      final String inputHdfsPath,
      final String outputReducerCount,
      final String authorizations, // Array of strings
      final String indexId,
      final String partitionMaxMemberSelection,
      final String partitionPartitionerClass,
      final String partitionPartitionPrecision,
      final String partitioningDistanceThresholds,
      final String partitioningGeometricDistanceUnit,
      final String partitionSecondaryPartitionerClass) {

    final Response resp =
        analyticService.nearestNeighbor(
            storeName,
            mapReduceHdfsBaseDir,
            extractMaxInputSplit,
            extractMinInputSplit,
            adapterIds, // Array
                        // of
                        // strings
            outputHdfsOutputPath,
            partitionMaxDistance,
            mapReduceConfigFile,
            mapReduceHdfsHostPort,
            mapReduceJobtrackerHostPort,
            mapReduceYarnResourceManager,
            commonDistanceFunctionClass,
            extractQuery,
            outputOutputFormat,
            inputFormatClass,
            inputHdfsPath,
            outputReducerCount,
            authorizations, // Array of strings
            indexId,
            partitionMaxMemberSelection,
            partitionPartitionerClass,
            partitionPartitionPrecision,
            partitioningDistanceThresholds,
            partitioningGeometricDistanceUnit,
            partitionSecondaryPartitionerClass);
    return resp;
  }

  public Response nearestNeighbor(
      final String storeName,
      final String mapReduceHdfsBaseDir,
      final String extractMaxInputSplit,
      final String extractMinInputSplit,
      final String adapterIds, // Array
      // of
      // strings
      final String outputHdfsOutputPath,
      final String partitionMaxDistance) {
    return nearestNeighbor(
        storeName,
        mapReduceHdfsBaseDir,
        extractMaxInputSplit,
        extractMinInputSplit,
        adapterIds,
        outputHdfsOutputPath,
        partitionMaxDistance,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public Response sql(
      final String parameters, // Array of strings
      final String csvOutputFile,
      final String outputStoreName,
      final String outputTypeName,
      final Integer showResults) {

    final Response resp =
        analyticService.sql(
            parameters, // Array of strings
            csvOutputFile,
            outputStoreName,
            outputTypeName,
            showResults);
    return resp;
  }
}
