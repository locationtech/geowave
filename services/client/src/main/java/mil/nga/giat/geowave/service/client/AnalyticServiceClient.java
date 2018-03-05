package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import mil.nga.giat.geowave.service.AnalyticService;
import mil.nga.giat.geowave.service.RemoteService;

public class AnalyticServiceClient
{
	private final AnalyticService analyticService;

	public AnalyticServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public AnalyticServiceClient(
			final String baseUrl,
			String user,
			String password ) {
		// ClientBuilder bldr = ClientBuilder.newBuilder();
		// if (user != null && password != null) {
		// HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
		// user,
		// password);
		// bldr.register(feature);
		// }
		analyticService = WebResourceFactory.newResource(
				AnalyticService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response dbScan(
			final String storename,
			final String mapReduceHdfsBaseDir,
			final String extractMaxInputSplit,
			final String extractMinInputSplit,
			final String adapterIds, // Array of strings
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
			final String authorizations,// Array of strings
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
			final String partitionSecondaryPartitionerClass ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"store_name",
				storename);
		multiPart.field(
				"mapReduceHdfsBaseDir",
				mapReduceHdfsBaseDir);
		multiPart.field(
				"extractMaxInputSplit",
				extractMaxInputSplit);
		multiPart.field(
				"extractMinInputSplit",
				extractMinInputSplit);
		multiPart.field(
				"adapterIds",
				adapterIds);
		multiPart.field(
				"clusteringMaxIterations",
				clusteringMaxIterations);
		multiPart.field(
				"clusteringMinimumSize",
				clusteringMinimumSize);
		multiPart.field(
				"partitionMaxDistance",
				partitionMaxDistance);
		if (mapReduceConfigFile != null) {
			multiPart.field(
					"mapReduceConfigFile",
					mapReduceConfigFile);
		}
		if (mapReduceHdfsHostPort != null) {
			multiPart.field(
					"mapReduceHdfsHostPort",
					mapReduceHdfsHostPort);
		}
		if (mapReduceJobtrackerHostPort != null) {
			multiPart.field(
					"mapReduceJobtrackerHostPort",
					mapReduceJobtrackerHostPort);
		}
		if (mapReduceYarnResourceManager != null) {
			multiPart.field(
					"mapReduceYarnResourceManager",
					mapReduceYarnResourceManager);
		}
		if (commonDistanceFunctionClass != null) {
			multiPart.field(
					"commonDistanceFunctionClass",
					commonDistanceFunctionClass);
		}
		if (extractQuery != null) {
			multiPart.field(
					"extractQuery",
					extractQuery);
		}
		if (outputOutputFormat != null) {
			multiPart.field(
					"outputOutputFormat",
					outputOutputFormat);
		}
		if (inputFormatClass != null) {
			multiPart.field(
					"inputFormatClass",
					inputFormatClass);
		}
		if (inputHdfsPath != null) {
			multiPart.field(
					"inputHdfsPath",
					inputHdfsPath);
		}
		if (outputReducerCount != null) {
			multiPart.field(
					"outputReducerCount",
					outputReducerCount);
		}
		if (authorizations != null) {
			multiPart.field(
					"authorizations",
					authorizations);
		}
		if (indexId != null) {
			multiPart.field(
					"indexId",
					indexId);
		}
		if (outputHdfsOutputPath != null) {
			multiPart.field(
					"outputHdfsOutputPath",
					outputHdfsOutputPath);
		}
		if (partitioningDistanceThresholds != null) {
			multiPart.field(
					"partitioningDistanceThresholds",
					partitioningDistanceThresholds);
		}
		if (partitioningGeometricDistanceUnit != null) {
			multiPart.field(
					"partitioningGeometricDistanceUnit",
					partitioningGeometricDistanceUnit);
		}
		if (globalBatchId != null) {
			multiPart.field(
					"globalBatchId",
					globalBatchId);
		}
		if (hullDataTypeId != null) {
			multiPart.field(
					"hullDataTypeId",
					hullDataTypeId);
		}
		if (hullProjectionClass != null) {
			multiPart.field(
					"hullProjectionClass",
					hullProjectionClass);
		}
		if (outputDataNamespaceUri != null) {
			multiPart.field(
					"outputDataNamespaceUri",
					outputDataNamespaceUri);
		}
		if (outputDataTypeId != null) {
			multiPart.field(
					"outputDataTypeId",
					outputDataTypeId);
		}
		if (outputIndexId != null) {
			multiPart.field(
					"outputIndexId",
					outputIndexId);
		}
		if (partitionMaxMemberSelection != null) {
			multiPart.field(
					"partitionMaxMemberSelection",
					partitionMaxMemberSelection);
		}
		if (partitionPartitionerClass != null) {
			multiPart.field(
					"partitionPartitionerClass",
					partitionPartitionerClass);
		}
		if (partitionPartitionDecreaseRate != null) {
			multiPart.field(
					"partitionPartitionDecreaseRate",
					partitionPartitionDecreaseRate);
		}
		if (partitionPartitionPrecision != null) {
			multiPart.field(
					"partitionPartitionPrecision",
					partitionPartitionPrecision);
		}
		if (partitionSecondaryPartitionerClass != null) {
			multiPart.field(
					"partitionSecondaryPartitionerClass",
					partitionSecondaryPartitionerClass);
		}
		final Response resp = analyticService.dbScan(multiPart);
		return resp;
	}

	public Response dbScan(
			final String storename,
			final String mapReduceHdfsBaseDir,
			final String extractMaxInputSplit,
			final String extractMinInputSplit,
			final String adapterIds, // Array of strings
			final String clusteringMaxIterations,
			final String clusteringMinimumSize,
			final String partitionMaxDistance ) {

		return dbScan(
				storename,
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
			final String input_storename,
			final String output_storename,
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
			final String cqlFilter ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"input_storename",
				input_storename);
		multiPart.field(
				"output_storename",
				output_storename);
		multiPart.field(
				"featuretype",
				featuretype);
		multiPart.field(
				"minLevel",
				minLevel.toString());
		multiPart.field(
				"maxLevel",
				maxLevel.toString());
		multiPart.field(
				"coverageName",
				coverageName);
		multiPart.field(
				"jobTrackerOrResourceManHostPort",
				jobTrackerOrResourceManHostPort);
		if (indexId != null) {
			multiPart.field(
					"indexId",
					indexId);
		}
		if (minSplits != null) {
			multiPart.field(
					"minSplits",
					minSplits.toString());
		}
		if (maxSplits != null) {
			multiPart.field(
					"maxSplits",
					maxSplits.toString());
		}
		if (hdfsHostPort != null) {
			multiPart.field(
					"hdfsHostPort",
					hdfsHostPort);
		}
		if (tileSize != null) {
			multiPart.field(
					"tileSize",
					tileSize.toString());
		}
		if (cqlFilter != null) {
			multiPart.field(
					"cqlFilter",
					cqlFilter);
		}
		final Response resp = analyticService.kde(multiPart);
		return resp;
	}

	public Response kde(
			final String input_storename,
			final String output_storename,
			final String featuretype,
			final Integer minLevel,
			final Integer maxLevel,
			final String coverageName,
			final String jobTrackerOrResourceManHostPort ) {

		return kde(
				input_storename,
				output_storename,
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
			final String input_storename,
			final String output_storename,
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
			final String hullTypeName ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"input_storename",
				input_storename);
		multiPart.field(
				"output_storename",
				output_storename);
		if (appName != null) {
			multiPart.field(
					"appName",
					appName);
		}
		if (host != null) {
			multiPart.field(
					"host",
					host);
		}
		if (master != null) {
			multiPart.field(
					"master",
					master);
		}
		if (numClusters != null) {
			multiPart.field(
					"numClusters",
					numClusters.toString());
		}
		if (numIterations != null) {
			multiPart.field(
					"numIterations",
					numIterations.toString());
		}
		if (epsilon != null) {
			multiPart.field(
					"epsilon",
					epsilon);
		}
		if (useTime != null) {
			multiPart.field(
					"useTime",
					useTime.toString());
		}
		if (generateHulls != null) {
			multiPart.field(
					"generateHulls",
					generateHulls.toString());
		}
		if (computeHullData != null) {
			multiPart.field(
					"computeHullData",
					computeHullData.toString());
		}
		if (cqlFilter != null) {
			multiPart.field(
					"cqlFilter",
					cqlFilter);
		}
		if (adapterId != null) {
			multiPart.field(
					"adapterId",
					adapterId);
		}
		if (minSplits != null) {
			multiPart.field(
					"minSplits",
					minSplits.toString());
		}
		if (maxSplits != null) {
			multiPart.field(
					"maxSplits",
					maxSplits.toString());
		}
		if (centroidTypeName != null) {
			multiPart.field(
					"centroidTypeName",
					centroidTypeName);
		}
		if (hullTypeName != null) {
			multiPart.field(
					"hullTypeName",
					hullTypeName);
		}

		final Response resp = analyticService.kmeansSpark(multiPart);
		return resp;
	}

	public Response kmeansSpark(
			final String input_storename,
			final String output_storename ) {

		return kmeansSpark(
				input_storename,
				output_storename,
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
			final String storename,
			final String mapReduceHdfsBaseDir,
			final String extractMaxInputSplit,
			final String extractMinInputSplit,
			final String adapterIds, // Array of strings
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
			final String authorizations,// Array of strings
			final String indexId,
			final String partitionMaxMemberSelection,
			final String partitionPartitionerClass,
			final String partitionPartitionPrecision,
			final String partitioningDistanceThresholds,
			final String partitioningGeometricDistanceUnit,
			final String partitionSecondaryPartitionerClass ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"store_name",
				storename);
		multiPart.field(
				"mapReduceHdfsBaseDir",
				mapReduceHdfsBaseDir);
		multiPart.field(
				"extractMaxInputSplit",
				extractMaxInputSplit);
		multiPart.field(
				"extractMinInputSplit",
				extractMinInputSplit);
		multiPart.field(
				"adapterIds",
				adapterIds);
		multiPart.field(
				"outputHdfsOutputPath",
				outputHdfsOutputPath);
		multiPart.field(
				"partitionMaxDistance",
				partitionMaxDistance);
		if (mapReduceConfigFile != null) {
			multiPart.field(
					"mapReduceConfigFile",
					mapReduceConfigFile);
		}
		if (mapReduceHdfsHostPort != null) {
			multiPart.field(
					"mapReduceHdfsHostPort",
					mapReduceHdfsHostPort);
		}
		if (mapReduceJobtrackerHostPort != null) {
			multiPart.field(
					"mapReduceJobtrackerHostPort",
					mapReduceJobtrackerHostPort);
		}
		if (mapReduceYarnResourceManager != null) {
			multiPart.field(
					"mapReduceYarnResourceManager",
					mapReduceYarnResourceManager);
		}
		if (commonDistanceFunctionClass != null) {
			multiPart.field(
					"commonDistanceFunctionClass",
					commonDistanceFunctionClass);
		}
		if (extractQuery != null) {
			multiPart.field(
					"extractQuery",
					extractQuery);
		}
		if (outputOutputFormat != null) {
			multiPart.field(
					"outputOutputFormat",
					outputOutputFormat);
		}
		if (inputFormatClass != null) {
			multiPart.field(
					"inputFormatClass",
					inputFormatClass);
		}
		if (inputHdfsPath != null) {
			multiPart.field(
					"inputHdfsPath",
					inputHdfsPath);
		}
		if (outputReducerCount != null) {
			multiPart.field(
					"outputReducerCount",
					outputReducerCount);
		}
		if (authorizations != null) {
			multiPart.field(
					"authorizations",
					authorizations);
		}
		if (indexId != null) {
			multiPart.field(
					"indexId",
					indexId);
		}
		if (partitionMaxMemberSelection != null) {
			multiPart.field(
					"partitionMaxMemberSelection",
					partitionMaxMemberSelection);
		}
		if (partitionPartitionerClass != null) {
			multiPart.field(
					"partitionPartitionerClass",
					partitionPartitionerClass);
		}
		if (partitionPartitionPrecision != null) {
			multiPart.field(
					"partitionPartitionPrecision",
					partitionPartitionPrecision);
		}
		if (partitioningDistanceThresholds != null) {
			multiPart.field(
					"partitioningDistanceThresholds",
					partitioningDistanceThresholds);
		}
		if (partitioningGeometricDistanceUnit != null) {
			multiPart.field(
					"partitioningGeometricDistanceUnit",
					partitioningGeometricDistanceUnit);
		}
		if (partitionSecondaryPartitionerClass != null) {
			multiPart.field(
					"partitionSecondaryPartitionerClass",
					partitionSecondaryPartitionerClass);
		}
		final Response resp = analyticService.nearestNeighbor(multiPart);
		return resp;

	}

	public Response nearestNeighbor(
			final String storename,
			final String mapReduceHdfsBaseDir,
			final String extractMaxInputSplit,
			final String extractMinInputSplit,
			final String adapterIds, // Array of strings
			final String outputHdfsOutputPath,
			final String partitionMaxDistance ) {
		return nearestNeighbor(
				storename,
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
			final String parameters,// Array of strings
			final String csvOutputFile,
			final String outputStoreName,
			final String outputTypeName,
			final Integer showResults ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		if (parameters != null) {
			multiPart.field(
					"parameters",
					parameters);
		}
		if (csvOutputFile != null) {
			multiPart.field(
					"csvOutputFile",
					csvOutputFile);
		}
		if (outputStoreName != null) {
			multiPart.field(
					"outputStoreName",
					outputStoreName);
		}
		if (outputTypeName != null) {
			multiPart.field(
					"outputTypeName",
					outputTypeName);
		}
		if (showResults != null) {
			multiPart.field(
					"showResults",
					showResults.toString());
		}

		final Response resp = analyticService.sql(multiPart);
		return resp;

	}

}
