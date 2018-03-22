package mil.nga.giat.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/analytic")
public interface AnalyticService
{
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/dbscan")
	public Response dbScan(
			@QueryParam("storename") String storename,
			@QueryParam("mapReduceHdfsBaseDir") String mapReduceHdfsBaseDir,
			@QueryParam("extractMaxInputSplit") String extractMaxInputSplit,
			@QueryParam("extractMinInputSplit") String extractMinInputSplit,
			@QueryParam("adapterIds") String adapterIds, // Array of strings
			@QueryParam("clusteringMaxIterations") String clusteringMaxIterations,
			@QueryParam("clusteringMinimumSize") String clusteringMinimumSize,
			@QueryParam("partitionMaxDistance") String partitionMaxDistance,
			@QueryParam("mapReduceConfigFile") String mapReduceConfigFile,
			@QueryParam("mapReduceHdfsHostPort") String mapReduceHdfsHostPort,
			@QueryParam("mapReduceJobtrackerHostPort") String mapReduceJobtrackerHostPort,
			@QueryParam("mapReduceYarnResourceManager") String mapReduceYarnResourceManager,
			@QueryParam("commonDistanceFunctionClass") String commonDistanceFunctionClass,
			@QueryParam("extractQuery") String extractQuery,
			@QueryParam("outputOutputFormat") String outputOutputFormat,
			@QueryParam("inputFormatClass") String inputFormatClass,
			@QueryParam("inputHdfsPath") String inputHdfsPath,
			@QueryParam("outputReducerCount") String outputReducerCount,
			@QueryParam("authorizations") String authorizations,// Array of
																// strings
			@QueryParam("indexId") String indexId,
			@QueryParam("outputHdfsOutputPath") String outputHdfsOutputPath,
			@QueryParam("partitioningDistanceThresholds") String partitioningDistanceThresholds,
			@QueryParam("partitioningGeometricDistanceUnit") String partitioningGeometricDistanceUnit,
			@QueryParam("globalBatchId") String globalBatchId,
			@QueryParam("hullDataTypeId") String hullDataTypeId,
			@QueryParam("hullProjectionClass") String hullProjectionClass,
			@QueryParam("outputDataNamespaceUri") String outputDataNamespaceUri,
			@QueryParam("outputDataTypeId") String outputDataTypeId,
			@QueryParam("outputIndexId") String outputIndexId,
			@QueryParam("partitionMaxMemberSelection") String partitionMaxMemberSelection,
			@QueryParam("partitionPartitionerClass") String partitionPartitionerClass,
			@QueryParam("partitionPartitionDecreaseRate") String partitionPartitionDecreaseRate,
			@QueryParam("partitionPartitionPrecision") String partitionPartitionPrecision,
			@QueryParam("partitionSecondaryPartitionerClass") String partitionSecondaryPartitionerClass );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/kde")
	public Response kde(
			@QueryParam("input_storename") String input_storename,
			@QueryParam("output_storename") String output_storename,
			@QueryParam("featuretype") String featuretype,
			@QueryParam("minLevel") Integer minLevel,
			@QueryParam("maxLevel") Integer maxLevel,
			@QueryParam("coverageName") String coverageName,
			@QueryParam("jobTrackerOrResourceManHostPort") String jobTrackerOrResourceManHostPort,
			@QueryParam("indexId") String indexId,
			@QueryParam("minSplits") Integer minSplits,
			@QueryParam("maxSplits") Integer maxSplits,
			@QueryParam("hdfsHostPort") String hdfsHostPort,
			@QueryParam("tileSize") Integer tileSize,
			@QueryParam("cqlFilter") String cqlFilter );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/kmeansspark")
	public Response kmeansSpark(
			@QueryParam("input_storename") String input_storename,
			@QueryParam("output_storename") String output_storename,
			@QueryParam("appName") String appName,
			@QueryParam("host") String host,
			@QueryParam("master") String master,
			@QueryParam("numClusters") Integer numClusters,
			@QueryParam("numIterations") Integer numIterations,
			@QueryParam("epsilon") String epsilon,
			@QueryParam("useTime") Boolean useTime,
			@QueryParam("generateHulls") Boolean generateHulls,
			@QueryParam("computeHullData") Boolean computeHullData,
			@QueryParam("cqlFilter") String cqlFilter,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("minSplits") Integer minSplits,
			@QueryParam("maxSplits") Integer maxSplits,
			@QueryParam("centroidTypeName") String centroidTypeName,
			@QueryParam("hullTypeName") String hullTypeName );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/nn")
	public Response nearestNeighbor(
			@QueryParam("storename") String storename,
			@QueryParam("mapReduceHdfsBaseDir") String mapReduceHdfsBaseDir,
			@QueryParam("extractMaxInputSplit") String extractMaxInputSplit,
			@QueryParam("extractMinInputSplit") String extractMinInputSplit,
			@QueryParam("adapterIds") String adapterIds, // Array of strings
			@QueryParam("outputHdfsOutputPath") String outputHdfsOutputPath,
			@QueryParam("partitionMaxDistance") String partitionMaxDistance,
			@QueryParam("mapReduceConfigFile") String mapReduceConfigFile,
			@QueryParam("mapReduceHdfsHostPort") String mapReduceHdfsHostPort,
			@QueryParam("mapReduceJobtrackerHostPort") String mapReduceJobtrackerHostPort,
			@QueryParam("mapReduceYarnResourceManager") String mapReduceYarnResourceManager,
			@QueryParam("commonDistanceFunctionClass") String commonDistanceFunctionClass,
			@QueryParam("extractQuery") String extractQuery,
			@QueryParam("outputOutputFormat") String outputOutputFormat,
			@QueryParam("inputFormatClass") String inputFormatClass,
			@QueryParam("inputHdfsPath") String inputHdfsPath,
			@QueryParam("outputReducerCount") String outputReducerCount,
			@QueryParam("authorizations") String authorizations,// Array of
																// strings
			@QueryParam("indexId") String indexId,
			@QueryParam("partitionMaxMemberSelection") String partitionMaxMemberSelection,
			@QueryParam("partitionPartitionerClass") String partitionPartitionerClass,
			@QueryParam("partitionPartitionPrecision") String partitionPartitionPrecision,
			@QueryParam("partitioningDistanceThresholds") String partitioningDistanceThresholds,
			@QueryParam("partitioningGeometricDistanceUnit") String partitioningGeometricDistanceUnit,
			@QueryParam("partitionSecondaryPartitionerClass") String partitionSecondaryPartitionerClass );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/sql")
	public Response sql(
			@QueryParam("parameters") String parameters,// Array of strings
			@QueryParam("csvOutputFile") String csvOutputFile,
			@QueryParam("outputStoreName") String outputStoreName,
			@QueryParam("outputTypeName") String outputTypeName,
			@QueryParam("showResults") Integer showResults );
}
