package mil.nga.giat.geowave.service;

import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/config")
public interface ConfigService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/list")
	public Response list(
			@QueryParam("filter") String filters );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addstore/hbase")
	public Response addHBaseStore(
			@QueryParam("name") String name,
			@QueryParam("zookeeper") String zookeeper,
			@QueryParam("makeDefault") Boolean makeDefault,
			@QueryParam("geowaveNamespace") String geowaveNamespace,
			@QueryParam("disableServiceSide") Boolean disableServiceSide,
			@QueryParam("coprocessorjar") String coprocessorjar,
			@QueryParam("persistAdapter") Boolean persistAdapter,
			@QueryParam("persistIndex") Boolean persistIndex,
			@QueryParam("persistDataStatistics") Boolean persistDataStatistics,
			@QueryParam("createTable") Boolean createTable,
			@QueryParam("useAltIndex") Boolean useAltIndex,
			@QueryParam("enableBlockCache") Boolean enableBlockCache );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addstore/accumulo")
	public Response addAccumuloStore(
			@QueryParam("name") String name,
			@QueryParam("zookeeper") String zookeeper,
			@QueryParam("instance") String instance,
			@QueryParam("user") String user,
			@QueryParam("password") String password,
			@QueryParam("makeDefault") Boolean makeDefault,
			@QueryParam("geowaveNamespace") String geowaveNamespace,
			@QueryParam("useLocalityGroups") Boolean useLocalityGroups,
			@QueryParam("persistAdapter") Boolean persistAdapter,
			@QueryParam("persistIndex") Boolean persistIndex,
			@QueryParam("persistDataStatistics") Boolean persistDataStatistics,
			@QueryParam("createTable") Boolean createTable,
			@QueryParam("useAltIndex") Boolean useAltIndex,
			@QueryParam("enableBlockCache") Boolean enableBlockCache );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addstore/bigtable")
	public Response addBigTableStore(
			@QueryParam("name") String name,
			@QueryParam("makeDefault") Boolean makeDefault,
			@QueryParam("scanCacheSize") Integer scanCacheSize,
			@QueryParam("projectId") String projectId,
			@QueryParam("instanceId") String instanceId,
			@QueryParam("geowaveNamespace") String geowaveNamespace,
			@QueryParam("useLocalityGroups") Boolean useLocalityGroups,
			@QueryParam("persistAdapter") Boolean persistAdapter,
			@QueryParam("persistIndex") Boolean persistIndex,
			@QueryParam("persistDataStatistics") Boolean persistDataStatistics,
			@QueryParam("createTable") Boolean createTable,
			@QueryParam("useAltIndex") Boolean useAltIndex,
			@QueryParam("enableBlockCache") Boolean enableBlockCache );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addstore/{type}")
	public Response addStore(
			@QueryParam("name") String name,
			@PathParam("type") String type,
			@QueryParam("geowaveNamespace") @DefaultValue("") String geowaveNamespace,
			Map<String, String> additionalQueryParams );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addindex/spatial")
	public Response addSpatialIndex(
			@QueryParam("name") String name,
			@QueryParam("makeDefault") Boolean makeDefault,
			@QueryParam("nameOverride") String nameOverride,
			@QueryParam("numPartitions") Integer numPartitions,
			@QueryParam("partitionStrategy") String partitionStrategy,
			@QueryParam("storeTime") Boolean storeTime,
			@QueryParam("crs") String crs );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addindex/spatial_temporal")
	public Response addSpatialTemporalIndex(
			@QueryParam("name") String name,
			@QueryParam("makeDefault") Boolean makeDefault,
			@QueryParam("nameOverride") String nameOverride,
			@QueryParam("numPartitions") Integer numPartitions,
			@QueryParam("partitionStrategy") String partitionStrategy,
			@QueryParam("periodicity") String periodicity,
			@QueryParam("bias") String bias,
			@QueryParam("maxDuplicates") Long maxDuplicates,
			@QueryParam("crs") String crs );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addindexgrp")
	public Response addIndexGroup(
			@QueryParam("name") String name,
			@QueryParam("indexes") String indexes );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/geoserver")
	public Response configGeoServer(
			@QueryParam("GeoServer_URL") String GeoServer_URL,
			@QueryParam("username") String username,
			@QueryParam("pass") String pass,
			@QueryParam("workspace") String workspace,
			@QueryParam("sslSecurityProtocol") String sslSecurityProtocol,
			@QueryParam("sslTrustStorePath") String sslTrustStorePath,
			@QueryParam("sslTrustStorePassword") String sslTrustStorePassword,
			@QueryParam("sslTrustStoreType") String sslTrustStoreType,
			@QueryParam("sslTruststoreProvider") String sslTruststoreProvider,
			@QueryParam("sslTrustManagerAlgorithm") String sslTrustManagerAlgorithm,
			@QueryParam("sslTrustManagerProvider") String sslTrustManagerProvider,
			@QueryParam("sslKeyStorePath") String sslKeyStorePath,
			@QueryParam("sslKeyStorePassword") String sslKeyStorePassword,
			@QueryParam("sslKeyStoreProvider") String sslKeyStoreProvider,
			@QueryParam("sslKeyPassword") String sslKeyPassword,
			@QueryParam("sslKeyStoreType") String sslKeyStoreType,
			@QueryParam("sslKeyManagerAlgorithm") String sslKeyManagerAlgorithm,
			@QueryParam("sslKeyManagerProvider") String sslKeyManagerProvider );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/hdfs")
	public Response configHDFS(
			@QueryParam("HDFS_DefaultFS_URL") String HDFS_DefaultFS_URL );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmindex")
	public Response removeIndex(
			@QueryParam("name") String name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmstore")
	public Response removeStore(
			@QueryParam("name") String name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmindexgrp")
	public Response removeIndexGroup(
			@QueryParam("name") String name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/set")
	public Response set(
			@QueryParam("name") String name,
			@QueryParam("value") String value,
			@QueryParam("password") Boolean password );
}
