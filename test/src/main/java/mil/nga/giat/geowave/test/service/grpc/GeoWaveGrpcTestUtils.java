package mil.nga.giat.geowave.test.service.grpc;

import mil.nga.giat.geowave.test.ZookeeperTestEnvironment;
import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;

public class GeoWaveGrpcTestUtils
{
	public final static String adapterId = "TestGeometry";
	public final static String indexId = "grpc-spatial";
	public final static String storeName = "grpc";
	public final static String outputStoreName = "grpc-output";
	public final static String cqlSpatialQuery = "BBOX(geometry,0.0,0.0, 25.0, 25.0)";
	public final static String wktSpatialQuery = "POLYGON (( " + "0.0 0.0, " + "0.0 25.0, " + "25.0 25.0, "
			+ "25.0 0.0, " + "0.0 0.0" + "))";
	public final static String temporalQueryStartTime = "2016-02-20T01:32:56Z";
	public final static String temporalQueryEndTime = "2016-02-21T01:32:56Z";

	// this is purely a convenience method so the gRPC test client does not need
	// any dependency on the test environment directly
	public static MapReduceTestEnvironment getMapReduceTestEnv() {
		return MapReduceTestEnvironment.getInstance();
	}

	public static ZookeeperTestEnvironment getZookeeperTestEnv() {
		return ZookeeperTestEnvironment.getInstance();
	}
}
