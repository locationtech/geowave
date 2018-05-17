package mil.nga.giat.geowave.service.grpc;

import java.io.File;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

//This class is intended to provide easily accessible global properties for gRPC clients and servers 
public class GeoWaveGrpcServiceOptions
{
	public static String host = "localhost"; // the ip or address that the
												// server resides at
	public static int port = 8090; // the client and server connection port
									// number
	public static File geowaveConfigFile = ConfigOptions.getDefaultPropertyFile(); // the
																					// config
																					// file
																					// that
																					// the
																					// service
																					// implementations
																					// will
																					// use
}
