package mil.nga.giat.geowave.service.grpc;

import io.grpc.BindableService;

public interface GeoWaveGrpcServiceSpi
{
	// classes that implement this interface just need to return
	// "this" cast as a BindableService.
	public BindableService getBindableService();
}
