package mil.nga.giat.geowave.adapter.vector.render;

import org.geoserver.wms.WMS;
import org.geoserver.wms.WMSInfo;
import org.geoserver.wms.WMSInfo.WMSInterpolation;
import org.geoserver.wms.WMSInfoImpl;

public class DistributedRenderWMSFacade extends
		WMS
{
	private final DistributedRenderOptions options;

	public DistributedRenderWMSFacade(
			final DistributedRenderOptions options ) {
		super(
				null);
		this.options = options;
	}

	@Override
	public int getMaxBuffer() {
		return options.getBuffer();
	}

	@Override
	public int getMaxRenderingTime() {
		return options.getMaxRenderTime();
	}

	@Override
	public int getMaxRenderingErrors() {
		return options.getMaxErrors();
	}

	@Override
	public WMSInterpolation getInterpolation() {
		return WMSInterpolation.values()[options.getWmsInterpolationOrdinal()];
	}

	@Override
	public boolean isContinuousMapWrappingEnabled() {
		return options.isContinuousMapWrapping();
	}

	@Override
	public boolean isAdvancedProjectionHandlingEnabled() {
		return options.isAdvancedProjectionHandlingEnabled();
	}

	@Override
	public WMSInfo getServiceInfo() {
		return new WMSInfoImpl();
	}

	@Override
	public int getMaxRequestMemory() {
		// bypass checking memory within distributed rendering
		return -1;
	}

}
