/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
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
