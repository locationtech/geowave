package org.locationtech.geowave.core.geotime.ingest;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;

import com.beust.jcommander.Parameter;

public abstract class CommonSpatialOptions implements
		DimensionalityTypeOptions
{
	@Parameter(names = {
		"-c",
		"--crs"
	}, required = false, description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
	protected String crs = GeometryUtils.DEFAULT_CRS_STR;

	@Parameter(names = {
		"-gp",
		"--geometryPrecision"
	}, required = false, description = "The maximum precision of the geometry when encoding.  Lower precision will save more disk space when encoding. (Between -8 and 7)")
	protected int geometryPrecision = GeometryUtils.MAX_GEOMETRY_PRECISION;

	@Parameter(names = {
		"-fp",
		"--fullGeometryPrecision"
	}, required = false, description = "If specified, geometry will be encoded losslessly.  Uses more disk space.")
	protected boolean fullGeometryPrecision = false;

	public void setCrs(
			String crs ) {
		this.crs = crs;
	}

	public void setGeometryPrecision(
			final @Nullable Integer geometryPrecision ) {
		if (geometryPrecision == null) {
			this.fullGeometryPrecision = true;
		}
		else {
			this.fullGeometryPrecision = false;
			this.geometryPrecision = geometryPrecision;
		}
	}

	public Integer getGeometryPrecision() {
		if (fullGeometryPrecision) {
			return null;
		}
		else {
			return geometryPrecision;
		}
	}
}
