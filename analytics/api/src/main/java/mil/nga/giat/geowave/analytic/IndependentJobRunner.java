package mil.nga.giat.geowave.analytic;

import java.util.Collection;

import mil.nga.giat.geowave.analytic.param.ParameterEnum;

public interface IndependentJobRunner
{
	public int run(
			PropertyManagement properties )
			throws Exception;

	public Collection<ParameterEnum<?>> getParameters();
}
