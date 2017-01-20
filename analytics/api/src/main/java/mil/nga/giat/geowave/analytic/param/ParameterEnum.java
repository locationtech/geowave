package mil.nga.giat.geowave.analytic.param;

import java.io.Serializable;

public interface ParameterEnum<T> extends
		Serializable
{
	public ParameterHelper<T> getHelper();

	public Enum<?> self();
}
