package mil.nga.giat.geowave.analytic.param;

import java.util.Set;

import org.apache.commons.cli.Option;

public interface GroupParameterEnum extends
		ParameterEnum
{
	public void fillOptions(
			Set<Option> options );

}
