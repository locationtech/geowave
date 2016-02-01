package mil.nga.giat.geowave.analytic.model;

import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public interface IndexModelBuilder extends
		java.io.Serializable
{
	public CommonIndexModel buildModel();
}
