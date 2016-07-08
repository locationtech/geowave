package mil.nga.giat.geowave.core.store.spi;

import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class TestDimensionalityTypeProvider implements
		DimensionalityTypeProviderSpi
{

	@Override
	public Class<? extends CommonIndexValue>[] getRequiredIndexTypes() {
		return null;
	}

	@Override
	public String getDimensionalityTypeName() {
		return "test";
	}

	@Override
	public String getDimensionalityTypeDescription() {
		return null;
	}

	@Override
	public int getPriority() {
		return 0;
	}

	@Override
	public PrimaryIndex createPrimaryIndex() {
		return null;
	}

	@Override
	public DimensionalityTypeOptions getOptions() {
		return null;
	}

}
