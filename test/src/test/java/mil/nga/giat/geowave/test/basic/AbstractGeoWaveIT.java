package mil.nga.giat.geowave.test.basic;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.TestUtils;

public abstract class AbstractGeoWaveIT
{
	abstract protected DataStorePluginOptions getDataStorePluginOptions();

	@Before
	public void cleanBefore()
			throws IOException {
		TestUtils.deleteAll(getDataStorePluginOptions());
	}

	@After
	public void cleanAfter()
			throws IOException {
		TestUtils.deleteAll(getDataStorePluginOptions());
	}
}
