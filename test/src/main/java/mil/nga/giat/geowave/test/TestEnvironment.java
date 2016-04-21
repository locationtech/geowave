package mil.nga.giat.geowave.test;

public interface TestEnvironment
{
	public void setup()
			throws Exception;

	public void tearDown()
			throws Exception;

	public TestEnvironment[] getDependentEnvironments();
}
