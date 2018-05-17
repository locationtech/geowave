package mil.nga.giat.geowave.core.cli.api;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.restlet.data.Status;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;

public class ServiceEnableCommandTest
{

	private class ServiceEnabledCommand_TESTING extends
			ServiceEnabledCommand
	{

		private HttpMethod method;

		public ServiceEnabledCommand_TESTING(
				HttpMethod method ) {
			this.method = method;
		}

		@Override
		public void execute(
				OperationParams params )
				throws Exception {}

		@Override
		public Object computeResults(
				OperationParams params )
				throws Exception {
			return null;
		}

		@Override
		public HttpMethod getMethod() {
			return method;
		}

	}

	@Before
	public void setUp()
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test
	public void defaultSuccessStatusIs200ForGET() {

		ServiceEnabledCommand_TESTING classUnderTest = new ServiceEnabledCommand_TESTING(
				HttpMethod.GET);

		Assert.assertEquals(
				Status.SUCCESS_OK,
				classUnderTest.getSuccessStatus());
	}

	@Test
	public void defaultSuccessStatusIs201ForPOST() {

		ServiceEnabledCommand_TESTING classUnderTest = new ServiceEnabledCommand_TESTING(
				HttpMethod.POST);

		Assert.assertEquals(
				Status.SUCCESS_CREATED,
				classUnderTest.getSuccessStatus());
	}

}
