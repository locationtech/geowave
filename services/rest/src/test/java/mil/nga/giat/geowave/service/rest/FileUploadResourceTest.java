package mil.nga.giat.geowave.service.rest;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

public class FileUploadResourceTest
{

	private Representation mockedEntity(
			MediaType mediaType ) {

		Representation entity = Mockito.mock(Representation.class);

		Mockito.when(
				entity.getMediaType()).thenReturn(
				mediaType);

		return entity;
	}

	@Before
	public void setUp()
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test(expected = ResourceException.class)
	public void nullEntityThrows()
			throws Exception {
		FileUploadResource classUnderTest = new FileUploadResource();

		classUnderTest.accept(null);
	}

	@Test(expected = ResourceException.class)
	public void wrongMediaTypeThrows()
			throws Exception {
		FileUploadResource classUnderTest = new FileUploadResource();
		Representation entity = mockedEntity(MediaType.APPLICATION_PDF);

		classUnderTest.accept(entity);
	}
}
