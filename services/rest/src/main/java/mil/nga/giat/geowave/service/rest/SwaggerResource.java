package mil.nga.giat.geowave.service.rest;

import java.io.IOException;

import javax.servlet.ServletContext;

import org.restlet.data.MediaType;
import org.restlet.ext.apispark.internal.conversion.swagger.v1_2.model.ApiDeclaration;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.FileRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

public class SwaggerResource extends
		ServerResource
{
	/**
	 * This resource returns the swagger.json
	 */

	@Get("json")
	public String listResources() {
		
		final ServletContext servlet = (ServletContext) getContext().getAttributes().get(
				"org.restlet.ext.servlet.ServletContext");
		final String realPath = servlet.getRealPath("/");

		final JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
				new FileRepresentation(
						realPath + "swagger.json",
						MediaType.APPLICATION_JSON),
				ApiDeclaration.class);
		try {
			return result.getText();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "Not Found: swagger.json";
	}
}
