package mil.nga.giat.geowave.service.rest.security;

import java.io.Serializable;

import javax.servlet.ServletContext;

import org.springframework.web.context.ServletContextAware;

abstract public class GeoWaveBaseApiKeyDB implements
		Serializable,
		ServletContextAware
{
	/**
	 * Base class for implementing ApiKey databases
	 */
	static final long serialVersionUID = 1L;
	transient private ServletContext servletContext;

	public GeoWaveBaseApiKeyDB() {}

	public abstract void initApiKeyDatabase();

	public abstract boolean hasKey(
			String apiKey );

	/**
	 * Returns the username and associated key value. Must be in the form
	 * "name:key"
	 */
	public abstract String getCurrentUserAndKey();

	@Override
	public void setServletContext(
			ServletContext servletContext ) {
		this.servletContext = servletContext;
	}
}
