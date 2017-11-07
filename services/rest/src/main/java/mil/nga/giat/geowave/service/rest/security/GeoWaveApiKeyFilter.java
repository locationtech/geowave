package mil.nga.giat.geowave.service.rest.security;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.filter.GenericFilterBean;

public class GeoWaveApiKeyFilter extends
		GenericFilterBean
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveApiKeyFilter.class);

	/**
	 * This filter can be put in front of API routes to ensure that valid keys
	 * are used to make calls to the API.
	 */

	@Override
	public void doFilter(
			ServletRequest request,
			ServletResponse response,
			FilterChain chain )
			throws IOException,
			ServletException {

		boolean validKeyFound = true;
		try {
			final ServletContext servletContext = this.getServletContext();
			final ApplicationContext ac = WebApplicationContextUtils.getWebApplicationContext(servletContext);
			final GeoWaveBaseApiKeyDB dbBean = (GeoWaveBaseApiKeyDB) ac.getBean("apiKeyDB");
			final String apiKey = request.getParameter("apiKey");
			// early outs for apiKey not in request and/or not existing in the
			// DB
			if (apiKey == null) {
				LOGGER.error("apiKey is null");
				validKeyFound = false;
			}
			else if (!dbBean.hasKey(apiKey)) {
				LOGGER.error("apiKey is invalid");
				validKeyFound = false;
			}
		}
		catch (Exception e) {
			LOGGER.error(
					"Error: ",
					e.getMessage());
		}

		if (!validKeyFound) return;

		chain.doFilter(
				request,
				response);
	}
}
