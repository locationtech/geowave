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

public class GeoWaveApiKeySetterFilter extends
		GenericFilterBean
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveApiKeySetterFilter.class);

	/**
	 * This class is only responsible for setting two servlet context
	 * attributes: "userName" and "apiKey"
	 */

	@Override
	public void doFilter(
			ServletRequest request,
			ServletResponse response,
			FilterChain chain )
			throws IOException,
			ServletException {

		try {
			final ServletContext servletContext = this.getServletContext();
			final ApplicationContext ac = WebApplicationContextUtils.getWebApplicationContext(servletContext);
			final GeoWaveBaseApiKeyDB apiKeyDB = (GeoWaveBaseApiKeyDB) ac.getBean("apiKeyDB");
			final String userAndKey = apiKeyDB.getCurrentUserAndKey();

			if (!userAndKey.equals("")) {
				final String[] userAndKeyToks = userAndKey.split(":");
				servletContext.setAttribute(
						"userName",
						userAndKeyToks[0]);
				servletContext.setAttribute(
						"apiKey",
						userAndKeyToks[1]);
			}
		}
		catch (Exception e) {
			return;
		}
		finally {
			chain.doFilter(
					request,
					response);
		}

	}
}
