package mil.nga.giat.geowave.service.rest.webapp;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.web.DefaultRedirectStrategy;
import org.springframework.security.web.RedirectStrategy;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;

/**
 * This class provides the mechanism to conduct a simple redirect after a user
 * logs in so we may send them to a default page instead of the original URL
 * they may have tried accessing
 */
public class RedirectLoginSuccessHandler implements
		AuthenticationSuccessHandler
{

	@Override
	public void onAuthenticationSuccess(
			HttpServletRequest httpServletRequest,
			HttpServletResponse httpServletResponse,
			Authentication authentication )
			throws IOException,
			ServletException {

		RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();
		redirectStrategy.sendRedirect(
				httpServletRequest,
				httpServletResponse,
				"/");
	}
}
