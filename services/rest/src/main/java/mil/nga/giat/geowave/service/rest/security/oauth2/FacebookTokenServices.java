package mil.nga.giat.geowave.service.rest.security.oauth2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.RemoteTokenServices;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

public class FacebookTokenServices extends
		RemoteTokenServices
{
	protected final Log logger = LogFactory.getLog(getClass());

	private RestOperations restTemplate;

	private String checkTokenEndpointUrl;

	private String tokenName = "token";

	private AccessTokenConverter tokenConverter = new FacebookAccessTokenConverter();

	public FacebookTokenServices() {
		restTemplate = new RestTemplate();
		((RestTemplate) restTemplate).setErrorHandler(new DefaultResponseErrorHandler() {
			@Override
			// Ignore 400
			public void handleError(
					ClientHttpResponse response )
					throws IOException {
				if (response.getRawStatusCode() != 400) {
					super.handleError(response);
				}
			}
		});
	}

	public void setRestTemplate(
			RestOperations restTemplate ) {
		this.restTemplate = restTemplate;
	}

	public void setCheckTokenEndpointUrl(
			String checkTokenEndpointUrl ) {
		this.checkTokenEndpointUrl = checkTokenEndpointUrl;
	}

	public void setAccessTokenConverter(
			AccessTokenConverter accessTokenConverter ) {
		this.tokenConverter = accessTokenConverter;
	}

	public void setTokenName(
			String tokenName ) {
		this.tokenName = tokenName;
	}

	@Override
	public OAuth2Authentication loadAuthentication(
			String accessToken )
			throws AuthenticationException,
			InvalidTokenException {

		MultiValueMap<String, String> formData = new LinkedMultiValueMap<String, String>();
		formData.add(
				tokenName,
				accessToken);

		HttpHeaders headers = new HttpHeaders();
		String req = "";
		try {
			req = checkTokenEndpointUrl + "?access_token=" + URLEncoder.encode(
					accessToken,
					"UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			logger.error(
					"Unsupported encoding",
					e);
		}

		Map<String, Object> map = getForMap(
				req,
				formData,
				headers);

		if (map.containsKey("error")) {
			logger.debug("check_token returned error: " + map.get("error"));
			throw new InvalidTokenException(
					accessToken);
		}

		return tokenConverter.extractAuthentication(map);
	}

	@Override
	public OAuth2AccessToken readAccessToken(
			String accessToken ) {
		throw new UnsupportedOperationException(
				"Not supported: read access token");
	}

	private Map<String, Object> getForMap(
			String path,
			MultiValueMap<String, String> formData,
			HttpHeaders headers ) {
		if (headers.getContentType() == null) {
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
		}
		@SuppressWarnings("rawtypes")
		Map map = restTemplate.exchange(
				path,
				HttpMethod.GET,
				new HttpEntity<MultiValueMap<String, String>>(
						formData,
						headers),
				Map.class).getBody();
		@SuppressWarnings("unchecked")
		Map<String, Object> result = map;
		return result;
	}
}
