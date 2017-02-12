package mil.nga.giat.geowave.test.service;

import mil.nga.giat.geowave.cli.geoserver.GeoServerConfig;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRestClient;
import mil.nga.giat.geowave.test.config.ConfigCacheIT;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class GeoServerRestClientIT {

    private final static Logger LOGGER = Logger.getLogger(ConfigCacheIT.class);
    private static long startMillis;

    @BeforeClass
    public static void startTimer() {
        startMillis = System.currentTimeMillis();
        LOGGER.warn("-----------------------------------------");
        LOGGER.warn("*                                       *");
        LOGGER.warn("*       RUNNING GeoServerCLITest        *");
        LOGGER.warn("*                                       *");
        LOGGER.warn("-----------------------------------------");
    }

    @AfterClass
    public static void reportTest() {
        LOGGER.warn("-----------------------------------------");
        LOGGER.warn("*                                       *");
        LOGGER.warn("*      FINISHED GeoServerCLITest        *");
        LOGGER.warn("*         "
                + ((System.currentTimeMillis() - startMillis) / 1000)
                + "s elapsed.                 *");
        LOGGER.warn("*                                       *");
        LOGGER.warn("-----------------------------------------");
    }


    WebTarget webTarget;
    GeoServerConfig config;
    GeoServerRestClient client;


    private WebTarget mockedWebTarget() {
        WebTarget webTarget = mock(WebTarget.class);
        Invocation.Builder invBuilder = mock(Invocation.Builder.class);
        Response response = mock(Response.class);

        when(webTarget.path(anyString())).thenReturn(webTarget);
        when(webTarget.request()).thenReturn(invBuilder);

        when(invBuilder.get()).thenReturn(response);
        when(invBuilder.delete()).thenReturn(response);
        when(invBuilder.post(any(Entity.class))).thenReturn(response);

        return webTarget;

    }

    @Before
    public void prepare() {
        webTarget = mockedWebTarget();
        config = new GeoServerConfig();
        client = new GeoServerRestClient(config, webTarget);
    }

    @Test
    public void testGetFeatureLayer() {
        client.getFeatureLayer("some_layer");
        verify(webTarget).path("rest/layers/some_layer.json");
    }

    @Test
    public void testGetConfig() {
        GeoServerConfig returnedConfig = client.getConfig();
        assertEquals(config, returnedConfig);
    }

    @Test
    public void testGetCoverage() {
        client.getCoverage("some_workspace", "some_cvgStore", "some_coverage");
        verify(webTarget).path("rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages/some_coverage.json");
    }

    @Test
    public void testGetCoverageStores() {
        client.getCoverageStores("some_workspace");
        verify(webTarget).path("rest/workspaces/some_workspace/coveragestores.json");
    }


    @Test
    public void testGetCoverages() {
        client.getCoverages("some_workspace", "some_cvgStore");
        verify(webTarget).path("rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages.json");
    }


    @Test
    public void testGetDatastore() {
        client.getDatastore("some_workspace", "some_datastore");
        verify(webTarget).path("rest/workspaces/some_workspace/datastores/some_datastore.json");
    }

    @Test
    public void testGetStyle() {
        client.getStyle("some_style");
        verify(webTarget).path("rest/styles/some_style.sld");
    }

    @Test
    public void testGetStyles() {
        client.getStyles();
        verify(webTarget).path("rest/styles.json");
    }

    @Test
    public void testGetWorkspaces() {
        client.getWorkspaces();
        verify(webTarget).path("rest/workspaces.json");
    }

    @Test
    public void addFeatureLayer() {
        client.addFeatureLayer("some_workspace", "some_datastore",
                "some_layer", "some_style");
        verify(webTarget).path("rest/layers/some_layer.json");
    }

    @Test
    public void addCoverage() {
        client.addCoverage("some_workspace", "some_cvgStore", "some_coverage");
        verify(webTarget).path("rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages");
    }

    @Test
    public void addWorkspace() {
        client.addWorkspace("some_workspace");
        verify(webTarget).path("rest/workspaces");
    }
}
