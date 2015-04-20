package mil.nga.giat.geowave.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.service.InfoService;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/info")
public class InfoServiceImpl implements
		InfoService
{
	private final static Logger log = Logger.getLogger(InfoServiceImpl.class);
	private final static int defaultIndentation = 2;

	private final String zookeeperUrl;
	private final String instance;
	private final String username;
	private final String password;

	private Connector connector;

	public InfoServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		final Properties props = ServiceUtils.loadProperties(servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties")));

		zookeeperUrl = ServiceUtils.getProperty(
				props,
				"zookeeper.url");

		instance = ServiceUtils.getProperty(
				props,
				"zookeeper.instance");

		username = ServiceUtils.getProperty(
				props,
				"zookeeper.username");

		password = ServiceUtils.getProperty(
				props,
				"zookeeper.password");

		try {
			connector = new ConnectorPool().getConnector(
					zookeeperUrl,
					instance,
					username,
					password);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			log.error(
					"Could not create the Accumulo Connector. ",
					e);
		}
	}

	// lists the namespaces in geowave
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces")
	public Response getNamespaces() {
		final Collection<String> namespaces = AccumuloUtils.getNamespaces(connector);
		final JSONArray namespacesArray = new JSONArray();
		for (final String namespace : namespaces) {
			final JSONObject namespaceObj = new JSONObject();
			namespaceObj.put(
					"name",
					namespace);
			namespacesArray.add(namespaceObj);
		}
		final JSONObject namespacesObj = new JSONObject();
		namespacesObj.put(
				"namespaces",
				namespacesArray);
		return Response.ok(
				namespacesObj.toString(defaultIndentation)).build();
	}

	// lists the indices associated with the given namespace
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces/{namespace}/indices")
	public Response getIndices(
			@PathParam("namespace")
			final String namespace ) {
		final List<Index> indices = AccumuloUtils.getIndices(
				connector,
				namespace);
		final JSONArray indexNames = new JSONArray();
		for (final Index index : indices) {
			if ((index != null) && (index.getId() != null)) {
				final JSONObject indexObj = new JSONObject();
				indexObj.put(
						"name",
						index.getId().getString());
				indexNames.add(indexObj);
			}
		}
		final JSONObject indicesObj = new JSONObject();
		indicesObj.put(
				"indices",
				indexNames);
		return Response.ok(
				indicesObj.toString(defaultIndentation)).build();
	}

	// lists the adapters associated with the given namespace
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces/{namespace}/adapters")
	public Response getAdapters(
			@PathParam("namespace")
			final String namespace ) {
		final Collection<DataAdapter<?>> dataAdapters = AccumuloUtils.getDataAdapters(
				connector,
				namespace);
		final JSONArray dataAdapterNames = new JSONArray();
		for (final DataAdapter<?> dataAdapter : dataAdapters) {
			if ((dataAdapter != null) && (dataAdapter.getAdapterId() != null)) {
				final JSONObject adapterObj = new JSONObject();
				adapterObj.put(
						"name",
						dataAdapter.getAdapterId().getString());
				dataAdapterNames.add(adapterObj);
			}
		}
		final JSONObject dataAdaptersObj = new JSONObject();
		dataAdaptersObj.put(
				"adapters",
				dataAdapterNames);
		return Response.ok(
				dataAdaptersObj.toString(defaultIndentation)).build();
	}
}