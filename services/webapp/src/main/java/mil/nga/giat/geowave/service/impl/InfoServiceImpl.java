package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.service.InfoService;
import mil.nga.giat.geowave.service.ServiceUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/info")
public class InfoServiceImpl implements
		InfoService
{
	private final static Logger LOGGER = LoggerFactory.getLogger(InfoServiceImpl.class);
	private final static int defaultIndentation = 2;
	private final Properties serviceProperties;

	public InfoServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		Properties props = null;
		try (InputStream is = servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties"))) {
			props = ServiceUtils.loadProperties(is);
		}
		catch (IOException e) {
			LOGGER.error(
					e.getLocalizedMessage(),
					e);
		}

		serviceProperties = props;
	}

	// lists the namespaces in geowave
	// TODO should we create a namespace store for this?
	// @Override
	// @GET
	// @Produces(MediaType.APPLICATION_JSON)
	// @Path("/namespaces")
	// public Response getNamespaces() {
	// final Collection<String> namespaces =
	// AccumuloUtils.getNamespaces(connector);
	// final JSONArray namespacesArray = new JSONArray();
	// for (final String namespace : namespaces) {
	// final JSONObject namespaceObj = new JSONObject();
	// namespaceObj.put(
	// "name",
	// namespace);
	// namespacesArray.add(namespaceObj);
	// }
	// final JSONObject namespacesObj = new JSONObject();
	// namespacesObj.put(
	// "namespaces",
	// namespacesArray);
	// return Response.ok(
	// namespacesObj.toString(defaultIndentation)).build();
	// }

	// lists the indices associated with the given namespace
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/indices/{storeName}")
	public Response getIndices(
			@PathParam("storeName")
			final String storeName ) {
		if ((storeName == null) || storeName.isEmpty()) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Get Indices Failed - Missing Store Name").build());
		}
		// Store
		final String namespace = DataStorePluginOptions.getStoreNamespace(storeName);
		final DataStorePluginOptions dataStorePlugin = new DataStorePluginOptions();
		if (!dataStorePlugin.load(
				serviceProperties,
				namespace)) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Get Indices Failed - Invalid Store").build());
		}
		try (CloseableIterator<Index<?, ?>> indices = dataStorePlugin.createIndexStore().getIndices()) {
			final JSONArray indexNames = new JSONArray();
			while (indices.hasNext()) {
				final Index<?, ?> index = indices.next();
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
		catch (final IOException e) {
			LOGGER.error(
					"Unable to read from index store for store '" + storeName + "'",
					e);
			return Response.serverError().build();
		}
	}

	// lists the adapters associated with the given namespace
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/adapters/{storeName}")
	public Response getAdapters(
			@PathParam("storeName")
			final String storeName ) {
		if ((storeName == null) || storeName.isEmpty()) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Get Adapters Failed - Missing Store Name").build());
		}
		// Store
		final String namespace = DataStorePluginOptions.getStoreNamespace(storeName);
		final DataStorePluginOptions dataStorePlugin = new DataStorePluginOptions();
		if (!dataStorePlugin.load(
				serviceProperties,
				namespace)) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Get Adapters Failed - Invalid Store").build());
		}
		try (CloseableIterator<DataAdapter<?>> dataAdapters = dataStorePlugin.createAdapterStore().getAdapters()) {
			final JSONArray dataAdapterNames = new JSONArray();
			while (dataAdapters.hasNext()) {
				final DataAdapter<?> dataAdapter = dataAdapters.next();
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
		catch (final IOException e) {
			LOGGER.error(
					"Unable to read from adapter store for store '" + storeName + "'",
					e);
			return Response.serverError().build();
		}
	}
}