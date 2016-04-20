package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.service.InfoService;
import mil.nga.giat.geowave.service.ServiceUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/info")
public class InfoServiceImpl implements
		InfoService
{
	private final static Logger LOGGER = Logger.getLogger(InfoServiceImpl.class);
	private final static int defaultIndentation = 2;
	private final IndexStoreFactorySpi indexStoreFactory;
	private final AdapterStoreFactorySpi adapterStoreFactory;
	private final Map<String, Object> configOptions;

	public InfoServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		final Properties props = ServiceUtils.loadProperties(servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties")));
		final Map<String, String> strMap = new HashMap<String, String>();

		final Set<Object> keySet = props.keySet();
		final Iterator<Object> it = keySet.iterator();
		while (it.hasNext()) {
			final String key = it.next().toString();
			strMap.put(
					key,
					props.getProperty(key));
		}
		configOptions = ConfigUtils.valuesFromStrings(strMap);
		indexStoreFactory = GeoWaveStoreFinder.findIndexStoreFactory(configOptions);
		adapterStoreFactory = GeoWaveStoreFinder.findAdapterStoreFactory(configOptions);
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
	@Path("/namespaces/{namespace}/indices")
	public Response getIndices(
			@PathParam("namespace")
			final String namespace ) {
		try (CloseableIterator<Index<?, ?>> indices = indexStoreFactory.createStore(
				configOptions,
				namespace).getIndices()) {

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
					"Unable to read from index store for namespace '" + namespace + "'",
					e);
			return Response.serverError().build();
		}
	}

	// lists the adapters associated with the given namespace
	@Override
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces/{namespace}/adapters")
	public Response getAdapters(
			@PathParam("namespace")
			final String namespace ) {
		try (CloseableIterator<DataAdapter<?>> dataAdapters = adapterStoreFactory.createStore(
				configOptions,
				namespace).getAdapters()) {
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
					"Unable to read from adapter store for namespace '" + namespace + "'",
					e);
			return Response.serverError().build();
		}
	}
}