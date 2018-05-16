/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.service.client;

import java.lang.reflect.AnnotatedElement;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Path;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.service.ConfigService;

public class ConfigServiceClient implements
		ConfigService
{
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServiceClient.class);
	private final ConfigService configService;
	// Jersey 2 web resource proxy client doesn't work well with dynamic
	// key-value pair queryparams such as the generic addStore
	private final WebTarget addStoreTarget;

	public ConfigServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public ConfigServiceClient(
			final String baseUrl,
			final String user,
			final String password ) {
		WebTarget target = ClientBuilder.newClient().target(
				baseUrl);
		configService = WebResourceFactory.newResource(
				ConfigService.class,
				target);
		addStoreTarget = createAddStoreTarget(target);
	}

	private static WebTarget createAddStoreTarget(
			WebTarget baseTarget ) {

		WebTarget addStoreTarget = addPathFromAnnotation(
				ConfigService.class,
				baseTarget);
		try {
			addStoreTarget = addPathFromAnnotation(
					ConfigService.class.getMethod(
							"addStore",
							String.class,
							String.class,
							String.class,
							Map.class),
					addStoreTarget);
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.warn(
					"Unable to derive path from method annotations",
					e);
			// default to hardcoded method path
			addStoreTarget = addStoreTarget.path("/addstore/{type}");
		}
		return addStoreTarget;
	}

	private static WebTarget addPathFromAnnotation(
			final AnnotatedElement ae,
			WebTarget target ) {
		final Path p = ae.getAnnotation(Path.class);
		if (p != null) {
			target = target.path(p.value());
		}
		return target;
	}

	@Override
	public Response list(
			final String filter ) {
		final Response resp = configService.list(filter);
		resp.bufferEntity();
		return resp;
	}

	public Response list() {
		return configService.list(null);
	}

	public Response addHBaseStore(
			final String name,
			final String zookeeper ) {

		return addHBaseStore(
				name,
				zookeeper,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response addHBaseStore(
			final String name,
			final String zookeeper,
			final Boolean makeDefault,
			final String geowaveNamespace,
			final Boolean disableServiceSide,
			final String coprocessorjar,
			final Boolean persistAdapter,
			final Boolean persistIndex,
			final Boolean persistDataStatistics,
			final Boolean createTable,
			final Boolean useAltIndex,
			final Boolean enableBlockCache ) {

		final Response resp = configService.addHBaseStore(
				name,
				zookeeper,
				makeDefault,
				geowaveNamespace,
				disableServiceSide,
				coprocessorjar,
				persistAdapter,
				persistIndex,
				persistDataStatistics,
				createTable,
				useAltIndex,
				enableBlockCache);
		return resp;
	}

	public Response addAccumuloStore(
			final String name,
			final String zookeeper,
			final String instance,
			final String user,
			final String password ) {

		return addAccumuloStore(
				name,
				zookeeper,
				instance,
				user,
				password,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response addAccumuloStore(
			final String name,
			final String zookeeper,
			final String instance,
			final String user,
			final String password,
			final Boolean makeDefault,
			final String geowaveNamespace,
			final Boolean useLocalityGroups,
			final Boolean persistAdapter,
			final Boolean persistIndex,
			final Boolean persistDataStatistics,
			final Boolean createTable,
			final Boolean useAltIndex,
			final Boolean enableBlockCache ) {

		final Response resp = configService.addAccumuloStore(
				name,
				zookeeper,
				instance,
				user,
				password,
				makeDefault,
				geowaveNamespace,
				useLocalityGroups,
				persistAdapter,
				persistIndex,
				persistDataStatistics,
				createTable,
				useAltIndex,
				enableBlockCache);
		return resp;
	}

	public Response addBigTableStore(
			final String name ) {

		return addBigTableStore(
				name,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response addBigTableStore(
			final String name,
			final Boolean makeDefault,
			final Integer scanCacheSize,
			final String projectId,
			final String instanceId,
			final String geowaveNamespace,
			final Boolean useLocalityGroups,
			final Boolean persistAdapter,
			final Boolean persistIndex,
			final Boolean persistDataStatistics,
			final Boolean createTable,
			final Boolean useAltIndex,
			final Boolean enableBlockCache ) {

		final Response resp = configService.addBigTableStore(
				name,
				makeDefault,
				scanCacheSize,
				projectId,
				instanceId,
				geowaveNamespace,
				useLocalityGroups,
				persistAdapter,
				persistIndex,
				persistDataStatistics,
				createTable,
				useAltIndex,
				enableBlockCache);
		return resp;
	}

	public Response addSpatialIndex(
			final String name ) {
		return addSpatialIndex(
				name,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response addSpatialIndex(
			final String name,
			final Boolean makeDefault,
			final String nameOverride,
			final Integer numPartitions,
			final String partitionStrategy,
			final Boolean storeTime,
			final String crs ) {

		final Response resp = configService.addSpatialIndex(
				name,
				makeDefault,
				nameOverride,
				numPartitions,
				partitionStrategy,
				storeTime,
				crs);
		return resp;
	}

	public Response addIndexGroup(
			final String name,
			final String[] indexes ) {
		final Response resp = configService.addIndexGroup(
				name,
				String.join(
						",",
						indexes));
		return resp;
	}

	public Response addSpatialTemporalIndex(
			final String name ) {
		return addSpatialTemporalIndex(
				name,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response addSpatialTemporalIndex(
			final String name,
			final Boolean makeDefault,
			final String nameOverride,
			final Integer numPartitions,
			final String partitionStrategy,
			final String periodicity,
			final String bias,
			final Long maxDuplicates,
			final String crs ) {

		final Response resp = configService.addSpatialTemporalIndex(
				name,
				makeDefault,
				nameOverride,
				numPartitions,
				partitionStrategy,
				periodicity,
				bias,
				maxDuplicates,
				crs);
		return resp;
	}

	public Response configGeoServer(
			final String GeoServer_URL ) {

		return configGeoServer(
				GeoServer_URL,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null);
	}

	@Override
	public Response configGeoServer(
			final String GeoServer_URL,
			final String username,
			final String pass,
			final String workspace,
			final String sslSecurityProtocol,
			final String sslTrustStorePath,
			final String sslTrustStorePassword,
			final String sslTrustStoreType,
			final String sslTruststoreProvider,
			final String sslTrustManagerAlgorithm,
			final String sslTrustManagerProvider,
			final String sslKeyStorePath,
			final String sslKeyStorePassword,
			final String sslKeyStoreProvider,
			final String sslKeyPassword,
			final String sslKeyStoreType,
			final String sslKeyManagerAlgorithm,
			final String sslKeyManagerProvider ) {

		final Response resp = configService.configGeoServer(
				GeoServer_URL,
				username,
				pass,
				workspace,
				sslSecurityProtocol,
				sslTrustStorePath,
				sslTrustStorePassword,
				sslTrustStoreType,
				sslTruststoreProvider,
				sslTrustManagerAlgorithm,
				sslTrustManagerProvider,
				sslKeyStorePath,
				sslKeyStorePassword,
				sslKeyStoreProvider,
				sslKeyPassword,
				sslKeyStoreType,
				sslKeyManagerAlgorithm,
				sslKeyManagerProvider);
		return resp;
	}

	@Override
	public Response configHDFS(
			final String HDFS_DefaultFS_URL ) {

		final Response resp = configService.configHDFS(HDFS_DefaultFS_URL);
		return resp;
	}

	@Override
	public Response removeIndex(
			final String name ) {

		final Response resp = configService.removeIndex(name);
		return resp;
	}

	@Override
	public Response removeIndexGroup(
			final String name ) {

		final Response resp = configService.removeIndexGroup(name);
		return resp;
	}

	@Override
	public Response removeStore(
			final String name ) {

		final Response resp = configService.removeStore(name);
		return resp;
	}

	public Response set(
			final String name,
			final String value ) {

		return set(
				name,
				value,
				null);
	}

	@Override
	public Response set(
			final String name,
			final String value,
			final Boolean password ) {

		final Response resp = configService.set(
				name,
				value,
				password);
		return resp;
	}

	@Override
	public Response addStore(
			String name,
			String type,
			String geowaveNamespace,
			Map<String, String> additionalQueryParams ) {
		WebTarget internalAddStoreTarget = addStoreTarget.resolveTemplate(
				"type",
				type);
		internalAddStoreTarget = internalAddStoreTarget.queryParam(
				"name",
				name);
		if (geowaveNamespace != null && !geowaveNamespace.isEmpty()) {
			internalAddStoreTarget = internalAddStoreTarget.queryParam(
					"geowaveNamespace",
					name);
		}
		for (Entry<String, String> e : additionalQueryParams.entrySet()) {
			internalAddStoreTarget = internalAddStoreTarget.queryParam(
					e.getKey(),
					e.getValue());
		}
		return internalAddStoreTarget.request().accept(
				MediaType.APPLICATION_JSON).method(
				"POST");
	}

	@Override
	public Response addIndexGroup(
			final String name,
			final String indexes ) {
		final Response resp = configService.addIndexGroup(
				name,
				indexes);
		return resp;
	}
}
