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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.service.ConfigService;
import net.sf.json.JSONObject;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

public class ConfigServiceClient
{
	private final ConfigService configService;

	public ConfigServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public ConfigServiceClient(
			final String baseUrl,
			String user,
			String password ) {
		// ClientBuilder bldr = ClientBuilder.newBuilder();
		// if (user != null && password != null) {
		// HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
		// user,
		// password);
		// bldr.register(feature);
		// }
		configService = WebResourceFactory.newResource(
				ConfigService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	// }

	public JSONObject list() {
		final Response resp = configService.list();
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		multiPart.field(
				"zookeeper",
				zookeeper);
		if (makeDefault != null) {
			multiPart.field(
					"makeDefault",
					makeDefault.toString());
		}
		if (geowaveNamespace != null) {
			multiPart.field(
					"geowaveNamespace",
					geowaveNamespace);
		}
		if (disableServiceSide != null) {
			multiPart.field(
					"disableServiceSide",
					disableServiceSide.toString());
		}
		if (coprocessorjar != null) {
			multiPart.field(
					"coprocessorjar",
					coprocessorjar);
		}
		if (persistAdapter != null) {
			multiPart.field(
					"persistAdapter",
					persistAdapter.toString());
		}
		if (persistIndex != null) {
			multiPart.field(
					"persistIndex",
					persistIndex.toString());
		}
		if (persistDataStatistics != null) {
			multiPart.field(
					"persistDataStatistics",
					persistDataStatistics.toString());
		}
		if (createTable != null) {
			multiPart.field(
					"createTable",
					createTable.toString());
		}
		if (useAltIndex != null) {
			multiPart.field(
					"useAltIndex",
					useAltIndex.toString());
		}
		if (enableBlockCache != null) {
			multiPart.field(
					"enableBlockCache",
					enableBlockCache.toString());
		}
		final Response resp = configService.addHBaseStore(multiPart);
		return resp;
	}

	public Response addAccumuloStore(
			final String name,
			final String zookeeper,
			final String username,
			final String password ) {

		return addAccumuloStore(
				name,
				zookeeper,
				username,
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

	public Response addAccumuloStore(
			final String name,
			final String zookeeper,
			final String username,
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		multiPart.field(
				"zookeeper",
				zookeeper);
		multiPart.field(
				"username",
				username);
		multiPart.field(
				"password",
				password);
		if (makeDefault != null) {
			multiPart.field(
					"makeDefault",
					makeDefault.toString());
		}
		if (geowaveNamespace != null) {
			multiPart.field(
					"geowaveNamespace",
					geowaveNamespace);
		}
		if (useLocalityGroups != null) {
			multiPart.field(
					"useLocalityGroups",
					useLocalityGroups.toString());
		}
		if (persistAdapter != null) {
			multiPart.field(
					"persistAdapter",
					persistAdapter.toString());
		}
		if (persistIndex != null) {
			multiPart.field(
					"persistIndex",
					persistIndex.toString());
		}
		if (persistDataStatistics != null) {
			multiPart.field(
					"persistDataStatistics",
					persistDataStatistics.toString());
		}
		if (createTable != null) {
			multiPart.field(
					"createTable",
					createTable.toString());
		}
		if (useAltIndex != null) {
			multiPart.field(
					"useAltIndex",
					useAltIndex.toString());
		}
		if (enableBlockCache != null) {
			multiPart.field(
					"enableBlockCache",
					enableBlockCache.toString());
		}
		final Response resp = configService.addAccumuloStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		if (scanCacheSize != null) {
			multiPart.field(
					"scanCacheSize",
					scanCacheSize.toString());
		}
		if (projectId != null) {
			multiPart.field(
					"projectId",
					projectId);
		}
		if (instanceId != null) {
			multiPart.field(
					"instanceId",
					instanceId);
		}
		if (makeDefault != null) {
			multiPart.field(
					"makeDefault",
					makeDefault.toString());
		}
		if (persistAdapter != null) {
			multiPart.field(
					"persistAdapter",
					persistAdapter.toString());
		}
		if (persistIndex != null) {
			multiPart.field(
					"persistIndex",
					persistIndex.toString());
		}
		if (persistDataStatistics != null) {
			multiPart.field(
					"persistDataStatistics",
					persistDataStatistics.toString());
		}
		if (createTable != null) {
			multiPart.field(
					"createTable",
					createTable.toString());
		}
		if (useAltIndex != null) {
			multiPart.field(
					"useAltIndex",
					useAltIndex.toString());
		}
		if (enableBlockCache != null) {
			multiPart.field(
					"enableBlockCache",
					enableBlockCache.toString());
		}
		final Response resp = configService.addBigTableStore(multiPart);
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

	public Response addSpatialIndex(
			final String name,
			final Boolean makeDefault,
			final String nameOverride,
			final Integer numPartitions,
			final String partitionStrategy,
			final Boolean storeTime,
			final String crs ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		if (makeDefault != null) {
			multiPart.field(
					"makeDefault",
					makeDefault.toString());
		}
		if (nameOverride != null) {
			multiPart.field(
					"nameOverride",
					nameOverride);
		}
		if (numPartitions != null) {
			multiPart.field(
					"numPartitions",
					numPartitions.toString());
		}
		if (partitionStrategy != null) {
			multiPart.field(
					"partitionStrategy",
					partitionStrategy);
		}
		if (storeTime != null) {
			multiPart.field(
					"storeTime",
					storeTime.toString());
		}
		if (crs != null) {
			multiPart.field(
					"crs",
					crs);
		}
		final Response resp = configService.addSpatialIndex(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		if (makeDefault != null) {
			multiPart.field(
					"makeDefault",
					makeDefault.toString());
		}
		if (nameOverride != null) {
			multiPart.field(
					"nameOverride",
					nameOverride);
		}
		if (numPartitions != null) {
			multiPart.field(
					"numPartitions",
					numPartitions.toString());
		}
		if (partitionStrategy != null) {
			multiPart.field(
					"partitionStrategy",
					partitionStrategy);
		}
		if (periodicity != null) {
			multiPart.field(
					"periodicity",
					periodicity);
		}
		if (bias != null) {
			multiPart.field(
					"bias",
					bias);
		}
		if (maxDuplicates != null) {
			multiPart.field(
					"maxDuplicates",
					maxDuplicates.toString());
		}
		if (crs != null) {
			multiPart.field(
					"crs",
					crs);
		}
		final Response resp = configService.addSpatialIndex(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"GeoServer_URL",
				GeoServer_URL);
		if (username != null) {
			multiPart.field(
					"username",
					username);
		}
		if (pass != null) {
			multiPart.field(
					"pass",
					pass);
		}
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		if (sslSecurityProtocol != null) {
			multiPart.field(
					"sslSecurityProtocol",
					sslSecurityProtocol);
		}
		if (sslTrustStorePath != null) {
			multiPart.field(
					"sslTrustStorePath",
					sslTrustStorePath);
		}
		if (sslTrustStorePassword != null) {
			multiPart.field(
					"sslTrustStorePassword",
					sslTrustStorePassword);
		}
		if (sslTrustStoreType != null) {
			multiPart.field(
					"sslTrustStoreType",
					sslTrustStoreType);
		}
		if (sslTruststoreProvider != null) {
			multiPart.field(
					"sslTruststoreProvider",
					sslTruststoreProvider);
		}
		if (sslTrustManagerAlgorithm != null) {
			multiPart.field(
					"sslTrustManagerAlgorithm",
					sslTrustManagerAlgorithm);
		}
		if (sslTrustManagerProvider != null) {
			multiPart.field(
					"sslTrustManagerProvider",
					sslTrustManagerProvider);
		}
		if (sslKeyStorePath != null) {
			multiPart.field(
					"sslKeyStorePath",
					sslKeyStorePath);
		}
		if (sslKeyStorePassword != null) {
			multiPart.field(
					"sslKeyStorePassword",
					sslKeyStorePassword);
		}
		if (sslKeyStoreProvider != null) {
			multiPart.field(
					"sslKeyStoreProvider",
					sslKeyStoreProvider);
		}
		if (sslKeyPassword != null) {
			multiPart.field(
					"sslKeyPassword",
					sslKeyPassword);
		}
		if (sslKeyStoreType != null) {
			multiPart.field(
					"sslKeyStoreType",
					sslKeyStoreType);
		}
		if (sslKeyManagerAlgorithm != null) {
			multiPart.field(
					"sslKeyManagerAlgorithm",
					sslKeyManagerAlgorithm);
		}
		if (sslKeyManagerProvider != null) {
			multiPart.field(
					"sslKeyManagerProvider",
					sslKeyManagerProvider);
		}
		final Response resp = configService.configGeoServer(multiPart);
		return resp;
	}

	public Response configHDFS(
			final String HDFS_DefaultFS_URL ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"HDFS_DefaultFS_URL",
				HDFS_DefaultFS_URL);
		final Response resp = configService.configHDFS(multiPart);
		return resp;
	}

	public Response removeIndex(
			final String name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		final Response resp = configService.removeIndex(multiPart);
		return resp;
	}

	public Response removeIndexGroup(
			final String name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		final Response resp = configService.removeIndexGroup(multiPart);
		return resp;
	}

	public Response removeStore(
			final String name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		final Response resp = configService.removeStore(multiPart);
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

	public Response set(
			final String name,
			final String value,
			Boolean password ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"name",
				name);
		multiPart.field(
				"value",
				value);
		if (password != null) {
			multiPart.field(
					"password",
					password.toString());
		}
		final Response resp = configService.set(multiPart);
		return resp;
	}
}
