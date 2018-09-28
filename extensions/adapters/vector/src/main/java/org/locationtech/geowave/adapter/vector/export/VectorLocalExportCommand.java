/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.export;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.geotools.filter.text.cql2.CQLException;
import org.locationtech.geowave.adapter.vector.AvroFeatureUtils;
import org.locationtech.geowave.adapter.vector.avro.AttributeValues;
import org.locationtech.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import org.locationtech.geowave.adapter.vector.cli.VectorSection;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.CQLQuery;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "localexport", parentOperation = VectorSection.class)
@Parameters(commandDescription = "Export data directly")
public class VectorLocalExportCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VectorLocalExportOptions options = new VectorLocalExportOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	public void execute(
			OperationParams params )
			throws IOException,
			CQLException {

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <store name>");
		}

		String storeName = parameters.get(0);

		// Config file
		File configFile = getGeoWaveConfigFile(params);
		StoreLoader inputStoreLoader = new StoreLoader(
				storeName);
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		PersistentAdapterStore adapterStore = inputStoreOptions.createAdapterStore();
		IndexStore indexStore = inputStoreOptions.createIndexStore();
		DataStore dataStore = inputStoreOptions.createDataStore();
		InternalAdapterStore internaAdapterStore = inputStoreOptions.createInternalAdapterStore();

		try (final DataFileWriter<AvroSimpleFeatureCollection> dfw = new DataFileWriter<AvroSimpleFeatureCollection>(
				new GenericDatumWriter<AvroSimpleFeatureCollection>(
						AvroSimpleFeatureCollection.SCHEMA$))) {
			dfw.setCodec(CodecFactory.snappyCodec());
			dfw.create(
					AvroSimpleFeatureCollection.SCHEMA$,
					options.getOutputFile());
			// get appropriate feature adapters
			final List<GeotoolsFeatureDataAdapter> featureAdapters = new ArrayList<GeotoolsFeatureDataAdapter>();
			if ((options.getAdapterIds() != null) && !options.getAdapterIds().isEmpty()) {
				for (final String adapterId : options.getAdapterIds()) {
					short internalAdapterId = internaAdapterStore.getInternalAdapterId(new ByteArrayId(
							adapterId));
					final InternalDataAdapter<?> internalDataAdapter = adapterStore.getAdapter(internalAdapterId);
					if (internalDataAdapter == null) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' not found");
						continue;
					}
					else if (!(internalDataAdapter.getAdapter() instanceof GeotoolsFeatureDataAdapter)) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' does not support vector export. Instance of "
										+ internalDataAdapter.getAdapter().getClass());
						continue;
					}
					featureAdapters.add((GeotoolsFeatureDataAdapter) internalDataAdapter.getAdapter());
				}
			}
			else {
				final CloseableIterator<InternalDataAdapter<?>> adapters = adapterStore.getAdapters();
				while (adapters.hasNext()) {
					final InternalDataAdapter<?> adapter = adapters.next();
					if (adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter) {
						featureAdapters.add((GeotoolsFeatureDataAdapter) adapter.getAdapter());
					}
				}
				adapters.close();
			}
			if (featureAdapters.isEmpty()) {
				JCommander.getConsole().println(
						"Unable to find any vector data types in store");
			}
			Index queryIndex = null;
			if (options.getIndexId() != null) {
				queryIndex = indexStore.getIndex(new ByteArrayId(
						options.getIndexId()));
				if (queryIndex == null) {
					JCommander.getConsole().println(
							"Unable to find index '" + options.getIndexId() + "' in store");
					return;
				}
			}
			for (final GeotoolsFeatureDataAdapter adapter : featureAdapters) {
				final SimpleFeatureType sft = adapter.getFeatureType();
				JCommander.getConsole().println(
						"Exporting type '" + sft.getTypeName() + "'");
				final QueryOptions queryOptions = new QueryOptions();
				if (queryIndex != null) {
					queryOptions.setIndex(queryIndex);
				}
				QueryConstraints queryConstraints = null;
				if (options.getCqlFilter() != null) {
					queryConstraints = CQLQuery.createOptimalQuery(
							options.getCqlFilter(),
							adapter,
							queryIndex,
							null);
				}
				queryOptions.setAdapter(adapter);

				final CloseableIterator<Object> it = dataStore.query(
						queryOptions,
						queryConstraints);
				int iteration = 0;
				while (it.hasNext()) {
					final AvroSimpleFeatureCollection simpleFeatureCollection = new AvroSimpleFeatureCollection();

					simpleFeatureCollection.setFeatureType(AvroFeatureUtils.buildFeatureDefinition(
							null,
							sft,
							null,
							""));
					final List<AttributeValues> avList = new ArrayList<AttributeValues>(
							options.getBatchSize());
					while (it.hasNext() && (avList.size() < options.getBatchSize())) {
						final Object obj = it.next();
						if (obj instanceof SimpleFeature) {
							final AttributeValues av = AvroFeatureUtils.buildAttributeValue(
									(SimpleFeature) obj,
									sft);
							avList.add(av);
						}
					}
					JCommander.getConsole().println(
							"Exported " + (avList.size() + (iteration * options.getBatchSize())) + " features from '"
									+ sft.getTypeName() + "'");
					iteration++;
					simpleFeatureCollection.setSimpleFeatureCollection(avList);
					dfw.append(simpleFeatureCollection);
					dfw.flush();
				}
				JCommander.getConsole().println(
						"Finished exporting '" + sft.getTypeName() + "'");
			}
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setOptions(
			VectorLocalExportOptions options ) {
		this.options = options;
	}

	public VectorLocalExportOptions getOptions() {
		return options;
	}
}
