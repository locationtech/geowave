package mil.nga.giat.geowave.adapter.vector.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.beust.jcommander.JCommander;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureUtils;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class VectorLocalExportCommand
{
	// TODO annotate appropriately when new commandline tools is merged
	private VectorLocalExportOptions options = new VectorLocalExportOptions();

	public void run()
			throws IOException,
			CQLException {
		try (final DataFileWriter<AvroSimpleFeatureCollection> dfw = new DataFileWriter<AvroSimpleFeatureCollection>(
				new GenericDatumWriter<AvroSimpleFeatureCollection>(
						AvroSimpleFeatureCollection.SCHEMA$))) {
			dfw.create(
					AvroSimpleFeatureCollection.SCHEMA$,
					options.getOutputFile());
			dfw.setCodec(CodecFactory.snappyCodec());
			// get appropriate feature adapters
			final List<GeotoolsFeatureDataAdapter> featureAdapters = new ArrayList<GeotoolsFeatureDataAdapter>();
			if ((options.getAdapterIds() != null) && !options.getAdapterIds().isEmpty()) {
				for (final String adapterId : options.getAdapterIds()) {
					final DataAdapter<?> adapter = options.getAdapterStore().getAdapter(
							new ByteArrayId(
									adapterId));
					if (adapter == null) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' not found");
						continue;
					}
					else if (!(adapter instanceof GeotoolsFeatureDataAdapter)) {
						JCommander.getConsole().println(
								"Type '" + adapterId + "' does not support vector export. Instance of " + adapter.getClass());
						continue;
					}
					featureAdapters.add((GeotoolsFeatureDataAdapter) adapter);
				}
			}
			else {
				final CloseableIterator<DataAdapter<?>> adapters = options.getAdapterStore().getAdapters();
				while (adapters.hasNext()) {
					final DataAdapter<?> adapter = adapters.next();
					if (adapter instanceof GeotoolsFeatureDataAdapter) {
						featureAdapters.add((GeotoolsFeatureDataAdapter) adapter);
					}
				}
				adapters.close();
			}
			if (featureAdapters.isEmpty()) {
				JCommander.getConsole().println(
						"Unable to find any vector data types in store");
			}
			PrimaryIndex queryIndex = null;
			if (options.getIndexId() != null) {
				final Index index = options.getIndexStore().getIndex(
						new ByteArrayId(
								options.getIndexId()));
				if (index == null) {
					JCommander.getConsole().println(
							"Unable to find index '" + options.getIndexId() + "' in store");
					return;
				}
				if (index instanceof PrimaryIndex) {
					queryIndex = (PrimaryIndex) index;
				}
				else {
					JCommander.getConsole().println(
							"Index '" + options.getIndexId() + "' is not a primary index");
					return;
				}
			}
			for (final GeotoolsFeatureDataAdapter adapter : featureAdapters) {
				final SimpleFeatureType sft = adapter.getType();
				JCommander.getConsole().println(
						"Exporting type '" + sft.getTypeName() + "'");
				final QueryOptions queryOptions = new QueryOptions();
				if (queryIndex != null) {
					queryOptions.setIndex(queryIndex);
				}
				Query queryConstraints = null;
				if (options.getCqlFilter() != null) {
					queryConstraints = new CQLQuery(
							options.getCqlFilter(),
							adapter);
				}
				queryOptions.setAdapter(adapter);

				final CloseableIterator<Object> it = options.getDataStore().query(
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
							"Exported " + (avList.size() + (iteration * options.getBatchSize())) + " features from '" + sft.getTypeName() + "'");
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
}
