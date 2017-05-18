package mil.nga.giat.geowave.cli.debug;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

@GeowaveOperation(name = "fullscan", parentOperation = DebugSection.class)
@Parameters(commandDescription = "fulltable scan")
public class FullTableScan extends
		AbstractGeoWaveQuery
{
	private static Logger LOGGER = Logger.getLogger(FullTableScan.class);

	@Parameter(names = "--attr", description = "Attribute for unique count")
	private String attr;

	@Parameter(names = "--top", description = "How many top counts to print (default: 10)")
	private Integer topCount = 10;

	@Parameter(names = "--max", description = "Don't print values above (default: -1, not used)")
	private Integer maxValue = -1;

	@Parameter(names = "--min", description = "Stop printing when value drops below (default: -1, not used)")
	private Integer minValue = -1;

	@Override
	protected long runQuery(
			final GeotoolsFeatureDataAdapter adapter,
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final DataStore dataStore,
			final boolean debug ) {
		long count = 0;
		HashMap<String, Integer> uniqueValueMap = new HashMap<>();

		try (final CloseableIterator<Object> it = dataStore.query(
				new QueryOptions(
						adapterId,
						indexId),
				null)) {
			while (it.hasNext()) {
				if (debug) {
					System.out.println(it.next());
				}
				else {
					Object obj = it.next();
					if (attr != null && !attr.isEmpty()) {
						if (obj instanceof SimpleFeature) {
							SimpleFeature feature = (SimpleFeature) obj;
							Object val = feature.getAttribute(attr);
							if (val instanceof String) {
								String valStr = (String) val;
								Integer valCount = uniqueValueMap.get(valStr);
								if (valCount == null) {
									uniqueValueMap.put(
											valStr,
											1);
								}
								else {
									uniqueValueMap.put(
											valStr,
											valCount + 1);
								}
							}
						}
					}
				}
				count++;
			}

			if (attr != null && !attr.isEmpty()) {
				System.out.println("There were " + uniqueValueMap.entrySet().size() + " unique values for " + attr);

				Map<String, Integer> sortedMap = sortByValue(uniqueValueMap);

				int showCount = 0;
				for (Entry<String, Integer> entry : sortedMap.entrySet()) {
					showCount++;

					if (maxValue != -1 && entry.getValue() > maxValue) {
						continue;
					}
					else if (minValue > 0) {
						if (entry.getValue() < minValue) {
							break;
						}
					}
					else if (showCount > topCount) {
						break;
					}

					System.out.println("(" + showCount + ") Unique value for " + attr + ": '" + entry.getKey()
							+ "' was counted " + entry.getValue() + " times.");
				}
			}

		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read result",
					e);
		}
		return count;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
			Map<K, V> map ) {
		return map
				.entrySet()
				.stream()
				.sorted(
						Map.Entry.comparingByValue(
								Collections.reverseOrder()))
				.collect(
						Collectors.toMap(
								Map.Entry::getKey,
								Map.Entry::getValue,
								(
										e1,
										e2 ) -> e1,
								LinkedHashMap::new));
	}
}
