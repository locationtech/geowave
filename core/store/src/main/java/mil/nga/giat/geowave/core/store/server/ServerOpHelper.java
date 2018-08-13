package mil.nga.giat.geowave.core.store.server;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.OptionProvider;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;

public class ServerOpHelper
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ServerOpHelper.class);
	private static final String ROW_MERGING_SUFFIX = "_COMBINER";
	public static final String ROW_MERGING_VISIBILITY_SUFFIX = "_VISIBILITY_COMBINER";

	public static boolean updateServerOps(
			final ServerSideOperations operations,
			final String index,
			final ServerOpConfig... configs ) {
		if ((configs != null) && (configs.length > 0)) {
			final Map<String, ImmutableSet<ServerOpScope>> iteratorScopes = operations.listServerOps(index);
			for (final ServerOpConfig config : configs) {
				boolean mustDelete = false;
				boolean exists = false;
				final ImmutableSet<ServerOpScope> existingScopes = iteratorScopes.get(config.getServerOpName());
				ImmutableSet<ServerOpScope> configuredScopes;
				if (config.getScopes() == null) {
					configuredScopes = Sets.immutableEnumSet(EnumSet.allOf(ServerOpScope.class));
				}
				else {
					configuredScopes = Sets.immutableEnumSet(config.getScopes());
				}
				Map<String, String> configuredOptions = null;
				if (existingScopes != null) {
					if (existingScopes.size() == configuredScopes.size()) {
						exists = true;
						for (final ServerOpScope s : existingScopes) {
							if (!configuredScopes.contains(s)) {
								// this iterator exists with the wrong
								// scope, we will assume we want to remove
								// it and add the new configuration
								LOGGER.warn("found iterator '" + config.getServerOpName() + "' missing scope '"
										+ s.name() + "', removing it and re-attaching");

								mustDelete = true;
								break;
							}
						}
					}
					if (existingScopes.size() > 0) {
						// see if the options are the same, if they are not
						// the same, apply a merge with the existing options
						// and the configured options
						final Iterator<ServerOpScope> it = existingScopes.iterator();
						while (it.hasNext()) {
							final ServerOpScope scope = it.next();
							final Map<String, String> existingOptions = operations.getServerOpOptions(
									index,
									config.getServerOpName(),
									scope);
							configuredOptions = config.getOptions(existingOptions);
							if (existingOptions == null) {
								mustDelete = (configuredOptions == null);
							}
							else if (configuredOptions == null) {
								mustDelete = true;
							}
							else {
								// neither are null, compare the size of
								// the entry sets and check that they
								// are equivalent
								final Set<Entry<String, String>> existingEntries = existingOptions.entrySet();
								final Set<Entry<String, String>> configuredEntries = configuredOptions.entrySet();
								if (existingEntries.size() != configuredEntries.size()) {
									mustDelete = true;
								}
								else {
									mustDelete = (!existingEntries.containsAll(configuredEntries));
								}
							}
							// we found the setting existing in one
							// scope, assume the options are the same
							// for each scope
							break;
						}
					}
				}
				if (configuredOptions == null) {
					configuredOptions = config.getOptions(new HashMap<String, String>());
				}
				if (mustDelete) {
					operations.updateServerOp(
							index,
							config.getServerOpPriority(),
							config.getServerOpName(),
							config.getServerOpClass(),
							configuredOptions,
							existingScopes,
							configuredScopes);
				}
				else if (!exists) {
					operations.addServerOp(
							index,
							config.getServerOpPriority(),
							config.getServerOpName(),
							config.getServerOpClass(),
							configuredOptions,
							configuredScopes);
				}
			}
		}
		return true;
	}

	public static void addServerSideRowMerging(
			final RowMergingDataAdapter<?, ?> adapter,
			final short internalAdapterId,
			final ServerSideOperations operations,
			final String serverOpClassName,
			final String serverOpVisiblityClassName,
			final String tableName ) {
		final RowTransform rowTransform = adapter.getTransform();
		if (rowTransform != null) {
			final OptionProvider optionProvider = new RowMergingAdapterOptionProvider(
					internalAdapterId,
					adapter);
			final ServerOpConfig rowMergingCombinerConfig = new ServerOpConfig(
					EnumSet.allOf(ServerOpScope.class),
					rowTransform.getBaseTransformPriority(),
					rowTransform.getTransformName() + ROW_MERGING_SUFFIX,
					serverOpClassName,
					optionProvider);
			final ServerOpConfig rowMergingVisibilityCombinerConfig = new ServerOpConfig(
					EnumSet.of(ServerOpScope.SCAN),
					rowTransform.getBaseTransformPriority() + 1,
					rowTransform.getTransformName() + ROW_MERGING_VISIBILITY_SUFFIX,
					serverOpVisiblityClassName,
					optionProvider);

			updateServerOps(
					operations,
					tableName,
					rowMergingCombinerConfig,
					rowMergingVisibilityCombinerConfig);
		}
	}

	public static void addServerSideMerging(
			final ServerSideOperations operations,
			final String mergingOpBaseName,
			final int mergingOpBasePriority,
			final String serverOpClassName,
			final String serverOpVisiblityClassName,
			final OptionProvider optionProvider,
			final String tableName ) {
		final ServerOpConfig rowMergingCombinerConfig = new ServerOpConfig(
				EnumSet.allOf(ServerOpScope.class),
				mergingOpBasePriority,
				mergingOpBaseName + ROW_MERGING_SUFFIX,
				serverOpClassName,
				optionProvider);
		final ServerOpConfig rowMergingVisibilityCombinerConfig = new ServerOpConfig(
				EnumSet.of(ServerOpScope.SCAN),
				mergingOpBasePriority + 1,
				mergingOpBaseName + ROW_MERGING_VISIBILITY_SUFFIX,
				serverOpVisiblityClassName,
				optionProvider);

		updateServerOps(
				operations,
				tableName,
				rowMergingCombinerConfig,
				rowMergingVisibilityCombinerConfig);
	}
}
