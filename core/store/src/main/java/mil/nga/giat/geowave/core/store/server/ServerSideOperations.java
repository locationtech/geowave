package mil.nga.giat.geowave.core.store.server;

import java.util.Map;

import com.google.common.collect.ImmutableSet;

import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;

public interface ServerSideOperations extends
		DataStoreOperations
{
	/**
	 * Returns a mapping of existing registered server-side operations with
	 * serverop name as the key and the registered scopes as the value
	 *
	 * @return the mapping
	 */
	public Map<String, ImmutableSet<ServerOpScope>> listServerOps(
			String index );

	/**
	 * get the particular existing configured options for this server op at this
	 * scope
	 *
	 * @param index
	 *            the index/table
	 * @param serverOpName
	 *            the operation name
	 * @param scope
	 *            the scope
	 * @return the options
	 */
	public Map<String, String> getServerOpOptions(
			String index,
			String serverOpName,
			ServerOpScope scope );

	/**
	 * remove this server operation - because accumulo requires scopes as a
	 * parameter it is passed into this method, but the server op will be
	 * removed entirely regardless of scopes
	 *
	 * @param index
	 *            the index/table
	 * @param serverOpName
	 *            the operation name
	 * @param scopes
	 *            the existing scopes
	 */
	public void removeServerOp(
			String index,
			String serverOpName,
			ImmutableSet<ServerOpScope> scopes );

	/**
	 * add this server operation
	 *
	 * @param index
	 *            the index/table
	 * @param priority
	 *            the operation priority (this is merely relative, it defines
	 *            how to order multiple operations, from low to high)
	 * @param serverOpName
	 *            the operation name
	 * @param operationClass
	 *            the operation class
	 * @param properties
	 *            the operation options
	 * @param configuredScopes
	 *            the scopes
	 */
	public void addServerOp(
			String index,
			int priority,
			String name,
			String operationClass,
			Map<String, String> properties,
			ImmutableSet<ServerOpScope> configuredScopes );

	/**
	 * update this server operation, the current scopes are passed in because
	 * accumulo requires iteratorscope as a parameter to remove the iterator.
	 * This will update the server op to the new scope.
	 *
	 * @param index
	 *            the index/table
	 * @param priority
	 *            the operation priority (this is merely relative, it defines
	 *            how to order multiple operations, from low to high)
	 * @param serverOpName
	 *            the operation name
	 * @param operationClass
	 *            the operation class
	 * @param properties
	 *            the operation options
	 * @param currentScopes
	 *            the existing scopes
	 * @param newScopes
	 *            the new configured scopes
	 */
	public void updateServerOp(
			String index,
			int priority,
			String name,
			String operationClass,
			Map<String, String> properties,
			ImmutableSet<ServerOpScope> currentScopes,
			ImmutableSet<ServerOpScope> newScopes );

	/**
	 * Method to lookup the version of a remote datastore
	 */
	public String getVersion();
}
