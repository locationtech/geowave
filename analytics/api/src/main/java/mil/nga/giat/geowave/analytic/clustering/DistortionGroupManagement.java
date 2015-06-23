package mil.nga.giat.geowave.analytic.clustering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find the max change in distortion between some k and k-1, picking the value k
 * associated with that change.
 * 
 * In a multi-group setting, each group may have a different optimal k. Thus,
 * the optimal batch may be different for each group. Each batch is associated
 * with a different value k.
 * 
 * Choose the appropriate batch for each group. Then change the batch identifier
 * for group centroids to a final provided single batch identifier ( parent
 * batch ).
 * 
 */
public class DistortionGroupManagement
{

	final static Logger LOGGER = LoggerFactory.getLogger(DistortionGroupManagement.class);

	/**
	 * 
	 * @param ops
	 * @param distortationTableName
	 *            the name of the table holding the distortions
	 * @param parentBatchId
	 *            the batch id to associate with the centroids for each group
	 * @return
	 */
	public static <T> int retainBestGroups(
			final BasicAccumuloOperations ops,
			final AnalyticItemWrapperFactory<T> itemWrapperFactory,
			final String dataTypeId,
			final String indexId,
			final String distortationTableName,
			final String parentBatchId,
			final int level ) {

		try {
			final Scanner scanner = ops.createScanner(distortationTableName);
			final Map<String, DistortionGroup> groupDistortions = new HashMap<String, DistortionGroup>();

			// row id is group id
			// colQual is cluster count

			for (final Entry<Key, Value> entry : scanner) {
				final String groupID = entry.getKey().getRow().toString();
				final Integer clusterCount = Integer.parseInt(entry.getKey().getColumnQualifier().toString());
				final Double distortion = Double.parseDouble(entry.getValue().toString());

				DistortionGroup grp = groupDistortions.get(groupID);
				if (grp == null) {
					grp = new DistortionGroup(
							groupID);
					groupDistortions.put(
							groupID,
							grp);
				}
				grp.addPair(
						clusterCount,
						distortion);

			}

			scanner.clearScanIterators();
			scanner.close();

			final CentroidManagerGeoWave<T> centroidManager = new CentroidManagerGeoWave<T>(
					ops,
					itemWrapperFactory,
					dataTypeId,
					indexId,
					parentBatchId,
					level);

			for (final DistortionGroup grp : groupDistortions.values()) {
				final int optimalK = grp.bestCount();
				LOGGER.info("Batch: " + parentBatchId + "; Group: " + grp.groupID + "; Optimal Cluster Size: " + optimalK);
				final String batchId = parentBatchId + "_" + optimalK;
				centroidManager.transferBatch(
						batchId,
						grp.getGroupID());
			}
		}
		catch (final RuntimeException ex) {
			throw ex;
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot detremine groups for batch" + parentBatchId,
					ex);
			return 1;
		}
		return 0;
	}

	private static class DistortionGroup
	{
		final String groupID;
		final List<Pair<Integer, Double>> clusterCountToDistortion = new ArrayList<Pair<Integer, Double>>();

		public DistortionGroup(
				final String groupID ) {
			this.groupID = groupID;
		}

		public void addPair(
				final Integer count,
				final Double distortation ) {
			clusterCountToDistortion.add(Pair.of(
					count,
					distortation));
		}

		public String getGroupID() {
			return groupID;
		}

		public int bestCount() {
			Collections.sort(
					clusterCountToDistortion,
					new Comparator<Pair<Integer, Double>>() {

						@Override
						public int compare(
								final Pair<Integer, Double> arg0,
								final Pair<Integer, Double> arg1 ) {
							return arg0.getKey().compareTo(
									arg1.getKey());
						}
					});
			double maxJump = -1.0;
			Integer jumpIdx = -1;
			Double oldD = 0.0; // base case !?
			for (final Pair<Integer, Double> pair : clusterCountToDistortion) {
				final Double jump = pair.getValue() - oldD;
				if (jump > maxJump) {
					maxJump = jump;
					jumpIdx = pair.getKey();
				}
				oldD = pair.getValue();
			}
			return jumpIdx;
		}
	}
}
