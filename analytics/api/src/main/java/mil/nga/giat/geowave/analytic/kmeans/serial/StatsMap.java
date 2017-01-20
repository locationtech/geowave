package mil.nga.giat.geowave.analytic.kmeans.serial;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsMap implements
		AnalyticStats
{

	Map<StatValue, List<Double>> stats = new HashMap<StatValue, List<Double>>();

	@Override
	public void notify(
			final StatValue stat,
			final double amount ) {
		List<Double> list = stats.get(stat);
		if (list == null) {
			list = new ArrayList<Double>();
			stats.put(
					stat,
					list);
		}
		list.add(amount);

	}

	public List<Double> getStats(
			final StatValue stat ) {
		return stats.get(stat);
	}

	@Override
	public void reset() {
		stats.clear();

	}

}
