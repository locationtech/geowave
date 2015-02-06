function plotResults(pointsPerColl, pointsPerTile, numQueries, numRuns)

	load('smallQueryRuntimes.txt');
	load('medQueryRuntimes.txt');
	load('largeQueryRuntimes.txt');
	
	smallQueryRuntimes = reshape(smallQueryRuntimes, [numQueries(1), size(pointsPerTile, 2)+1, numRuns]);
	medQueryRuntimes = reshape(medQueryRuntimes, [numQueries(2), size(pointsPerTile, 2)+1, numRuns]);
	largeQueryRuntimes = reshape(largeQueryRuntimes, [numQueries(3), size(pointsPerTile, 2)+1, numRuns]);
	
	featSmallQueryTimes = mean(smallQueryRuntimes(:, 1, :), 3);
	featMedQueryTimes = mean(medQueryRuntimes(:, 1, :), 3);
	featLargeQueryTimes = mean(largeQueryRuntimes(:, 1, :), 3);
	
	collSmallQueryTimes = mean(smallQueryRuntimes(:, 2:end, :), 3);
	collMedQueryTimes = mean(medQueryRuntimes(:, 2:end, :), 3);
	collLargeQueryTimes = mean(largeQueryRuntimes(:, 2:end, :), 3);
		
	% Plot query times
	plotQueryTimes('Small Query Runtime', pointsPerTile, featSmallQueryTimes, collSmallQueryTimes', min(smallQueryRuntimes(:, 2:end, :), [], 3)', max(smallQueryRuntimes(:, 2:end, :), [], 3)', std(smallQueryRuntimes(:, 2:end, :), 0, 3)')
	plotQueryTimes('Medium Query Runtime', pointsPerTile, featMedQueryTimes, collMedQueryTimes', min(medQueryRuntimes(:, 2:end, :), [], 3)', max(medQueryRuntimes(:, 2:end, :), [], 3)', std(medQueryRuntimes(:, 2:end, :), 0, 3)')
	plotQueryTimes('Large Query Runtime', pointsPerTile, featLargeQueryTimes, collLargeQueryTimes', min(largeQueryRuntimes(:, 2:end, :), [], 3)', max(largeQueryRuntimes(:, 2:end, :), [], 3)', std(largeQueryRuntimes(:, 2:end, :), 0, 3)')
end