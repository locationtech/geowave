function plotResults(pointsPerColl, pointsPerTile, numQueries, numRuns)

	rasterSmall = load('./raster/smallQueryRuntimes.txt');
	rasterMed = load('./raster/medQueryRuntimes.txt');
	rasterLarge = load('./raster/largeQueryRuntimes.txt');
	
	vectorSmall = load('./vector/smallQueryRuntimes.txt');
	vectorMed = load('./vector/medQueryRuntimes.txt');
	vectorLarge = load('./vector/largeQueryRuntimes.txt');
	
	rasterSmallQueryRuntimes = reshape(rasterSmall, [numQueries(1), size(pointsPerTile, 2)+1, numRuns]);
	rasterMedQueryRuntimes = reshape(rasterMed, [numQueries(2), size(pointsPerTile, 2)+1, numRuns]);
	rasterLargeQueryRuntimes = reshape(rasterLarge, [numQueries(3), size(pointsPerTile, 2)+1, numRuns]);
	
	vectorSmallQueryRuntimes = reshape(vectorSmall, [numQueries(1), size(pointsPerTile, 2)+1, numRuns]);
	vectorMedQueryRuntimes = reshape(vectorMed, [numQueries(2), size(pointsPerTile, 2)+1, numRuns]);
	vectorLargeQueryRuntimes = reshape(vectorLarge, [numQueries(3), size(pointsPerTile, 2)+1, numRuns]);
	
	rasterFeatSmallQueryTimes = mean(rasterSmallQueryRuntimes(:, 1, :), 3);
	rasterFeatMedQueryTimes = mean(rasterMedQueryRuntimes(:, 1, :), 3);
	rasterFeatLargeQueryTimes = mean(rasterLargeQueryRuntimes(:, 1, :), 3);
	
	vectorFeatSmallQueryTimes = mean(vectorSmallQueryRuntimes(:, 1, :), 3);
	vectorFeatMedQueryTimes = mean(vectorMedQueryRuntimes(:, 1, :), 3);
	vectorFeatLargeQueryTimes = mean(vectorLargeQueryRuntimes(:, 1, :), 3);
	
	rasterCollSmallQueryTimes = mean(rasterSmallQueryRuntimes(:, 2:end, :), 3);
	rasterCollMedQueryTimes = mean(rasterMedQueryRuntimes(:, 2:end, :), 3);
	rasterCollLargeQueryTimes = mean(rasterLargeQueryRuntimes(:, 2:end, :), 3);
	
	vectorCollSmallQueryTimes = mean(vectorSmallQueryRuntimes(:, 2:end, :), 3);
	vectorCollMedQueryTimes = mean(vectorMedQueryRuntimes(:, 2:end, :), 3);
	vectorCollLargeQueryTimes = mean(vectorLargeQueryRuntimes(:, 2:end, :), 3);
		
	% Plot query times
	plotQueryTimes('Small Query Times', 
				   pointsPerTile, 
				   rasterFeatSmallQueryTimes, rasterCollSmallQueryTimes', min(rasterSmallQueryRuntimes(:, 2:end, :), [], 3)', max(rasterSmallQueryRuntimes(:, 2:end, :), [], 3)', std(rasterSmallQueryRuntimes(:, 2:end, :), 0, 3)', 
				   vectorFeatSmallQueryTimes, vectorCollSmallQueryTimes', min(vectorSmallQueryRuntimes(:, 2:end, :), [], 3)', max(vectorSmallQueryRuntimes(:, 2:end, :), [], 3)', std(vectorSmallQueryRuntimes(:, 2:end, :), 0, 3)')
	plotQueryTimes('Medium Query Times', 
	               pointsPerTile, 
				   rasterFeatMedQueryTimes, rasterCollMedQueryTimes', min(rasterMedQueryRuntimes(:, 2:end, :), [], 3)', max(rasterMedQueryRuntimes(:, 2:end, :), [], 3)', std(rasterMedQueryRuntimes(:, 2:end, :), 0, 3)',
				   vectorFeatMedQueryTimes, vectorCollMedQueryTimes', min(vectorMedQueryRuntimes(:, 2:end, :), [], 3)', max(vectorMedQueryRuntimes(:, 2:end, :), [], 3)', std(vectorMedQueryRuntimes(:, 2:end, :), 0, 3)')
	plotQueryTimes('Large Query Times', 
	               pointsPerTile, 
				   rasterFeatLargeQueryTimes, rasterCollLargeQueryTimes', min(rasterLargeQueryRuntimes(:, 2:end, :), [], 3)', max(rasterLargeQueryRuntimes(:, 2:end, :), [], 3)', std(rasterLargeQueryRuntimes(:, 2:end, :), 0, 3)',
				   vectorFeatLargeQueryTimes, vectorCollLargeQueryTimes', min(vectorLargeQueryRuntimes(:, 2:end, :), [], 3)', max(vectorLargeQueryRuntimes(:, 2:end, :), [], 3)', std(vectorLargeQueryRuntimes(:, 2:end, :), 0, 3)')
end