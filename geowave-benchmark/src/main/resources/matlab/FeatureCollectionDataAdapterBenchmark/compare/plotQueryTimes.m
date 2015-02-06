function plotQueryTimes(plotTitle, pointsPerTile, rastFeatQueryTimes, rastCollQueryTimes, rastMinTimes, rastMaxTimes, rastStanDev, vecFeatQueryTimes, vecCollQueryTimes, vecMinTimes, vecMaxTimes, vecStanDev)
	for i=1:size(rastCollQueryTimes, 2)
		myFig = figure;
		semilogx(pointsPerTile, ones(size(pointsPerTile))*rastFeatQueryTimes(i)/1000, 'r--') % feature data - raster
		hold on;
		semilogx(pointsPerTile, ones(size(pointsPerTile))*vecFeatQueryTimes(i)/1000, 'b--') %feature data - vector
		xlim([pointsPerTile(1)*0.8 pointsPerTile(end)*1.2])
		
		text(pointsPerTile(1), rastFeatQueryTimes(i)/1000, sprintf('%.2f', rastFeatQueryTimes(i)/1000))
		text(pointsPerTile(1), vecFeatQueryTimes(i)/1000, sprintf('%.2f', vecFeatQueryTimes(i)/1000))
		
		semilogx(pointsPerTile, rastCollQueryTimes(:,i)./1000, 'rx-')
		semilogx(pointsPerTile, vecCollQueryTimes(:,i)./1000, 'bx-')
		% now plot error bars
		for j=1:size(pointsPerTile, 2)
			semilogx([pointsPerTile(j) pointsPerTile(j)], [(rastCollQueryTimes(j,i)-rastStanDev(j,i))/1000 (rastCollQueryTimes(j,i)+rastStanDev(j,i))/1000], 'm+-')
			
			semilogx([pointsPerTile(j) pointsPerTile(j)], [(vecCollQueryTimes(j,i)-vecStanDev(j,i))/1000 (vecCollQueryTimes(j,i)+vecStanDev(j,i))/1000], 'c+-')
		end
		
		title(sprintf('%s - Query %d', plotTitle, i))
		ylabel('Time in Seconds')
		xlabel('Points Per Tile')
		legend('FeatureData (Raster)', 'FeatureData (Vector)', 'FeatureCollectionData (Raster)', 'FeatureCollectionData (Vector)')
		for j=1:size(pointsPerTile, 2)
			text(pointsPerTile(j), rastCollQueryTimes(j,i)/1000, sprintf('[%.0f, %.2f]', pointsPerTile(j), rastCollQueryTimes(j,i)/1000))
			text(pointsPerTile(j), vecCollQueryTimes(j,i)/1000, sprintf('[%.0f, %.2f]', pointsPerTile(j), vecCollQueryTimes(j,i)/1000))
		end
		
		hold off;
		saveas(myFig, sprintf('%s - Query %d', plotTitle, i), 'png');
	end
end