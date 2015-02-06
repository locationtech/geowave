function plotQueryTimes(plotTitle, pointsPerTile, featQueryTimes, collQueryTimes, minTimes, maxTimes, vars)
	for i=1:size(collQueryTimes, 2)
		myFig = figure;
		semilogx(pointsPerTile, ones(size(pointsPerTile))*featQueryTimes(i)/1000, '--')
		xlim([pointsPerTile(1)*0.8 pointsPerTile(end)*1.2])
		text(pointsPerTile(1), featQueryTimes(i)/1000, sprintf('%.2f', featQueryTimes(i)/1000))
		hold on;
		semilogx(pointsPerTile, collQueryTimes(:,i)./1000, 'rx-')
		% now plot error bars
		for j=1:size(pointsPerTile, 2)
			semilogx([pointsPerTile(j) pointsPerTile(j)], [(collQueryTimes(j,i)-vars(j,i))/1000 (collQueryTimes(j,i)+vars(j,i))/1000], 'm+-')
		end
		title(sprintf('%s - Query %d', plotTitle, i))
		ylabel('Time in Seconds')
		xlabel('Points Per Tile')
		legend('FeatureData', 'FeatureCollectionData')
		for j=1:size(pointsPerTile, 2)
			text(pointsPerTile(j), collQueryTimes(j,i)/1000, sprintf('[%.0f, %.2f]', pointsPerTile(j), collQueryTimes(j,i)/1000))
		end
		hold off;
		saveas(myFig, sprintf('%s - Query %d', plotTitle, i), 'png');
	end
end