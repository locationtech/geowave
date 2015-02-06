% use this function to plot multiple histograms automatically
function plotHists(tileSizes, queryNums)
	figure
	colors = hsv(size(tileSizes, 1));
	hold on
	for i=1:size(tileSizes, 1)
		plotHist(tileSizes(i), queryNums(i), '-', colors(i,:))
	end
	hold off
end