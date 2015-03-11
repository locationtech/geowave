% use this code to load all of the histograms in the directory
function [hists, featsPerQuery, runtimes] = loadHistograms(loadFullData)
	global tileSizes;
	
	if nargin < 1
		loadFullData = false;
	end
	
	% load the number of features returned for each query
	featsPerQuery = load('large-results.txt');
	
	% load the runtimes
	load('largeQueryRuntimes.txt');
	largeQueryRuntimes = reshape(largeQueryRuntimes, [size(featsPerQuery, 1), size(tileSizes, 1)+1, 10]);
	runtimes = mean(largeQueryRuntimes(:, 2:end, :), 3);
	
	% initialize data for numTiles x numQueries
	hists = cell(size(tileSizes, 1), size(featsPerQuery, 1));

	% load each histogram
	if ~loadFullData
		for i=1:size(tileSizes, 1)
			for j=1:size(featsPerQuery, 1)
				hists(i, j) = load(sprintf('./data/hist_%d_%d.txt', j, tileSizes(i)));
			end
		end
	else
		% load the unfiltered histograms
		for i=1:size(tileSizes, 1)
			for j=1:size(featsPerQuery, 1)
				hists(i, j) = load(sprintf('./data/hist_full_%d_%d.txt', j, tileSizes(i)));
			end
		end
	end
end