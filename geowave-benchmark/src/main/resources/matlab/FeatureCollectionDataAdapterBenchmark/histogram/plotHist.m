% use this function to plot a single histogram
%  - assumes runtimes, featsPerQuery, tileSizes and hists are loaded
function plotHist(tileSize, queryNum, lineFormat, color)
	global tileSizes;
	global hists;
	global featsPerQuery;
	global runtimes;
	
	hist = hists{find(tileSizes == tileSize), queryNum};
	x = (0:(size(hist, 1)-1))*2;
	
	if nargin == 2
		p1 = plot(x, hist / sum(hist));
	elseif nargin == 3
		p1 = plot(x, hist / sum(hist), lineFormat);
	elseif nargin == 4
		p1 = plot(x, hist / sum(hist), lineFormat, 'Color', color);
	end
	
	xlabel('Features Per Collection')
	
	% get the current title for the figure
	t = get(get(gca,'Title'),'String');
		
	% update the title
	title(
		sprintf('%sQuery %d - %d Features - %.2fs - Tile Size %d\n', 
				t, 
				queryNum, 
				featsPerQuery(queryNum), 
				runtimes(queryNum, find(tileSizes == tileSize))/1000, 
				tileSize))
	
	% Get object handles
	[LEGH,OBJH,OUTH,OUTM] = legend; 
	legend([OUTH p1],OUTM,sprintf('Query %d - %d', queryNum, tileSize))
end