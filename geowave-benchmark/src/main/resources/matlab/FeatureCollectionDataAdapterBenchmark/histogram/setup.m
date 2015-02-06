% this will setup the environment for looking at histograms

global hists = [];
global featsPerQuery = [];
global runtimes = [];
global tileSizes = [100; 500; 1000; 5000; 10000; 50000];

[hists, featsPerQuery, runtimes] = loadHistograms();
