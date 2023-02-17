\l E:/beetroot/

tables[]


dateStart:2017.05.29;
dateEnd:2017.06.10;
getMostActiveDataOnly: { 
   currentSymbolsForDay : 0! select first[sym], first[date], first[Volume] by ssym from (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from trades where date=x) where Volume=(max;Volume) fby ssym; 
   onlyMostActiveSymbolsData: select date, sym, time, Price, Qty, Volume from trades where date=x, sym in currentSymbolsForDay`sym ;
   :onlyMostActiveSymbolsData;
   };
activelyTradedFuturesTrades: {x,y} over (getMostActiveDataOnly each (dateStart + til (dateEnd-dateStart+1)));
activeContractsEachDay: {x,y} over { : 0! select first[sym], first[date], first[Volume] by ssym from (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from trades where date=x) where Volume=(max;Volume) fby ssym; } each (dateStart + til (dateEnd-dateStart+1));
activelyTradedFuturesBooks: {x,y} over (activeContractsEachDay`sym)  { [x;y] :select from books where date=y, sym=x; }'  (activeContractsEachDay`date);

select distinct date from activeContractsEachDay;
count[activelyTradedFuturesBooks];
count[activelyTradedFuturesTrades];

`:D:/data/sampleData/activelyTradedFuturesTrades.csv 0: csv 0: activelyTradedFuturesTrades;
`:D:/data/sampleData/activelyTradedFuturesBooks.csv 0: csv 0: select from activelyTradedFuturesBooks where date within(dateStart,dateStart+2);

// select distinct date from activelyTradedFuturesTrades where date within(dateStart,dateStart+2)