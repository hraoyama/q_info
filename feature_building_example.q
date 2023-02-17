system "l ",getenv[`KDB_LIB]; //  E:/beetroot;
system "l ",getenv[`BLUE_DIR],"/src/q/stat.q";  // D:\\Code\\ProjectBlue\\src\\q\\stat.q 
system "l ",getenv[`BLUE_DIR],"/src/q/feature_building.q";
system "l ",getenv[`BLUE_DIR],"/src/q/utils.q";

dateStart:2017.05.29;
// dateEnd:2017.06.10;  
dateEnd:2021.06.10;
activeContractsEachDay: {x,y} over { : 0! select first[sym], first[date], first[Volume] by ssym from 
                                                (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from 
                                                        trades where date=x) where Volume=(max;Volume) fby ssym; } each (dateStart + til (dateEnd-dateStart+1));

// run this code if you need to create daily summaries
    / dailySummaries: makeDailySummaryWithSPR[activeContractsEachDay];
// run this code if the daily summaries were already stored on the KDB server
dailySummaries: select from daily; 

// symDate:(select distinct sym, date from activeContractsEachDay)[0];
barSummaries: {x,y} over makeBarSecondSummaryFuncA[30;08:00;17:15;] each (select distinct sym, date from activeContractsEachDay);

// `:E:/celeriac/barX set .Q.en[`:E:/celeriac] barXhansr;
// .Q.dpft[hsym[`:E:/celeriac];2017.05.29;`sym;`barXhansr];
