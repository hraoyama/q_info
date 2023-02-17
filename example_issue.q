system "l ",getenv[`KDB_LIB]; 
system "l ",getenv[`BLUE_DIR],"/src/q/stat.q";  
system "l ",getenv[`BLUE_DIR],"/src/q/feature_building.q";
system "l ",getenv[`BLUE_DIR],"/src/q/utils.q";


min[select min[date] from trades]


count[select from trades where date=2017.05.03]=0
count[select from books where date=2017.05.03]=0

dateStart:2017.05.01;
dateEnd:2017.05.30;  
activeContractsEachDay: {x,y} over { : 0! select first[sym], first[date], first[Volume] by ssym from 
                                                (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from 
                                                        trades where date=x) where Volume=(max;Volume) fby ssym; } each (dateStart + til (dateEnd-dateStart+1));
allSymDates:(select distinct sym, date from activeContractsEachDay);
// just pick one date
symDate:allSymDates[6]

s:symDate[`sym];
d:symDate[`date];
relevantData: 0! update rebaseVol: (first[Qty],first[Qty] + 1_ deltas Volume), Price: $[`float;Price], trTime:time from (select from trades where sym=s, date=d, time within (07:30;17:15));
relevantData2: update bkTime:time from (select from books where sym=s, date=d, time within (07:30;17:15));
tradesWithBook: aj[`date`sym`time;relevantData; 0! relevantData2];
// count[select from tradesWithBook where trTime >= bkTime] = count[tradesWithBook]; // time is the trade time
tradesWithBook: get_up_down_trade_assignment[tradesWithBook];