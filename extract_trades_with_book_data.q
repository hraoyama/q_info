
system "l ",getenv[`KDB_LIB]; //  E:/beetroot;
system "l ",getenv[`BLUE_DIR],"/src/q/stat.q";  // D:\\Code\\ProjectBlue\\src\\q\\stat.q 
system "l ",getenv[`BLUE_DIR],"/src/q/utils.q";


availableDates: select distinct date from trades;
max[availableDates]

dateStart:2019.10.29;
// dateEnd:2017.06.10;  
dateEnd:2019.11.04;

activeContractsEachDay: {x,y} over { : 0! select first[sym], first[date], first[Volume] by ssym from 
                                                (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from 
                                                        trades where date=x) where Volume=(max;Volume) fby ssym; } each (dateStart + til (dateEnd-dateStart+1));


nMarkovStates:5;
twqFESX: {x,y} over { [nMarkovStates; x] :TradesWithQuotes[x; `FESX201912; nMarkovStates]; }[nMarkovStates;] each exec distinct date from activeContractsEachDay;
// count[twqFESX]
// select distinct state from twqFESX;
// save `:D:/Code/ProjectBlue/src/python/tf_deeplearning/resources/twqFESX.csv