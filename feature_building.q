// Ctrl-R to run selection in Jo Shinomune's q plugin for Visual Studio Code
// https://github.com/jshinonome/vscode-q


// assumes beetroot connected

prevWeekday: {[d] $[2=d mod 7;d-3; $[1=d mod 7;d-2;d-1]]};
nextWeekday: {[d] $[6=d mod 7;d+3; $[0=d mod 7;d+2;d+1]]};
dayOfWeek: { [d] `Sat`Sun`Mon`Tue`Wed`Thu`Fri d mod 7 };
rollWindow: { [f;w;s] f each {1_x,y}\[w#0;s]  }; // replaces the first w values with zero
ema_local: { first[y](1-x)\x*y };
ema2_local: { {z+x*y}\[first y; 1-x; x*y] };
roundDp: {  (1%(10 xexp y))*(`long$(x*(10 xexp y)) ) };
isSummertime : { [d]
    dm: `mm$d;
    isSummer: $[ dm within (4;9);1b;0b];
    if[ (dm=3) or (dm=10);
         canBeLastSun: ("D"$("." sv string (`year$d;dm;22)))  + til 11;
         datesToConfirm:  canBeLastSun[ where { x=`mm$y }[dm;] each canBeLastSun ];
         lastSun: datesToConfirm[ (-1)# where (dayOfWeek each datesToConfirm)=`Sun];
         isSummer: (((d > lastSun) & (dm=3)) or ((d < lastSun) & (dm=10)))[0];
    ];
    :isSummer;
    };

// https://www.macroption.com/rsi-calculation/
// http://chart-formations.com/indicators/pivot-calculator.aspx?cat=supres 
standardRSI: { [upValues;downValues; colNameStartString ;periodOfEMA]
    if[(count upValues)>periodOfEMA;
        em_up: ema_local[2%periodOfEMA+1;upValues];
        em_down: ema_local[2%periodOfEMA+1;downValues] ;
        rsi: sqrt[rollWindow[ {avg x*x} ; periodOfEMA; em_up] - rollWindow[ {(avg x) xexp 2}; periodOfEMA; em_up] ] % sqrt[rollWindow[ {avg x*x} ; periodOfEMA; em_down] - rollWindow[ {(avg x) xexp 2}; periodOfEMA; em_down] ];
        rsi :100 - 100%(1.0+rsi);
        rsi: 50f,@ [rsi;til (periodOfEMA-1); : ;50f];
    : flip (enlist `$(colNameStartString,string[periodOfEMA])) ! enlist rsi;
    ];
    :flip (enlist `$(colNameStartString, string[periodOfEMA])) ! enlist (1 + count upValues)#50f;
    };


bbgRSI: { [upValues;downValues;colNameStartString;periodOfEMA]
    if[(count upValues)>periodOfEMA;
        upValues_p: rollWindow[avg;periodOfEMA; upValues];
        downValues_p: rollWindow[avg;periodOfEMA;downValues];
        uuu_p_mma: upValues_p;
        ddd_p_mma: downValues_p;
        uuu_p_mma[til periodOfEMA] : avg uuu_p_mma[til periodOfEMA];
        uuu_p_mma[periodOfEMA]: (upValues[periodOfEMA]+((upValues_p[periodOfEMA-1]*(periodOfEMA-1))))%periodOfEMA;
        uuu_p_mma[(periodOfEMA + 1)_ til count uuu_p_mma] : upValues[(periodOfEMA + 1)_ til count uuu_p_mma];
        uuu_p_mma[(periodOfEMA + 1)_ til count uuu_p_mma] : (1)_ema2_local[1f%(periodOfEMA) ;uuu_p_mma[(periodOfEMA)_ til count uuu_p_mma]];
        ddd_p_mma[til periodOfEMA] : avg ddd_p_mma[til periodOfEMA];
        ddd_p_mma[periodOfEMA]: (downValues[periodOfEMA]+((downValues_p[periodOfEMA-1]*(periodOfEMA-1))))%periodOfEMA;
        ddd_p_mma[(periodOfEMA + 1)_ til count ddd_p_mma] : downValues[(periodOfEMA + 1)_ til count ddd_p_mma];
        ddd_p_mma[(periodOfEMA + 1)_ til count ddd_p_mma] : (1)_ema2_local[1f%periodOfEMA;ddd_p_mma[(periodOfEMA)_ til count ddd_p_mma]];
        rsi:100 - 100%(1.0-uuu_p_mma%ddd_p_mma);
        rsi:rsi[0],rsi;
        rsi: @[rsi;til (periodOfEMA); : ;50f];
        : flip (enlist `$(colNameStartString, string[periodOfEMA])) ! enlist rsi;
    ];
    :flip (enlist `$(colNameStartString, string[periodOfEMA])) ! enlist (1 + count upValues)#50f;
    };

upDownBaseIdxsForEMA: { [df;colName]
    changes: 1_deltas df[colName];
    posIdxsChanges: where changes>=0.0;
    negIdxsChanges: where changes<0.0;
    upIdxs: (count[changes])#0f;
    downIdxs: upIdxs;
    upIdxs: @[upIdxs;posIdxsChanges;:;changes@posIdxsChanges];
    downIdxs: @[downIdxs;negIdxsChanges;:;changes@negIdxsChanges];
    :(`uix`dix ! (upIdxs;downIdxs));
    };

addDayRSI : {  [ohlcData; emePeriodsForRSI]
    :{x,y} over {  
        [ohlcData; emaPeriodsForRSI;symLocal]
        sData: `date xasc select from ohlcData where sym=symLocal;
        openUpDown: upDownBaseIdxsForEMA[sData;`open];
        closeUpDown: upDownBaseIdxsForEMA[sData;`close];
        sData: sData ^ {x^y} over standardRSI[openUpDown[`uix];openUpDown[`dix];"rsi_day_open_";] each emaPeriodsForRSI;
        sData: sData ^ {x^y} over standardRSI[closeUpDown[`uix];closeUpDown[`dix];"rsi_day_close_";] each emaPeriodsForRSI;
        sData: sData ^ {x^y} over bbgRSI[closeUpDown[`uix];closeUpDown[`dix];"bbg_rsi_day_close_";] each emaPeriodsForRSI;
        :sData;
        }[ohlcData; emePeriodsForRSI; ] each (exec distinct sym from ohlcData);
    };

// test (source stat.q)
// td: flip `open`close`sym`date!(100 + rnorm[100];100.25+rnorm[100];`TESTSYM;.z.D + til 100) ; 
// addDayRSI[td; [6 13]]
// http://chart-formations.com/indicators/pivot-calculator.aspx?cat=supres 

addBBGFormulas : { 
    x : update true_range_day:(x[`high] |' @[x[`prev_close]; where null[x[`prev_close]];:; x[`high][where null[x[`prev_close]]]]) - (x[`low] &' @[x[`prev_close]; where null[x[`prev_close]];:; x[`low][where null[x[`prev_close]]]]) from x;
    x : update pivP: (high+low+close)%3 from x;
    x : update pivR1: (2*pivP)-low, pivR2: pivP + (high-low) from x;
    x : update pivS1: (2*pivP)-high, pivS2: pivP - (high-low) from x;
    x : update pivR3: (pivP-pivS2)+pivR2, pivS3:pivP - (pivR2-pivS2) from x;
    :x;
    };

makeDailySummary: {  [symDate]
        s:symDate[`sym];
        d:symDate[`date];
        relevantData: 0! update rebaseVol: (first[Qty],first[Qty] + 1_ deltas Volume), Price: $[`float;Price] from (select from trades where sym=s, date=d, time within (07:30;17:15));
        dailyohlc: flip ( `date`sym ! (enlist d; enlist s) ),{`open`high`low`close!('[enlist;first];'[enlist;max];'[enlist;min];'[enlist;last])@\:x} relevantData[`Price];
        vd: exec sum[Qty] by Price from relevantData;
        numPxs: count[vd];
        tick: min[1_deltas key[vd]];
        modePx: mode[relevantData[`Price]][0];
        maxVPx: (key[vd] where value[vd] = max[ value vd])[0];
        percMaxPx: vd[maxVPx]%sum[value vd];
        percArndMaxVPx: (exec sum[Qty] from relevantData where Price in key[vd][where key[vd] within (maxVPx-tick;maxVPx+tick)]) % sum[value vd];
        percArndModePx: $[modePx=maxVPx;percArndMaxVPx;(exec sum[Qty] from relevantData where Price in key[vd][where key[vd] within (modePx-tick;modePx+tick)])%sum[value vd]];
        dailyd2: flip ( `totVolume`vwPx`numPxs`modePx`percArndModePx`maxVPx`percMaxPx`percArndMaxVPx ! 
                    (enlist sum[relevantData[`rebaseVol]]; enlist sum[relevantData[`Qty]*relevantData[`Price]]%sum[relevantData[`Qty]]; 
                    enlist numPxs; enlist modePx; enlist percArndModePx; enlist maxVPx; enlist percMaxPx; enlist percArndMaxVPx)  );
        dSummary: 0! dailyohlc^dailyd2;          
        :dSummary;
    };

makeDailySummaryWithSPR: { [activeContractsEachDay]
     dailySummaries: `sym`date xasc {x,y} over makeDailySummary each (select distinct sym, date from activeContractsEachDay);
     dailySummaries: update prev_close:(prev; close) fby sym from dailySummaries;
     :{x,y} over addBBGFormulas each { [x;y] :select from x where sym=y; }[dailySummaries;] each (exec distinct sym from dailySummaries);  
    };

// barSeconds:30 ; startTime: 08:00:00; endTime: 17:15:00; symDate: (select distinct sym, date from activeContractsEachDayLocal where i=1)[0]
makeBarSecondSummaryFuncA : {  [barSeconds;startTime;endTime;symDate]
        s:symDate[`sym];
        d:symDate[`date];
        show[symDate];
        relevantData: 0! update rebaseVol: (first[Qty],first[Qty] + 1_ deltas Volume), Price: $[`float;Price], trTime:time from (select from trades where sym=s, date=d, time within (startTime;endTime));
        relevantData2: update bkTime:time from (select from books where sym=s, date=d, time within (startTime;endTime));
        tradesWithBook: aj[`date`sym`time;relevantData; 0! relevantData2];
        // count[select from tradesWithBook where trTime >= bkTime] = count[tradesWithBook]; // time is the trade time
        tradesWithBook: get_up_down_trade_assignment[tradesWithBook];

        barTrades: 0! `barTime xasc select date:d, sym:s, open:first[Price], high:max[Price], low:min[Price], close:last[Price],
                                                    totSize:sum[Qty],vwap:sum[Price*Qty]%sum[Qty],maxSize: max[Qty],minSize: min[Qty],
                                                    medSize: med[Qty], avgSize: avg[Qty], numTrades: count[Qty],
                                                    hhi: sum[(Qty*Qty)%(sum[Qty]*sum[Qty])],
                                                    imb1: (last[Ask_Qty_Lev_0]-last[Bid_Qty_Lev_0])%(last[Ask_Qty_Lev_0]+last[Bid_Qty_Lev_0]), 
                                                    imb2: (last[Ask_Qty_Lev_0]+last[Ask_Qty_Lev_1]-last[Bid_Qty_Lev_0]-last[Bid_Qty_Lev_1])%(last[Ask_Qty_Lev_0]+last[Ask_Qty_Lev_1]+last[Bid_Qty_Lev_0]+last[Bid_Qty_Lev_1]), 
                                                    imbAvg1: (avg[Ask_Qty_Lev_0]-avg[Bid_Qty_Lev_0])%(avg[Ask_Qty_Lev_0]+avg[Bid_Qty_Lev_0]), 
                                                    imbAvg2: (avg[Ask_Qty_Lev_0]+avg[Ask_Qty_Lev_1]-avg[Bid_Qty_Lev_0]-avg[Bid_Qty_Lev_1])%(avg[Ask_Qty_Lev_0]+avg[Ask_Qty_Lev_1]+avg[Bid_Qty_Lev_0]+avg[Bid_Qty_Lev_1]),
                                                    openUp:first[((dir=`up)*Price) except 0], highUp:max[((dir=`up)*Price) except 0], lowUp:min[((dir=`up)*Price) except 0], closeUp:last[((dir=`up)*Price) except 0], 
                                                    highUpSize:max[((dir=`up)*Qty) except 0], lowUpSize:min[((dir=`up)*Qty) except 0], 
                                                    totSizeUp:sum[(dir=`up)*Qty]%sum[dir=`up], vwapUp: sum[(dir=`up)*Price*Qty]%sum[(dir=`up)*Qty], maxUpSize: max[(dir=`up)*Qty], minUpSize: min[(dir=`up)*Qty],
                                                    medUpSize: med[((dir=`up)*Qty) except 0], avgUpSize: avg[((dir=`up)*Qty) except 0], numUpTrades: sum[dir=`up],
                                                    hhiUp: sum[((dir=`up)*Qty*Qty)%(sum[(dir=`up)*Qty]*sum[(dir=`up)*Qty])],
                                                    openDown:first[((dir=`down)*Price) except 0], highDown:max[((dir=`down)*Price) except 0], lowDown:min[((dir=`down)*Price) except 0], closeDown:last[((dir=`down)*Price) except 0], 
                                                    highDownSize:max[((dir=`down)*Qty) except 0], lowDownSize:min[((dir=`down)*Qty) except 0],
                                                    totSizeDown:sum[(dir=`down)*Qty]%sum[dir=`down], vwapDown: sum[(dir=`down)*Price*Qty]%sum[(dir=`down)*Qty], maxDownSize: max[(dir=`down)*Qty], minDownSize: min[(dir=`down)*Qty],
                                                    medDownSize: med[((dir=`down)*Qty) except 0], avgDownSize: avg[((dir=`down)*Qty) except 0], numDownTrades: sum[dir=`down],
                                                    hhiDown: sum[((dir=`down)*Qty*Qty)%(sum[(dir=`down)*Qty]*sum[(dir=`down)*Qty])],
                                                    lastBidQtyLev0: last[Bid_Qty_Lev_0],
                                                    lastBidQtyLev1: last[Bid_Qty_Lev_1],
                                                    AvgBidQtyLev0: avg[Bid_Qty_Lev_0],
                                                    AvgBidQtyLev1: avg[Bid_Qty_Lev_1],
                                                    lastAskQtyLev0:last[Ask_Qty_Lev_0],
                                                    lastAskQtyLev1:last[Ask_Qty_Lev_1],
                                                    AvgAskQtyLev0:avg[Ask_Qty_Lev_0],
                                                    AvgAskQtyLev1:avg[Ask_Qty_Lev_1]
                                            by barTime:(barSeconds xbar `second$time)  from tradesWithBook;

        barTrades: update time:date+barTime, 
                        barTime:date+barTime,
                        hhiToEven: hhi-1%(numTrades),
                        hhiAvgSize: sum[avgSize*avgSize]%(totSize*totSize),
                        hhiAvgSizeToEven: sum[avgSize*avgSize]%(totSize*totSize)-1%numTrades,
                        avgSizeToBid2: avgSize%(lastBidQtyLev0+lastBidQtyLev1),
                        avgSizeToBid1: avgSize%lastBidQtyLev0,
                        avgSizeToAsk2: avgSize%(lastAskQtyLev0+lastAskQtyLev1),
                        avgSizeToAsk1: avgSize%lastAskQtyLev0,
                        avgSizeToAvgBid2: avgSize%(AvgBidQtyLev0+AvgBidQtyLev1),
                        avgSizeToAvgBid1: avgSize%AvgBidQtyLev0,
                        avgSizeToAvgAsk2: avgSize%(AvgAskQtyLev0+AvgAskQtyLev1),
                        avgSizeToAvgAsk1: avgSize%AvgAskQtyLev0,
                        medSizeToBid2: medSize%(lastBidQtyLev0+lastBidQtyLev1),
                        medSizeToBid1: medSize%lastBidQtyLev0,
                        medSizeToAsk2: medSize%(lastAskQtyLev0+lastAskQtyLev1),
                        medSizeToAsk1: medSize%lastAskQtyLev0,
                        medSizeToAvgBid1: medSize%AvgBidQtyLev0,
                        medSizeToAvgAsk1: medSize%AvgAskQtyLev0,
                        hhiUptoEven: hhiUp-1%(numUpTrades),
                        hhiUpAvgSize: sum[avgUpSize*avgUpSize]%(totSizeUp*totSizeUp),
                        hhiUpAvgSizeToEven: sum[avgUpSize*avgUpSize]%(totSizeUp*totSizeUp)-1%numUpTrades,
                        avgUpSizeToBid2: avgUpSize%(lastBidQtyLev0+lastBidQtyLev1),
                        avgUpSizeToBid1: avgUpSize%lastBidQtyLev0,
                        avgUpSizeToAsk2: avgUpSize%(lastAskQtyLev0+lastAskQtyLev1),
                        avgUpSizeToAsk1: avgUpSize%lastAskQtyLev0,
                        avgUpSizeToAvgBid2: avgUpSize%(AvgBidQtyLev0+AvgBidQtyLev1),
                        avgUpSizeToAvgBid1: avgUpSize%AvgBidQtyLev0,
                        avgUpSizeToAvgAsk2: avgUpSize%(AvgAskQtyLev0+AvgAskQtyLev1),
                        avgUpSizeToAvgAsk1: avgUpSize%AvgAskQtyLev0,
                        medUpSizeToBid2: medUpSize%(lastBidQtyLev0+lastBidQtyLev1),
                        medUpSizeToBid1: medUpSize%lastBidQtyLev0,
                        medUpSizeToAsk2: medUpSize%(lastAskQtyLev0+lastAskQtyLev1),
                        medUpSizeToAsk1: medUpSize%lastAskQtyLev0,
                        medUpSizeToAvgBid1: medUpSize%AvgBidQtyLev0,
                        medUpSizeToAvgAsk1: medUpSize%AvgAskQtyLev0,
                        hhiDowntoEven: hhiDown-1%(numDownTrades),
                        hhiDownAvgSize: sum[avgDownSize*avgDownSize]%(totSizeDown*totSizeDown),
                        hhiDownAvgSizeToEven: sum[avgDownSize*avgDownSize]%(totSizeDown*totSizeDown)-1%numDownTrades,
                        avgDownSizeToBid2: avgDownSize%(lastBidQtyLev0+lastBidQtyLev1),
                        avgDownSizeToBid1: avgDownSize%lastBidQtyLev0,
                        avgDownSizeToAsk2: avgDownSize%(lastAskQtyLev0+lastAskQtyLev1),
                        avgDownSizeToAsk1: avgDownSize%lastAskQtyLev0,
                        avgDownSizeToAvgBid2: avgDownSize%(AvgBidQtyLev0+AvgBidQtyLev1),
                        avgDownSizeToAvgBid1: avgDownSize%AvgBidQtyLev0,
                        avgDownSizeToAvgAsk2: avgDownSize%(AvgAskQtyLev0+AvgAskQtyLev1),
                        avgDownSizeToAvgAsk1: avgDownSize%AvgAskQtyLev0,
                        medDownSizeToBid2: medDownSize%(lastBidQtyLev0+lastBidQtyLev1),
                        medDownSizeToBid1: medDownSize%lastBidQtyLev0,
                        medDownSizeToAsk2: medDownSize%(lastAskQtyLev0+lastAskQtyLev1),
                        medDownSizeToAsk1: medDownSize%lastAskQtyLev0,
                        medDownSizeToAvgBid1: medDownSize%AvgBidQtyLev0,
                        medDownSizeToAvgAsk1: medDownSize%AvgAskQtyLev0
                        by barTime from barTrades;
        // barTrades now has summaries of data in barSeconds intervals

        tradesWithBookAndBar: aj[`date`sym`time;tradesWithBook; 0! barTrades];
        / count[select from tradesWithBookAndBar where time > barTime] = count[tradesWithBookAndBar];  / barTime is the time before the trade time!

        // note that this is look-ahead so only applicable to a barTime in the future!
        // x|0f is equivalent to max[0f;x]
        tradesWithBookAndBar: 
            update adjSizeMed3:(Qty-(medSize%3))|0f, adjSizeMed2: (Qty-(medSize%2))|0f, adjSizeMed1: (Qty-medSize)|0f,
                   adjSizeAvg3:(Qty-(avgSize%3))|0f, adjSizeAvg2: (Qty-(avgSize%2))|0f, adjSizeAvg1: (Qty-avgSize)|0f,
                   adjUpSizeMed3:(((dir=`up)*Qty)-(medUpSize%3))|0f, adjUpSizeMed2: (((dir=`up)*Qty)-(medUpSize%2))|0f, adjUpSizeMed1: (((dir=`up)*Qty)-medUpSize)|0f,
                   adjUpSizeAvg3:(((dir=`up)*Qty)-(avgUpSize%3))|0f, adjUpSizeAvg2: (((dir=`up)*Qty)-(avgUpSize%2))|0f, adjUpSizeAvg1: (((dir=`up)*Qty)-avgUpSize)|0f,
                   adjDownSizeMed3:(((dir=`down)*Qty)-(medDownSize%3))|0f, adjDownSizeMed2: (((dir=`down)*Qty)-(medDownSize%2))|0f, adjDownSizeMed1: (((dir=`down)*Qty)-medDownSize)|0f,
                   adjDownSizeAvg3:(((dir=`down)*Qty)-(avgDownSize%3))|0f, adjDownSizeAvg2: (((dir=`down)*Qty)-(avgDownSize%2))|0f, adjDownSizeAvg1: (((dir=`down)*Qty)-avgDownSize)|0f
                 from tradesWithBookAndBar;
        
        // note that the hhi related stuff is look-ahead so only applicable a barTime in the future!
        tradesWithBookAndBar: 
            update  hhiMed3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeMed3) fby barTime ),  
                    hhiMed2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeMed2) fby barTime ),  
                    hhiMed1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeMed1) fby barTime ),  
                    hhiAvg3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeAvg3) fby barTime ),   
                    hhiAvg2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeAvg2) fby barTime ),   
                    hhiAvg1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjSizeAvg1) fby barTime ),   
                    
                    hhiUpMed3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeMed3) fby barTime ),  
                    hhiUpMed2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeMed2) fby barTime ),  
                    hhiUpMed1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeMed1) fby barTime ),  
                    hhiUpAvg3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeAvg3) fby barTime ),   
                    hhiUpAvg2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeAvg2) fby barTime ),   
                    hhiUpAvg1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjUpSizeAvg1) fby barTime ),   

                    hhiDownMed3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeMed3) fby barTime ),  
                    hhiDownMed2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeMed2) fby barTime ),  
                    hhiDownMed1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeMed1) fby barTime ),  
                    hhiDownAvg3:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeAvg3) fby barTime ),   
                    hhiDownAvg2:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeAvg2) fby barTime ),   
                    hhiDownAvg1:( ({ sum[(x*x)%(sum[x]*sum[x])] }; adjDownSizeAvg1) fby barTime ),   

                    imbTr1: (Ask_Qty_Lev_0-Bid_Qty_Lev_0)%(Ask_Qty_Lev_0+Bid_Qty_Lev_0),
                    imbTr2: (Ask_Qty_Lev_0+Ask_Qty_Lev_1-Bid_Qty_Lev_0-Bid_Qty_Lev_1)%(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Bid_Qty_Lev_0+Bid_Qty_Lev_1)
            from tradesWithBookAndBar;

        tradesWithBookAndBar: 
            update  hhiMed3ToEven: hhiMed3-1%(numTrades),  
                    hhiMed2ToEven: hhiMed2-1%(numTrades),
                    hhiMed1ToEven: hhiMed1-1%(numTrades),
                    hhiAvg3ToEven: hhiAvg3-1%(numTrades), 
                    hhiAvg2ToEven: hhiAvg2-1%(numTrades), 
                    hhiAvg1ToEven: hhiAvg1-1%(numTrades),

                    hhiUpMed3ToEven: hhiUpMed3-1%(numUpTrades),  
                    hhiUpMed2ToEven: hhiUpMed2-1%(numUpTrades),
                    hhiUpMed1ToEven: hhiUpMed1-1%(numUpTrades),
                    hhiUpAvg3ToEven: hhiUpAvg3-1%(numUpTrades), 
                    hhiUpAvg2ToEven: hhiUpAvg2-1%(numUpTrades), 
                    hhiUpAvg1ToEven: hhiUpAvg1-1%(numUpTrades),

                    hhiDownMed3ToEven: hhiDownMed3-1%(numDownTrades),  
                    hhiDownMed2ToEven: hhiDownMed2-1%(numDownTrades),
                    hhiDownMed1ToEven: hhiDownMed1-1%(numDownTrades),
                    hhiDownAvg3ToEven: hhiDownAvg3-1%(numDownTrades), 
                    hhiDownAvg2ToEven: hhiDownAvg2-1%(numDownTrades), 
                    hhiDownAvg1ToEven: hhiDownAvg1-1%(numDownTrades)
            from tradesWithBookAndBar;

        // clean up date you will not need        
        tradesWithBookAndBar: (`lastBidQtyLev0`lastBidQtyLev1`AvgBidQtyLev0`AvgBidQtyLev1`lastAskQtyLev0`lastAskQtyLev1`AvgAskQtyLev0`AvgAskQtyLev1)_tradesWithBookAndBar;

        barDataOnly: (`time`Price`Qty`Volume`rebaseVol`trTime`bkTime`dir)_tradesWithBookAndBar;
        barDataOnly: (`Bid_Px_Lev_0`Bid_Px_Lev_1`Bid_Px_Lev_2`Bid_Px_Lev_3`Bid_Px_Lev_4)_barDataOnly;
        barDataOnly: (`Ask_Px_Lev_0`Ask_Px_Lev_1`Ask_Px_Lev_2`Ask_Px_Lev_3`Ask_Px_Lev_4)_barDataOnly;
        barDataOnly: (`Bid_Qty_Lev_0`Bid_Qty_Lev_1`Bid_Qty_Lev_2`Bid_Qty_Lev_3`Bid_Qty_Lev_4)_barDataOnly;
        barDataOnly: (`Ask_Qty_Lev_0`Ask_Qty_Lev_1`Ask_Qty_Lev_2`Ask_Qty_Lev_3`Ask_Qty_Lev_4)_barDataOnly;
        barDataOnly: (`adjSizeMed3`adjSizeMed2`adjSizeMed1`adjSizeAvg3`adjSizeAvg2`adjSizeAvg1`adjUpSizeMed3`adjUpSizeMed2`adjUpSizeMed1)_barDataOnly;
        barDataOnly: (`adjUpSizeAvg3`adjUpSizeAvg2`adjUpSizeAvg1`adjDownSizeMed3`adjDownSizeMed2`adjDownSizeMed1`adjDownSizeAvg3`adjDownSizeAvg2`adjDownSizeAvg1)_barDataOnly;
        barDataOnly: (`imbTr1`imbTr2)_barDataOnly;
        barDataOnly: distinct[barDataOnly];
        barDataOnly: update time:barTime from barDataOnly;
        barDataOnly: `date`sym`time xcols enlist[`barTime]_barDataOnly;  

        barDataOnly[`date]: `date$barDataOnly[`date];
        :0! barDataOnly;
        // return tradesWithBookAndBar if you want every trade with a look forward of the information in that interval
                // :tradesWithBookAndBar;

    };




/ I (aa Fo
/ a | : * [fcc sinew 2 £3]
/ day by day data : { [numSeconds; sym_date_row]
/ date here: "D"$string[sym_date_row[‘date]];
/ sym here: sym date row[ sym);
/ one day for sym: .t.HDB ({ [numSeconds;test_date;symbol]
/ :select date, seconds:numSeconds xbar ‘second$time, time,sym, last_trade time, last_trade price, last_trade size, trade direction,
/ bid 1, bid 2, bid_3, bid 4, bid_5,
/ bid real size 1, bid_real size _2, bid_real_ size 3, bid_real size 4, bid real size 5,
/ offer 1, offer 2, offer 3, offer 4, offer 5,
/ offer_real size _1, offer_real size 2, offer_real_ size 3, offer_real size 4, offer_real_ size 5,
/ total daily volume, yesterdays settlement_price from B2B PRICE FUTURES EUREX where date=test_date, sym=symbol, not null{last_trade price], not (deltas total daily volume ) = 0;
/ }zmumSeconds;date here; sym here);
/ one_day for sym: update adjusted size medi3: last_trade_size - (med[(last_trade size ]%3), adjusted_size medi2: last_trade size - (med[last_ trade size ]%2),
/ adjusted size medil: last_trade_ size - med(last_trade size ], med_ts: med({last_trade size ] by seconds from one day for sym;
/ one_day for sym: update adjusted size avgi3: last_trade_size - (avg(last_trade size ]%3), adjusted size avgi2: last trade size - (avg[last trade size ]%2),
/ adjusted size avgil: last_trade_size - avg{last_trade_ size ], avg_ts: avg(last_trade size ] by seconds from one day for sym;
/ one_day for sym: update adjusted size medi3: max (adjusted _size_medi3;0), adjusted_size_medi2: max(adjusted_ size medi2;0), adjusted size medil: max(adjusted size medil;0) from one day for sym;
/ one day for sym: update adjusted size _avgi3: max(adjusted_size_avgi3;0), adjusted_size_avgi2: max(adjusted_ size _avgi2;0), adjusted size avgil: max(adjusted_ size _avgil;0) from one day for sym;
/ one day for sym: update open min:first[{last_trade price],
/ high _min:max[last_trade price],
/ low_min:min[last trade price],
/ close min:last[last trade price],
/ volume min:sum({last trade size],
/ vwap min: sum[last_trade price * last trade size) %tsum(last_trade size],
/ low_ ts: min[last_ trade size],
/ max ts: max[last trade size],
/ hhi_ ts: sum[ last_trade size * last _trade size ] % (sum(last_trade size)*sum[(last_ trade size)),
/ hhi_to_even: (sum[ last_trade size * last_trade size ] % (sum[last_trade_size)*sum[(last_trade size])) - ltcount[last_trade_ size],
/ hhi_ts_med2ig: sum({ adjusted size medi2 * adjusted_size medi2 }] % (sum[adjusted_ size _medi2)*sum[adjusted size medi2)),
/ hhi_to_even med2ig: (sum[{ adjusted size medi2 * adjusted size medi2 ] % (sum[adjusted_ size medi2)*sum[(adjusted_size medi2])) - 1%count[{adjusted_ size medi2],
/ hhi_ ts_avg2ig: sum{ adjusted size avgi2 * adjusted size avgi2 ] % (sum[adjusted_ size _avgi2]*sum[adjusted size _avgi2)),
/ hhi_ to even _avg2ig: (sum[ adjusted size avgi2 * adjusted size avgi2 ] % (sum[adjusted_size avgi2)*sum[adjusted_size_avgi2))) - ltcount[adjusted_ size avgi2),
/ imb 1: (last{offer_real size_1]-last(bid_ real size _1})#(last(offer real size 1)+last[bid_real_ size_1)),
/ imb 2: ((last(offer real size 1)+last(offer real size 2))-(last(bid_ real size 1)+last[bid_real size 2)))%((last(offer real size 1)+last(offer real size 2])+
/ (last({bid real size 1])+last{bid real size 2])),
/ imb al: (avg(offer real size _1]-avg(bid_real size 1])%(avg{offer real size 1]+avg{bid_real size 1)),
/ imb a2: ((avgloffer real size 1]+avgloffer real size 2])-(avg{bid real size 1]+avg(bid real size 2}))%((avgloffer real size 1)+avg[of fer real size 2))+(avg({bid_ real size 1]+
/ avg(bid real size 2))),
/ avts bid2: avg ts%(last[bid_real size _1)+last(bid_real size 2)),
/ avts bidl: avg tstlast(bid_ real size 1],
/ avts_ask2: avg ts%(last(offer_real size 1)+last(offer_real size 2)),
/ avts_askl: avg tstlast(offer real size 1],
/ avts 11: avg_ts%t(last(offer_ real size _1)+last[{bid real size _1)),
/ avts 12: avg tst(last[offer real size 1]+last[{bid_ real size 1])+last[(offer_real_size_2]+last[bid_real size_2)),
/ avts avgll: avg _tst(avg(offer real size 1)+tavg(bid real size 1}),
/ avts avgl2: avg ts%(avg(offer_real size _1]+avg{bid real size 1)+avg{offer real size 2]+avg[bid_ real size 2)),
/ medts bid2 : med ts%(last(bid_ real size _1)+last[bid_real size 2)),
/ medts bidl: med ts*tlast[bid_ real size 1],
/ medts ask2: med ts%(last(offer real size 1)+last(offer_real_ size_2}),
/ medts askl: med tstlast{offer real size 1],
/ medts 11: med ts%(last(offer_real_size_1)+last[bid_ real size 1]),
/ medts 12: med ts%(last(offer real size 1)+last(bid_ real size_1)+last(offer_real_size_2]+last(bid_real_ size 2)),
/ medts avg 1: med ts% (avg(offer real size _1)+tavg[{bid_ real size 1)),
/ medts avgl2: med _ts%(avg(offer_real_ size 1]+avg(bid_real_ size 1)+avgloffer_real size 2)+avg{bid_real_ size_2]) by seconds from one_day for sym;

/ one day for _sym:update low _up min:min[last_ trade price],high_up min:max[{last_trade price], volume_up min:sum[(last_trade size], n_up_min: count i,
/ Oo hhi_ts_up: sum[ last trade size * last_trade_ size ] % (sum{last_trade_size)*sum[last_trade_size]),
/ hhi_ to _even_up: (sum|[ last trade size * last_trade size ] % (sum[{last_trade_size])*sum(last_trade_size])) - ltcount[last_trade size)
/ by seconds from one day for_sym where trade _direction="UP; | |
/ | " flanh tyada nyvicol nigh down min:maxllast trade pricel. volume down min:sumflast trade 51zel, n down min: count ).

/ one _day for sym:update low_up_min:min[last_trade_price],high_up_min:max(last_trade_price], volume_up min:sum[last_trade_size], n_up_min: count li,
/ ee: sum [ last trade size * last_trade_size ] % (sum[{last_trade_size]*sum[last_trade size)),
/ i_to_even_up: (sum[ last trade size * last trade size % (sum t i *su t : = .
/ by seconds from one day for sym Si: Beate aaecse ten eae ] ( [last_trade_size]*sum[{last_trade_size])) 1%count [last_trade_ size]
/ one day for_sym: update low_down_min:min[last_trade_price],high_down_min:max[last_trade_price], volume down min:sum[last trade size], n down min: i
/ nae 8—Gown: sum{ last_trade_size * last_trade_size ] % (sum[{last_trade_size]*sum[last_ trade size]),_ ‘= man? count 2
/ i_to_even_down: (sum[ last_trade size * last trade si u t i *su t ;
/ SY SuChGS Sead one May for som he elie viecctn ee ] % (sum{last_trade_size]*sum[last_trade_size])) - 1%count[last_trade size]
/ one day for_sym: update n_up min:avg[n_up_min], n down min:avg[n down min], low_up min:avg[low up min in: ;
/ volume_up_min:avg{volume_up_min], volume _down_min: Sey QelEm Asin Ging. 168 acde Miceavoliioedomn wink Bic aeee ; . ‘
/ hhi ts_up:avg(hhi_ts_up], hhi_to_even_up:avg[hhi_to_even_up],hhi_ts down:avg[hhi_ts down], hhi to e q 2 se ee on GoMn mini,
/ by seconds from one day for_sym; ~ _ = ’ —"0_even_down: avg [hhi_to_even_down]
/ one day min summary: select last[open_min], last{high_min], last[low_min], last[{close_min], last[volume_mi
/ last [med_ts], last [avg _ts], last({low_ts], last(max_ts], last [max ts], sernbtinl ee hh; .
/ last [hhi_ts_avg2ig], last[hhi_to_even_avg2ig], last[imb_1], last[imb 2], last{imb al], last{imb a2] = [ i_ts_med2ig], last [{hhi_to even med2ig]
/ last[avts_askl], last[avts_1l1l], last[avts_12], last[avts_avgll], last[avts avgl2), last[medts bid2 , fast lavts_pid2], sash Lewes bidl], last [avts ask2]}
/ last[medts_ bidl], last{medts_ask2], last({medts_askl], last[medts 11], last [medts 12) Lee tnaetee - - — °
/ last [low up min],last{high_up_min], last[{volume_up_min], last(n_up min], last [hhi ts up) ina paovgen s last [medts_avg1l2],
/ last[{low down_min], last[high_down_min], last [volume _down min], — last[n down sm Vase F tas [ hi_to_even_up],
/ by date,seconds from one_day_ for_sym; _ - , iast(hhi_ts_down], last [hhi_to even down]
/ one day min_summary : 0! update sym:sym_here from one day min summary; 7 —
/ :one day _min_ summary
/ }{numSecs;] each (select sym, date from inputData);
/ day by day data_as_one: {x,y} over day by day data;
/ //if(toKDB;
/ A kdbTableName: “$kdbTableName;
/ // set [kdbTableName;day by day data_as_one];
/ If { [numSecs;date] .Q.dpft[*:H:/export_dir/kdb_detailed_data;date; ‘sym; kdbTableName] } (numSecs;] each (exec dist;
/ ; stin
/ Mf) ct date from day_by day data_as one);
/ :day_by day data_as_one;
/ }i

