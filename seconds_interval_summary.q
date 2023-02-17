// this used by setup derived data
get_up_down_trade_assignment :
{
    [twb1]
    // assign based on bid/ask px level
    twb1: update dir:?[Price <= Bid_Px_Lev_0;`down;?[Price >= Ask_Px_Lev_0;`up;`unknown]] from twb1;

    // add what happened to TOB previously and what happens next
    twb1: update nxt_Bid_Px_Lev_0:(next[Bid_Px_Lev_0]-Bid_Px_Lev_0), nxt_Ask_Px_Lev_0:(next[Ask_Px_Lev_0]-Ask_Px_Lev_0),
                 nxt_Bid_Qty_Lev_0:(next[Bid_Qty_Lev_0]-Bid_Qty_Lev_0), nxt_Ask_Qty_Lev_0:(next[Ask_Qty_Lev_0]-Ask_Qty_Lev_0),
                 prv_Bid_Px_Lev_0:(Bid_Px_Lev_0-prev[Bid_Px_Lev_0]), prv_Ask_Px_Lev_0:(Ask_Px_Lev_0-prev[Ask_Px_Lev_0]),
                 prv_Bid_Qty_Lev_0:(Bid_Qty_Lev_0-prev[Bid_Qty_Lev_0]), prv_Ask_Qty_Lev_0:(Ask_Qty_Lev_0-prev[Ask_Qty_Lev_0]) from twb1;
    // make assignment based on the previous price level
    if[count[select from twb1 where dir=`unknown]>0;
        twb1: update dir:?[Price=(Ask_Px_Lev_0-prv_Ask_Px_Lev_0); ?[not[Price=(Bid_Px_Lev_0-prv_Bid_Px_Lev_0)];`up;`unknown];`unknown] from twb1 where dir=`unknown;
        twb1: update dir:?[Price=(Bid_Px_Lev_0-prv_Bid_Px_Lev_0); ?[not[Price=(Ask_Px_Lev_0-prv_Ask_Px_Lev_0)];`down;`unknown] ;`unknown] from twb1 where dir=`unknown;
    ];
    // make assignment based on the previous price level changes of the last 20 trades
    twb1 : update   last_Bid_Px_Delta_20: (0n,(1_(rollWindow[lastNonZeroDelta;20;Bid_Px_Lev_0]))),
                    last_Bid_Px_Delta_Loc_20:  (0n,(1_(rollWindow[locLastNonZeroDelta;20;Bid_Px_Lev_0]))),
                    last_Ask_Px_Delta_20: (0n,(1_(rollWindow[lastNonZeroDelta;20;Ask_Px_Lev_0]))),
                    last_Ask_Px_Delta_Loc_20: (0n,(1_(rollWindow[locLastNonZeroDelta;20;Ask_Px_Lev_0])))
             from twb1;

    // these two could be merged, but let us keep it readable...
    if[count[select from twb1 where dir=`unknown]>0;
        twb1: update dir:?[Price=(Ask_Px_Lev_0-last_Ask_Px_Delta_20);?[not[Price=(Bid_Px_Lev_0-last_Bid_Px_Delta_20)];`up;?[last_Ask_Px_Delta_Loc_20>last_Bid_Px_Delta_Loc_20;`up;`unknown]];`unknown]  from twb1 where dir=`unknown;
        twb1: update dir:?[Price=(Bid_Px_Lev_0-last_Bid_Px_Delta_20);?[not[Price=(Ask_Px_Lev_0-last_Ask_Px_Delta_20)];`down;?[last_Bid_Px_Delta_Loc_20>last_Ask_Px_Delta_Loc_20;`down;`unknown]];`unknown]  from twb1 where dir=`unknown;
    ];
    // make assignment based on the next price/qty levels
    if[count[select from twb1 where dir=`unknown]>0;
        twb1: update dir:?[nxt_Ask_Px_Lev_0=0f;
                                    ?[(nxt_Ask_Qty_Lev_0<0f) & (nxt_Bid_Qty_Lev_0>=0f) & (nxt_Bid_Px_Lev_0=0f);`up;`unknown];
                                    ?[nxt_Ask_Px_Lev_0>0f;`up;`unknown]
                                    ]  from twb1 where dir=`unknown;
        twb1: update dir:?[nxt_Bid_Px_Lev_0=0f;
                                    ?[(nxt_Bid_Qty_Lev_0<0f) & (nxt_Ask_Qty_Lev_0>=0f) & (nxt_Ask_Px_Lev_0=0f);`down;`unknown];
                                    ?[nxt_Bid_Px_Lev_0<0f;`down;`unknown]
                                    ]  from twb1 where dir=`unknown;
    ];

    // at this point only a very small percentage (<0.2%) should be assigned according to shown quantity
    // (count[select from twb1 where dir=`up]%count[twb1])+(count[select from twb1 where dir=`down]%count[twb1])
    if[count[select from twb1 where dir=`unknown]>0;
        twb1: update dir:?[Ask_Qty_Lev_0>Bid_Qty_Lev_0;`down;?[Ask_Qty_Lev_0=Bid_Qty_Lev_0;?[first[1?til 10]<5;`down;`up];`up]] from twb1 where dir=`unknown;
    ];

    // remove the helper columns
    twb1: (`nxt_Bid_Px_Lev_0`nxt_Ask_Px_Lev_0`nxt_Bid_Qty_Lev_0`nxt_Ask_Qty_Lev_0`prv_Bid_Px_Lev_0)_twb1;
    twb1: (`prv_Ask_Px_Lev_0`prv_Bid_Qty_Lev_0`prv_Ask_Qty_Lev_0)_twb1;
    twb1: (`last_Bid_Px_Delta_20`last_Bid_Px_Delta_Loc_20`last_Ask_Px_Delta_20`last_Ask_Px_Delta_Loc_20)_twb1;

    :twb1;

};

makeBarSecondSummaryFuncA : {  [barSeconds;startTime;endTime;symDate]
        s:symDate[`sym];
        d:symDate[`date];
        show[symDate];
        relevantData: 0! update rebaseVol: (first[Qty],first[Qty] + 1_ deltas Volume), Price: $[`float;Price], trTime:time from (select from trades where sym=s, date=d, time within (startTime;endTime));
        relevantData2: update bkTime:time from (select from books where sym=s, date=d, time within (startTime;endTime));
        tradesWithBook: aj[`date`sym`time;relevantData; 0! relevantData2];
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
                        hhiUpToEven: hhiUp-1%(numUpTrades),
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
                        hhiDownToEven: hhiDown-1%(numDownTrades),
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
