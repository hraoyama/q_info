
// pfdLine:pfd[15]
// this is a fifo implementation of pnl calculation (we will implement lifo later) at the time of trade using trade execution only
fifoTradesOnlyTotPnl : {  [pfdLine]
            if[pfdLine[`fPos]>=0;
                // the short position is gone, so locked in pnl is what we received from selling minus the corresponding ratio of what we spent on long position
                spentOnGoneShortPos: (pfdLine[`totLong]-pfdLine[`Price]*pfdLine[`fPos]);
                receivedFromGoneShortPos: pfdLine[`totShort];
                totPnl: receivedFromGoneShortPos-spentOnGoneShortPos;
                :([] totPnl : enlist totPnl );
            ];
            if[pfdLine[`fPos]<0;

                // the long position is gone, so locked in pnl is what we received from selling minus the corresponding ratio of what we spent on long position
                receivedOnGoneLongPos: (pfdLine[`totShort]-pfdLine[`Price]*abs[pfdLine[`fPos]]);
                spentFromGoneLongPos: pfdLine[`totLong];
                totPnl: receivedOnGoneLongPos-spentFromGoneLongPos;
                :([] totPnl : enlist totPnl);
            ];
    };

// this is a fifo implementation of pnl calculation (we will implement lifo later) at the time of trade 
fifo : {  [pfdLine]
            if[pfdLine[`fPos]>=0;
                // the short position is gone, so locked in pnl is what we received from selling minus the corresponding ratio of what we spent on long position
                lockedIn: $[not[pfdLine[`accLong]=0];pfdLine[`totShort] - (pfdLine[`totLong]%pfdLine[`accLong])*pfdLine[`accShort];0f];
                // the entry price of the current position
                entryPxfPos: ((pfdLine[`totLong]-pfdLine[`totShort]) - lockedIn ) % pfdLine[`fPos];
                // the running pnl is what we could got for the current positon minus what we would need to buy it at
                runningPnl: (pfdLine[`Price]-entryPxfPos)*pfdLine[`fPos];
                :([] lockPnl : enlist lockedIn; runPnl: enlist runningPnl);
            ];
            if[pfdLine[`fPos]<0;
                // the long position is gone, so (the average short entry minus average long entry) * total long position removed = locked in pnl
                // 2 implementations giving the same thing                
                lockedIn: ((pfdLine[`totShort]%pfdLine[`accShort]) - pfdLine[`totLong]%pfdLine[`accLong])*pfdLine[`accLong];
                // lockedIn: $[not[pfdLine[`accShort]=0];neg[pfdLine[`totLong] - (pfdLine[`totShort]%pfdLine[`accShort])*pfdLine[`accLong]];0f];
                // the entry price of the existing position is the total short spend minus the total long spend, adjusted for the locked in pnl, divided by the current position
                entryPxfPos: ((pfdLine[`totShort]-pfdLine[`totLong]) - lockedIn) %neg[pfdLine[`fPos]];
                runningPnl: (entryPxfPos-pfdLine[`Price])*neg[pfdLine[`fPos]];
                :([] lockPnl : enlist lockedIn; runPnl: enlist runningPnl);
            ];
    };

// update the running p&l when trades occur
fifoRunPnl : {  [pfdLine]
                 :$[not null[pfdLine[`runPnl]];
                    pfdLine[`runPnl];
                    $[pfdLine[`fPos]>=0; 
                      (pfdLine[`Price]-( ((pfdLine[`totLong]-pfdLine[`totShort]) - pfdLine[`lockPnl] ) % pfdLine[`fPos]))*pfdLine[`fPos] ;
                      ((((pfdLine[`totShort]-pfdLine[`totLong]) - pfdLine[`lockPnl]) %neg[pfdLine[`fPos]])-pfdLine[`Price])*neg[pfdLine[`fPos]] 
                      ] 
                   ];   
    };

// given a table of order outcomes, this function will return a table with the pnl at each time of trade
gettd: { [io] 
         symh: io[`sym][0]; dateh: 14h$(io[`time][0]);
         io: 0! `time xasc `date`sym`method`side`time`ExPrice xgroup (select from io where Qty > 0); // could have multiple fills at the same time
         ts: (1#io[`time])[0];   // start of fills
         te: ((-1)#io[`time])[0];  // end of fills
         io: update totQty:sum[raze Qty], fPos:(last each position) by time from io;  // fPos is the final net position at each timestamp
         ttd: select date, sym, time, Price from get["tdfd",string[symh]] where sym=symh, date=dateh, time within(ts;te);  // get relevant trades
         pfd: ttd lj (3! select date: 14h$date, sym, time, ExPrice, position, side, orderId, Qty, totQty, fPos from io);  // put trades and fills together
         pfd: select from pfd where not null[ExPrice];  // for now only keep the trades where fills happen
         pfd: update pChange:deltas[fPos] from pfd;
         // alter the last trade to a flattening trade
         pfd: update position: enlist 0f, Qty: enlist abs[fPos-pChange], side:?[(fPos-pChange)>=0;`offer;`bid], totQty: abs[fPos-pChange], fPos:0 from pfd where time=max[pfd[`time]];
         // add an extra line to go flat at EOD
         pfd: update pChange:deltas[fPos], fillId:(til count[pfd]),totLong:0+\?[side=`bid;ExPrice*totQty;0], totShort:0+\?[side=`offer;ExPrice*totQty;0] from pfd;  
         // fillId is required in case a buy & sell in the same timestamp, add change in positions and total spent on long and short trades  
         pfd: update accLong:0+\?[side=`bid;totQty;0], accShort:0+\?[side=`offer;totQty;0]  from pfd;
         // total number of long an short contracts traded until that time
         pfd: `fillId xcols pfd;  // move fillId to the start so no confustion with quantities and positions

          // pfd: pfd ^ {x,y} over fifo each pfd;  // add running pnl and locked in pnl
         pfd: pfd ^ {x,y} over fifoTradesOnlyTotPnl each pfd;  // add total pnl`
         pfd : update lockPnl: totPnl, runPnl:0 from pfd where fPos=0;
         pfd : update fills lockPnl from pfd;
         pfd : update runPnl:totPnl - lockPnl from pfd;

         pfd: update 0^runPnl from pfd; // set last runPnl to zero instead of null
         pfd: ttd lj 4! `date`sym`time`Price xcols pfd; // bring back the in-between trades and fill up the quantities that don't change with the trades
         pfd: update fPos:fills[fPos], totLong:fills[totLong],totShort:fills[totShort],accLong:fills[accLong],accShort:fills[accShort], lockPnl:fills[lockPnl] from pfd;
        // calculate the runPnl
         pfd: update runPnl:(fifoRunPnl each pfd) from pfd;
         pfd: update pnl:lockPnl+runPnl from pfd;
         // select from pfd where not null[fillId]
        :pfd;         
    };

// date,time,sym,ExPrice,Qty,orderId,orderSize,reason,method,side,position
// 2021.01.06,2021.01.06D08:00:03.905381,FDXM202103,13686.0,1,5,1,fill,hyperionSim,bid,1
// 
//  testdf : ("DPSFIIISSSI";enlist",") 0: `:d:/Code/ProjectBlue/order_outcomes/testFDXM.csv;
//  aa: pnlFromTradesOnly[testdf]  ;   io3:testdf;
// io3: ("DPSFIIISSSI";enlist",") 0: `:d:/Code/ProjectBlue/order_outcomes/testFDXM.csv;
// io3 : select from io3 where Qty > 0;
// io3: ("DPSFIISSSI";enlist",") 0: `:D:/Code/ProjectBlue/order_outcomes/mix_20210103/simul_orders_20200320_reloadFalse_tif30p0_phi0p1_alfa0p0001_kappa0p01_20210103T2357_outcomes.csv;
// pnlFromTradesOnly[x]
// ([] lockPnl : enlist 20; runPnl: enlist `test)
// cols[io3]

pnlFromTradesOnly: { [io3]
    io3 : select from io3 where Qty > 0;
    // date, time, sym, ExPrice, Qty, side, position, reason, method, lockPnl, runPnl
    if[count[io3]<=0;
        :([] date : enlist "1900.01.01"; time: enlist "1900.01.01D12:00:00.000"; sym: enlist `ALL; ExPrice: enlist 0f; Qty: enlist 0i; side: enlist `bid; position: enlist 0i; reason: enlist `cancel;method: enlist `hyperionSim; lockPnl: enlist 0f; runPnl: enlist 0f; runPnl: enlist 0f)];
    io3: `time xasc io3;
    pfd: update pChange:deltas[position] from io3;

    // alter the last trade to a flattening trade (could be unfair positive bias) if it is not already flat
    if[not[0i=((-1)#pfd`position)[0]];
       pfd: update position: 0, Qty: abs[first[(-2)#pfd[`position]]], side:?[(position-pChange)>=0;`offer;`bid], pChange: neg[first[(-2)#pfd[`position]]] from pfd where time=max[pfd[`time]];
      ];
    pfd: update accLong:0+\?[side=`bid;Qty;0], accShort:0+\?[side=`offer;Qty;0]  from pfd;
    pfd: `orderId xcols pfd;  // move fillId to the start so no confustion with quantities and positions

    // assume the Price is the traded price...
    pfd: update Price:ExPrice, fPos:position from pfd;
    pfd: update totLong:0+\?[side=`bid;ExPrice*Qty;0], totShort:0+\?[side=`offer;ExPrice*Qty;0] from pfd;  
    // add running pnl and locked in pnl`
    pfd: pfd ^ {x,y} over fifoTradesOnlyTotPnl each pfd;  // add total pnl`
    pfd : update lockPnl: totPnl, runPnl:0 from pfd where fPos=0;
    pfd : update lockPnl:0f from pfd where i=0;
    pfd : update fills lockPnl from pfd;
    pfd : update runPnl:totPnl - lockPnl from pfd;
    pfd: select date, time, sym, ExPrice, Qty, side, position, reason, method, lockPnl, runPnl, totPnl from pfd;
    :pfd;
 };


// for debug (source execution.q first):
// \l E:/beetroot
// \l D:\\Code\\ProjectBlue\\src\\q\\execution.q
// test_dateToTryOn:2017.05.30; test_contractToExecute: `FESX201706;
// test_bdfd: select from books where date=test_dateToTryOn, sym=test_contractToExecute;
// test_tdfd: select from trades where date=test_dateToTryOn, sym=test_contractToExecute;
//// here we set up a bunch of entry instructions with time outs for that particular day (the equivalent to this will get passed from python pandas)
//// so check out the layout of instructions because this is what you will need to create from python
// test_n:100;  // number of new limit orders
// test_instructions:`time xasc flip `orderId`time`sym`level`Qty`side`ordertype!(1+til test_n;test_n?test_bdfd`time;test_n?enlist[test_contractToExecute];test_n?til 5;test_n?1 + til 5; test_n? `bid`offer; `limit);
// test_instructions[`orderId]:1+til test_n; // make the id match increasing time
// test_instructions[`tif] : max[test_bdfd`time] & \: ((test_instructions`time) + `time$(1000*60*(test_n?til 180)));  // time in force
// test_instructions: select from test_instructions where not[test_instructions[`time]~'prev test_instructions[`time]]; // avoids same time orders
// delay:10 ; 
// bdfdFESX201706: select from books where date=test_dateToTryOn, sym=test_contractToExecute; 
// tdfdFESX201706: select from trades where date=test_dateToTryOn, sym=test_contractToExecute; 
// extype: `fullExOnTradePassed; 
// io: get_outcomes_of_limit_orders[delay;bdfdFESX201706; tdfdFESX201706; extype; test_instructions];
// pnlBreakdown: gettd[io];
// select from pnlBreakdown where not null[ExPrice];  // remove the "not null" to see the continuously tracked pnl

// io3: ("DPSFIISSSI";enlist",") 0: `:d:/Code/ProjectBlue/ignore/simul_orders_20201213T1659_outcomes.csv ;
// io3 : select from io3 where Qty > 0;
// pnlio3:  gettd[io3];

// type[io] 
// save `:d:/Code/ProjectBlue/ignore/pnlBreakdown.csv
// (-100) sublist pnlBreakdown
// save `:d:/io.csv # https://stackoverflow.com/questions/43709790/kdb-save-table-into-a-csv-file
// `:d:/Code/ProjectBlue/ignore/io_here.csv 0: csv 0: select date,time, sym, ExPrice, string each Qty, orderId, reason, method,side, position from io
// io2: read0 `:d:/Code/ProjectBlue/ignore/simul_orders_20201213T1659_outcomes.csv
/  type[io2]
/ 
/ 
/ ("FFFFS";enlist ",")0:`iris.csv

type[testVarDF[`time]]

pnlFromTradesOnly[testVarDF]




