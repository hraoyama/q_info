// this is loading the KDB database
// call this from python using a variable to denote your directory
\l E:/beetroot/
\l D:/data/beetroot/

/// set this for every contract date you want to back test
// you would call this from python
/ dateToTryOn: 2019.09.17;
/ contractToExecute: `FESX201912;
dateToTryOn: 2017.05.29;
contractToExecute: `FESX201706;

bdfd: select from books where date=dateToTryOn, sym=contractToExecute;
tdfd: select from trades where date=dateToTryOn, sym=contractToExecute;
/ bdfd: select date, sym, time, Bid_Px_Lev_0, Ask_Px_Lev_0 from books where date=dateToTryOn, sym=contractToExecute,time within (12:40;12:41);
/ tdfd: select from trades where date=dateToTryOn, sym=contractToExecute,time within (12:40;12:41);
/ bdfd: update bid_tob_change:{0f,1_deltas x} Bid_Px_Lev_0, ask_tob_change:{0f,1_deltas x} Ask_Px_Lev_0 from bdfd;
/ bidChangesTable: (select [1] from bdfd),(select from bdfd where not bid_tob_change=0f),(select [-1] from bdfd) ;
/ askChangesTable: (select [1] from bdfd),(select from bdfd where not ask_tob_change=0f),(select [-1] from bdfd);
/ select from tdfd where (i in (0;max[i])) or (not Price=3559);
/ count[select from tdfd where time<12:41:00.000]

/ select count[i] from tdfd where time within (12:40:20.03122;12:40:39.607567)
/ select count[i] from tdfd where time < 12:40:39.607567
delay:10; // delay in ms

/// this code gets the most active data and the data for the requested future (which has to be one of the actives for that day
// it is not part of the back test but is set up here in order to create example instructions that will go to the Q data base
// you should use this in Q (i.e. not from python) if you want to get the active futures for a test situation
getMostActiveDataOnly: { [x] 
   currentSymbolsForDay : 0! select first[sym], first[date], first[Volume] by ssym from (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from trades where date=x) where Volume=(max;Volume) fby ssym; 
   onlyMostActiveSymbolsData: select date, sym, time, Price, Qty, Volume from trades where date=x, sym in currentSymbolsForDay`sym ;
   :onlyMostActiveSymbolsData;
};
// activelyTradedFuturesTradesForDay: getMostActiveDataOnly[dateToTryOn];
// activeContractsForDay: { [x] : 0! select first[sym], first[date], first[Volume] by ssym from (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from trades where date=x) where Volume=(max;Volume) fby ssym; }[dateToTryOn]; 

/// here we set up a bunch of entry instructions with time outs for that particular day (the equivalent to this will get passed from python pandas)
// so check out the layout of instructions because this is what you will need to create from python
n:100;  // number of new limit orders
instructions:`time xasc flip `orderId`time`sym`level`Qty`side`ordertype!(1+til n;n?bdfd`time;n?enlist[contractToExecute];n?til 5;n?1 + til 5; n? `bid`offer; `limit);
instructions[`orderId]:1+til n; // make the id match increasing time
instructions[`tif] : max[bdfd`time] & \: ((instructions`time) + `time$(1000*60*(n?til 180)));  // time in force

/// this is a helper function you need to use that will return the exits, fill price, qty and trade history whilst resting
track_instruction : { [delt;dl;bd;td;exs;ins] 
    // delt:1e-5;dl:10;bd: bdfd;td:tdfd;exs:`fullExOnfullTradePassed; ins: instructions[0]

    show ins`orderId;
    issell:(ins`side)=`offer;
    pxlevel:`$($[issell;"Ask";"Bid"],"_Px_Lev_",string[(ins`level)]); 
    ixrest: (count[bd]-1) & (bin[bd`time;(ins`time)+(`time$dl)]+1); // +1 for the first index after resting
    ixtd: (bin[td`time;(bd`time)[ixrest]]+1) + til (bin[td`time;(ins`tif)+(`time$dl)]-bin[td`time;(bd`time)[ixrest]]-1); // all indices in the trades from resting till the stop time
    rp:bd[ixrest][pxlevel]; // entry resting px
    rpd: rp + delt * $[issell;1.0;-1.0]; // px+delta for stricly > or <
    tpxs: (td`Price)[ixtd]; // all traded prices from resting to time stop
    
    if[exs=`fullExOnTradePassed;
       idxex: (count[tpxs]-1) & ($[issell;binr[tpxs;rpd];binr[neg[tpxs];neg[rpd]]]); // index in trades of first cross (or the stop time if not found)
       idxex: $[idxex<0;(count[tpxs]-1);idxex];
       extt: $[idxex<(count[tpxs]-1);`fill;`cancel];
       tdh:td[ (count[td]-1) & (ixtd[0] + til idxex) ]; 
       bpx:td[ (count[td]-1) & (ixtd[0] + idxex) ];  
       istif: $[idxex>=(count[tpxs]-1);1b;$[issell;tpxs[idxex]<rpd;tpxs[idxex]>rpd]];
       rtt:([] date : enlist bpx[`date]; time: enlist bpx[`time]; sym : enlist bpx[`sym]; Price: $[istif;0n;enlist rp]; Qty: $[istif;0i;ins`Qty]; 
                       orderId : ins`orderId; reason: extt; method: exs; 
                       TrPxHstry: enlist tdh`Price; TrTimeHstry: enlist tdh`time ; TrQtyHstry: enlist tdh`Qty  );
       :rtt;
   ];
   if[exs=`fullExOnfullTradePassed;
       mxs: $[issell; tpxs>rpd; tpxs < rpd]; // boolean mask of where trades would fill
       xqtys: @[(td`Qty)[ixtd];where not mxs; {:0i;}]; // set the no fill trades to zero qty      
       idxex: (count[tpxs]-1) & binr[0 +\ xqtys;ins`Qty]; // idx of where the whole qty got filled
       tf: @[td[ixtd];idxex]; // trade at which the whole size would get filled
       extt: $[idxex<(count[tpxs]-1);`fill;`cancel];
       istif: extt=`cancel;
       rtt:([] date : enlist (1#tdfd)[`date][0]; 
               time: enlist $[istif;((-1)#td)[`time][0];tf`time]  ; sym : enlist ins[`sym]; 
                       Price: enlist $[istif;0n;rp];
                       Qty: $[istif;enlist 0i;ins`Qty]; 
                       orderId : ins`orderId; 
                       reason: extt; method: exs; 
                       TrPxHstry: $[istif;enlist 0n; enlist (td`Price)[(count[td]-1) & (ixtd[0] + til max(1;idxex) )]]; 
                       TrTimeHstry: $[istif;enlist 0Np; enlist (td`time)[(count[td]-1) & (ixtd[0] + til max(1;idxex))]]; 
                       TrQtyHstry: $[istif;enlist 0Ni; enlist (td`Qty)[(count[td]-1) & (ixtd[0] + til max(1;idxex))]] );
       :rtt;
   ];
   if[exs=`partExOnTradePassed;
       idxex: (count[tpxs]-1) & ($[issell;binr[tpxs;rpd];binr[neg[tpxs];neg[rpd]]]); // index in trades of first cross (or the stop time if not found)
       idxex: $[idxex<0;(count[tpxs]-1);idxex];
       idxaftx : idxex + til (count[tpxs] - idxex );
       istif: $[issell;tpxs[idxex]<rpd;tpxs[idxex]>rpd];
       if[not istif;
           fillaftx: $[issell;(td`Price)[ixtd[0] + idxaftx]>rpd;(td`Price)[ixtd[0] + idxaftx]<rpd]; // boolean mask of trades that fill the order
           fillqtys: 0 +\ @[(td`Qty)[ixtd[0] + idxaftx]; where not fillaftx; {:0i}];  // volume of order filled at each trade if infinite amount to fill
           idxrelfilqty: til (binr[fillqtys; ins`Qty]+1);
           if[count[idxrelfilqty]>count[idxaftx];idxrelfilqty:(count[idxaftx])#idxrelfilqty ]; // we run out of time in the day
           remrest: (ins`Qty) -\ deltas fillqtys[idxrelfilqty];   // remaining volume after each partial fill  
           tdwherefills: td[ixtd[0] + idxaftx[idxrelfilqty]];  // relevant fill trades
           if[(last remrest) = 0N; idxrelfilqty:(-1)_idxrelfilqty; remrest:(-1)_remrest; tdwherefills:(-1)_tdwherefills;]; 
           fillHistory: {x,y} over {
              [ins; rp; tdwherefills;remaining]
               extt: $[remaining>0;`partfill;`fill];
               :([] date : enlist tdwherefills[`date]; time: enlist tdwherefills[`time]; sym : enlist tdwherefills[`sym]; Price: enlist rp; 
                 Qty: $[remaining<0;(tdwherefills`Qty) + remaining;tdwherefills`Qty]; orderId : ins`orderId; reason: extt; method: `partExOnTradePassed);                      
           }[ins;rp;] '[tdwherefills;remrest];  // pass in the trades where the order gets filled and the remaining volume after the fill
           fillHistory: fillHistory ^ {x,y } over {  [tdfxo;fillpoint]  
               idxFill: bin[tdfxo`time;fillpoint`time]-1;  // -1 because we know the last one is the actual fill or stop
               :( [] TrPxHstry: enlist (tdfxo`Price)[ til idxFill]; TrTimeHstry: enlist (tdfxo`time)[ til idxFill] ; TrQtyHstry: enlist (tdfxo`Qty)[ til idxFill]  );           
           }[td[ixtd];] each fillHistory;  // pass the trade data from the first resting time until the stopping time
           // should be able to merge the above 2 over statements into 1...
           :fillHistory;  
       ];
       if[istif;
          :([] date : enlist (td`date)[0]; time: enlist (td`time)[ ixtd[0] + idxex ]; sym : enlist ins`sym; Price: enlist 0n ; 
               Qty: 0i; orderId : ins`orderId; reason: `cancel; method: `partExOnTradePassed; TrPxHstry: enlist 0n; TrTimeHstry: enlist 0Np  ; TrQtyHstry: enlist 0Ni );                      
       ];
   ];   
   :0b 
};  

// this is the function to call and get the data frame from in python, at the moment only 1 instruction method, but will add ability to add more later on
get_outcomes_of_limit_orders: { [delay; bdfd; tdfd; extype; instructions]
    :{x,y} over track_instruction[1e-5;delay;bdfd;tdfd;extype;] each instructions;
};
 
/// this is how you would use/call it at the moment (later on we add different fill methods or change other stuff) 
// it is basically the result from the instruction 'dataframe'
/ get_outcomes_of_limit_orders[delay;bdfd; tdfd; `fullExOnTradePassed; instructions];
/ get_outcomes_of_limit_orders[delay;bdfd; tdfd; `partExOnTradePassed; instructions];
/ get_outcomes_of_limit_orders[delay;bdfd; tdfd; `fullExOnfullTradePassed; instructions];
// `fullExOnfullTradePassed      (fill when full resting trade size trades beyond resting order)
// (-2)#instructions
/ orderId	time	sym	level	Qty	side	ordertype	tif
/ 99	2017-05-29T21:01:01.134461	FGBL201706	4	2	bid	limit	2017-05-29T21:59:59.948698
/ 100	2017-05-29T21:49:11.333872	FGBL201706	3	4	offer	limit	2017-05-29T21:59:59.948698















// (til 10)[5 + til (count[til 10] - 5) ]
// @[(td`Qty)[ixtd[0] + idxaftx]; where not fillaftx; {((meta(td))[`Qty]`t)$0}]
// ("car"; "far"; "mar") ,\: "e"

