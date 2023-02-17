// this is the function to call and get the data frame from in python, at the moment only 1 instruction method, but will add ability to add more later on
get_outcomes_of_limit_orders: { [delay; bdfd; tdfd; extype; instructions]
    io: {x,y} over track_instruction[1e-5;delay;bdfd;tdfd;extype;] each instructions; // put the outcomes of the resting orders in one table
    io: `time xasc io;   // orders don't necessarily get filled in the order they were issued
    io[`position]: 0 +\ (not[io[`reason]=`cancel])*(io`Qty)*{ $[x=1b;1;-1] } each (io`side)=`bid;  // could have multiple position changes at the same timestamp
    io: `date`time`sym`ExPrice xcol io;
    :io;
    };

/// this is a helper function you need to use that will return the exits, fill price, qty and trade history whilst resting
track_instruction : { [delt;dl;bd;td;exs;ins] 
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
       bpx:td[ (count[td]-1) & (ixtd[0] + idxex) ];
       istif: $[idxex>=(count[tpxs]-1);1b;$[issell;tpxs[idxex]<rpd;tpxs[idxex]>rpd]];
       rtt:([] date : enlist bpx[`date]; time: enlist bpx[`time]; sym : enlist bpx[`sym]; Price: $[istif;0n;enlist rp]; Qty: $[istif;0i;ins`Qty]; 
                       orderId : ins`orderId; reason: extt; method: exs; side:ins`side);
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
                       reason: extt; method: exs);
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
           :fillHistory;
       ];
       if[istif;
          :([] date : enlist (td`date)[0]; time: enlist (td`time)[ ixtd[0] + idxex ]; sym : enlist ins`sym; Price: enlist 0n ; 
               Qty: 0i; orderId : ins`orderId; reason: `cancel; method: `partExOnTradePassed);
       ];
   ];   
   :0b 
    };
