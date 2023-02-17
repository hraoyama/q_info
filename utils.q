find_largest_ticker:
    {
    tbl1: select date, sym from books where date=x;
    tbl1: update ssym:`$ 4#'string sym from tbl1;
    tbl2: select counter: count date by sym, ssym from tbl1;
    tbl3: select max_counter: max counter by ssym from tbl2;
    tbl2: tbl2 lj tbl3;
    tbl2: select sym, ssym from tbl2 where counter=max_counter;
	tbl2: update date:x from tbl2;
	tbl2
	};

deltas0:{first[x] -': x};
round:{floor x+0.5};
IBS:{[price;mid] $[price<mid;-1;$[price>mid;1;0]]};
MarkovState:{[oi; n] barr: reverse 1 - 2*(1+ til n-1)%n; l:min where barr >= oi;$[l=0W;n;l+1]};
mad:{avg abs x-avg[x] };
assign_bid_or_ask:{$[x=`up;`ask;`bid]}

microprice:
	{[tbl]
	tbl: update microPrice: (Bid_Px_Lev_0 * (Ask_Qty_Lev_0 % (Bid_Qty_Lev_0 + Ask_Qty_Lev_0))) + (Ask_Px_Lev_0 * (Bid_Qty_Lev_0 % (Bid_Qty_Lev_0 + Ask_Qty_Lev_0))) from tbl
	tbl};

kappa:
	{[tbl;qty]
	
    date: (select min date from tbl) `date;
	tot_a: count select from tbl where dir=`up;
	a0: count select from tbl where dir=`up,Qty>=Ask_Qty_Lev_0+qty;
	a1: count select from tbl where dir=`up,Qty>=Ask_Qty_Lev_0+Ask_Qty_Lev_1+qty;
	a2: count select from tbl where dir=`up,Qty>=Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+qty;
	a3: count select from tbl where dir=`up,Qty>=Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+qty;
	a4: count select from tbl where dir=`up,Qty>=Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+Ask_Qty_Lev_4+qty;
	tot_b: count select from tbl where dir=`down;
	b0: count select from tbl where dir=`down,Qty>=Bid_Qty_Lev_0+qty;
	b1: count select from tbl where dir=`down,Qty>=Bid_Qty_Lev_0+Bid_Qty_Lev_1+qty;
	b2: count select from tbl where dir=`down,Qty>=Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+qty;
	b3: count select from tbl where dir=`down,Qty>=Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+qty;
	b4: count select from tbl where dir=`down,Qty>=Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+Bid_Qty_Lev_4+qty;
	tab:([] date:date; Qty:qty; Na:enlist tot_a; Na0:a0; Na1:a1; Na2:a2; Na3:a3; Na4:a4; Nb:tot_b; Nb0:b0; Nb1:b1; Nb2:b2; Nb3:b3; Nb4:b4);
	tab};


fill_instructions:
	{[tbl;bdfd;ticker;level;qty;waitTime]
	//waitTime is in minutes
	n:count tbl;  // number of new limit orders
    instructions:`time xasc flip `orderId`time`sym`level`Qty`side`ordertype!(1+til n;tbl`time;n#ticker;n#level;n#qty;each[assign_bid_or_ask] tbl`dir;`limit);
    instructions[`orderId]:1+til n; // make the id match increasing time
    instructions[`tif] : max[bdfd`time] & \: (instructions`time) + `time$(1000*60*waitTime);  // time in force
	instructions
	}


fill_instructions_all:
	{[tbl;bdfd;ticker;qty;waitTime]
    :{x,y} over fill_instructions[tbl;bdfd;ticker;;qty;waitTime] each til 5;
	};

TradesWithQuotes:
	{[d;s]
	
	my_trades: update Price: $[`float;Price], trTime:time from (select from trades where date=d, sym=s, time within (07:30;17:15));
	my_quotes: select from books where date=d, time within (07:30;17:15),sym=s;
	tradesWithBook: aj[`time;my_trades; 0! my_quotes];
	tradesWithBook:get_up_down_trade_assignment[tradesWithBook];
	tradesWithBook:update state:MarkovState'[(Bid_Qty_Lev_0 - Ask_Qty_Lev_0)%(Bid_Qty_Lev_0 + Ask_Qty_Lev_0);3], mid:0.5*(Bid_Px_Lev_0+Ask_Px_Lev_0) from tradesWithBook;
	
	tradesWithBook};

fill_the_gaps:
	{[data]
	currentDate:(select trTime.date from data 0)`date;
	tab:select last state by 1 xbar trTime.minute from data;
	tab:select trTime, state from update trTime:currentDate+minute+1 from tab;
	tab
	};

duration_by_state_and_bucket:
	{[data; window]
	temp: `trTime xasc (select trTime, state from data) uj fill_the_gaps data;
	deltaT: select sum dt by trTime:window xbar trTime.minute, state from select trTime, dt:(next trTime) - trTime, state from temp;
	deltaT: select trTime, state, dt:(0.001 * `int$dt.time) + (`int$(dt.timespan - dt.time)) % (10 xexp 9) from deltaT;
	deltaT
	};	
	
transitions_between_states_and_buckets:
	{[data; window]
	nij: select count dir by trTime:window xbar trTime.minute, state, next state from data;
	nij: select from nij where state1 <> 0N;
	nij};	
	
number_of_market_orders_and_buckets:
	{[data; window]
	MO: select trade_nums:count trTime, trade_vols:sum Qty by trTime:window xbar trTime.minute, state, dir from data where dir in `up`down;
	MO
	};

ast_by_state_and_bucket:
	{[data; window]
	/this function computes alpha, mean absolute deviation (sigma), and duration (tau) for each state and window. Window should be int and in minutes.
	deltaT: duration_by_state_and_bucket[data;window];
	temp: fills `trTime xasc (select trTime, state, Price from data) uj fill_the_gaps data;
	alpha_sigma_tau: select alphaW:avg dP, alphaB:sum dP,volW:mad dP by trTime:window xbar trTime.minute, state from select trTime, dP:Price-(prev Price), state from temp;
	alpha_sigma_tau: deltaT lj alpha_sigma_tau;
	alpha_sigma_tau
	};
	
features_creation_by_state:
	{[data; window]
	//need to think about fills
	temp: fills `trTime xasc (select trTime, state, Price, Bid_Px_Lev_0, Ask_Px_Lev_0, Bid_Qty_Lev_0, Bid_Qty_Lev_1, Bid_Qty_Lev_2, Bid_Qty_Lev_3, Bid_Qty_Lev_4, Ask_Qty_Lev_0, Ask_Qty_Lev_1, Ask_Qty_Lev_2, Ask_Qty_Lev_3, Ask_Qty_Lev_4 from data) uj fill_the_gaps data;
	MO: select oimb0:(sum(Bid_Qty_Lev_0 - Ask_Qty_Lev_0))%(sum(Bid_Qty_Lev_0 + Ask_Qty_Lev_0)),
	oimb1:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1)), 
	oimb2:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2)),
	oimb3:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2+Bid_Qty_Lev_3+Ask_Qty_Lev_3)),
	oimb4:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+Bid_Qty_Lev_4)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+Ask_Qty_Lev_4)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2+Bid_Qty_Lev_3+Ask_Qty_Lev_3+Bid_Qty_Lev_4+Ask_Qty_Lev_4)),
	bav_rat0:(sum Bid_Qty_Lev_0) % (sum Ask_Qty_Lev_0),
	bav_rat1:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1)),
	bav_rat2:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2)),
	bav_rat3:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3)),
	bav_rat4:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+Bid_Qty_Lev_4)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+Ask_Qty_Lev_4)),
	avgP: avg Price, medP: med Price, qss: avg Ask_Px_Lev_0-Bid_Px_Lev_0, effs: avg Ask_Px_Lev_0+Bid_Px_Lev_0-2*Price,
    bq0:avg Bid_Qty_Lev_0, bq1:avg Bid_Qty_Lev_1, bq2:avg Bid_Qty_Lev_2, bq3:avg Bid_Qty_Lev_3, bq4:avg Bid_Qty_Lev_4,
    aq0:avg Ask_Qty_Lev_0, aq1:avg Ask_Qty_Lev_1, aq2:avg Ask_Qty_Lev_2, aq3:avg Ask_Qty_Lev_3, aq4:avg Ask_Qty_Lev_4	by trTime:window xbar trTime.minute, state from temp;
	MO
	};


number_of_market_orders:
	{[data; window]
	MO: select trade_nums:count Qty, trade_vols:sum Qty by trTime:window xbar trTime.minute, dir from data where dir in `up`down;
	MO
	};

alpha_vol:
	{[data; window]
	/this function computes alpha, mean absolute deviation (sigma) for each time window. Window should be int and in minutes.
	temp: fills `trTime xasc (select trTime, Price from data) uj (delete state from fill_the_gaps data);
	alpha_sigma: select alphaW:avg dP, alphaB:sum dP,volW:mad dP by trTime:window xbar trTime.minute from select trTime, dP:Price-(prev Price) from temp;
	alpha_sigma
	};

features_creation:
	{[data; window]
	temp: fills `trTime xasc (select trTime, Price, Bid_Px_Lev_0, Ask_Px_Lev_0, Bid_Qty_Lev_0, Bid_Qty_Lev_1, Bid_Qty_Lev_2, Bid_Qty_Lev_3, Bid_Qty_Lev_4, Ask_Qty_Lev_0, Ask_Qty_Lev_1, Ask_Qty_Lev_2, Ask_Qty_Lev_3, Ask_Qty_Lev_4 from data) uj (delete state from fill_the_gaps data);
	MO: select oimb0:(sum(Bid_Qty_Lev_0 - Ask_Qty_Lev_0))%(sum(Bid_Qty_Lev_0 + Ask_Qty_Lev_0)),
	oimb1:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1)), 
	oimb2:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2)),
	oimb3:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2+Bid_Qty_Lev_3+Ask_Qty_Lev_3)),
	oimb4:(sum((Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+Bid_Qty_Lev_4)-(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+Ask_Qty_Lev_4)))%(sum(Bid_Qty_Lev_0+Ask_Qty_Lev_0+Bid_Qty_Lev_1+Ask_Qty_Lev_1+Bid_Qty_Lev_2+Ask_Qty_Lev_2+Bid_Qty_Lev_3+Ask_Qty_Lev_3+Bid_Qty_Lev_4+Ask_Qty_Lev_4)),
	bav_rat0:(sum Bid_Qty_Lev_0) % (sum Ask_Qty_Lev_0),
	bav_rat1:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1)),
	bav_rat2:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2)),
	bav_rat3:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3)),
	bav_rat4:(sum(Bid_Qty_Lev_0+Bid_Qty_Lev_1+Bid_Qty_Lev_2+Bid_Qty_Lev_3+Bid_Qty_Lev_4)) % (sum(Ask_Qty_Lev_0+Ask_Qty_Lev_1+Ask_Qty_Lev_2+Ask_Qty_Lev_3+Ask_Qty_Lev_4)),
	qss: avg Ask_Px_Lev_0-Bid_Px_Lev_0, effs: avg Ask_Px_Lev_0+Bid_Px_Lev_0-2*Price,
    bq0:avg Bid_Qty_Lev_0, bq1:avg Bid_Qty_Lev_1, bq2:avg Bid_Qty_Lev_2, bq3:avg Bid_Qty_Lev_3, bq4:avg Bid_Qty_Lev_4,
    aq0:avg Ask_Qty_Lev_0, aq1:avg Ask_Qty_Lev_1, aq2:avg Ask_Qty_Lev_2, aq3:avg Ask_Qty_Lev_3, aq4:avg Ask_Qty_Lev_4	by trTime:window xbar trTime.minute from temp;
	MO
	};



// for each month future find which one is largest
// tickers: {x,y} over find_largest_ticker each ((select distinct date from trades where date within (2017.05.01; 2017.12.31)) `date);
// save `:d:/tickers.csv

// tickers:("SSD";enlist ",") 0: `:d:/tickers.csv

// utility functions for later
rollWindow: { [f;w;s] f each {1_x,y}\[w#0;s]  }; // replaces the first w values with zero
lastNonZeroDelta: {  :last[1_(deltas[x] except 0)]; };
locLastNonZeroDelta: {  :max[where not[deltas[x]=0]];  };

// twb1: tradesWithBook

// takes as input a table where the trade (Price, Qty, sym, time) is joined with the last available book and adds a 'dir' column 
// that is assigned the values `up, `down or `unknown
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
		// in theory every check for the `unknown count should be performed on each update/select separately, but that is so unlikely...
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

active_contracts_by_day: {
	[dateStart; dateEnd]
	:{x,y} over { : 0! select first[sym], first[date], first[Volume] by ssym from 
												(0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])] by sym from 
														trades where date=x) where Volume=(max;Volume) fby ssym; } each (dateStart + til (dateEnd-dateStart+1));            
	};

prevWeekday: {[d] $[2=d mod 7;d-3; $[1=d mod 7;d-2;d-1]]};
nextWeekday: {[d] $[6=d mod 7;d+3; $[0=d mod 7;d+2;d+1]]};
dayOfWeek: { [d] `Sat`Sun`Mon`Tue`Wed`Thu`Fri d mod 7 };
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

active_contracts_day:{[x]
    0! select first[sym], first[date], first[Volume] by ssym
    from (0! select last[Volume], last[date], ssym:last[(`$4#'string[sym])]
    by sym from trades where date=x) where Volume=(max;Volume) fby ssym};

microprice:
	{[tbl]
	select time, microPrice: (Bid_Px_Lev_0 * (Ask_Qty_Lev_0 % (Bid_Qty_Lev_0 + Ask_Qty_Lev_0)))
	        + (Ask_Px_Lev_0 * (Bid_Qty_Lev_0 % (Bid_Qty_Lev_0 + Ask_Qty_Lev_0))) from tbl
	};