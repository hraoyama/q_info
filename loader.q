find_largest_ticker:
    {
    tbl1: select from trades where date=x;
    tbl1: update ssym:`$ 4#'string sym from tbl1;
    tbl2: select count Price by sym, ssym from tbl1;
    tbl3: select max Price by ssym from tbl2;
    tbl2: tbl2 lj `ssym`MPrice xcol tbl3;
    tbl2: select sym, ssym from tbl2 where Price=MPrice;
    tbl1:ej[`ssym;tbl1;`sym2`ssym xcol tbl2];
    tbl1:select date, sym, ssym, time, Price, Qty, Volume from tbl1 where sym=sym2;
	
	tbl2: select from books where date=x;
	tbl1: aj[`sym`time;tbl1;tbl2];

    tbl1}

deltas0:{first[x] -': x}
round:{floor x+0.5}
IBS:{[price;mid] $[price<mid;-1;$[price>mid;1;0]]}


diff_by_sym:
	{
	tbl1: select date, time, ssym, ibs: IBS'[Price;0.5*(Bid_Px_Lev_0+Ask_Px_Lev_0)], BidAsk:Ask_Px_Lev_0-Bid_Px_Lev_0, dT:1e-9*`long$(deltas0 time), dPrice:deltas0 Price, Qty, loGdolVol:log Qty*Price from result where ssym=x;
	tbl2: select mint: first time, maxt: last time by date from tbl1;
	tbl1: tbl1 lj tbl2;
	tbl1: update dPrice:0.01 * round 100 * dPrice from tbl1;
	tbl1: select date, time, ssym, ibs1: prev ibs, ibs2: prev prev ibs, ibs3: prev prev prev ibs, ba1: prev BidAsk, ba2: prev prev BidAsk, ba3: prev prev prev BidAsk, dT, 
	dP:dPrice, dP1:prev dPrice, dP2:prev prev dPrice, dP3: prev prev prev dPrice, lsV1: prev ibs * loGdolVol, 
	lsV2: prev prev ibs * loGdolVol, lsV3: prev prev prev ibs * loGdolVol from tbl1 where time>mint,time<maxt;
	tbl1}


result: {x,y} over find_largest_ticker each ((select distinct date from trades where date within (2017.05.01; 2017.09.30)) `date);
dresult: {x,y} over diff_by_sym each ((select distinct ssym from result) `ssym);


