
// file formats

// trades
// date,sym,time,srctime,entrytime,aggrtime,seqn,evtseqn,price,size,tottrdqty,trdtype,revind,gapind,trdind,nordbuy,nordsell,aggr,packetStream,packetSeqNum
// 2018-11-07,F1COF201811,2018-11-07D12:33:08.053515000,2018-11-07D12:33:08.053391644,2018-11-07D12:33:08.053391644,2018-11-07D12:33:08.053370283,4627,,59.0759,5,,, , ,15,1,1,S,A,6184335

// quotes
// date,sym,time,bidQs,bidPs,askPs,askQs,spread,smid,lmid,totalBidQ,totalAskQ,wmid,suspect
// 2019-03-25,FFBTP201906,2019-03-25D08:01:02.505149000,4,129.57,129.64,3,0.07,129.605,129.6376,34,14,129.61,0
// 2019-03-25,FFBTP201906,2019-03-25D08:01:02.505159000,4,129.57,129.64,3,0.07,129.605,129.6354,63,35,129.61,0
// 2019-03-25,FFBTP201906,2019-03-25D08:01:02.505174000,4,129.57,129.64,3,0.07,129.605,129.6312,102,100,129.61,0
 
// depth
// date,sym,time,msgtype,srctime,aggrtime,seqn,updact,etype,price,prevprice,size,prio,matchid,completion
// 2019-08-21,F1ADS201909,2019-08-21D03:36:58.012994000,X,2019-08-21D03:30:18.789121585,,16189,J,J,,,,,6,0
// 2019-08-21,F1ADS201909,2019-08-21D03:36:58.012994000,f,2019-08-21D03:30:18.789121585,,16189,A, ,,,200,,0,0
// 2019-08-21,F1ADS201909,2019-08-21D07:30:00.028166000,h,2019-08-21D07:30:00.028161089,,18,M,P,,,2,,0,1
// 2019-08-21,F1ADS201909,2019-08-21D07:30:00.028166000,f,2019-08-21D07:30:00.028161089,,19,A, ,,,202,,0,1
// 2019-08-21,F1ADS201909,2019-08-21D08:55:00.017112000,h,2019-08-21D08:55:00.017107673,,35,D,T,,,2,,0,1
// 2019-08-21,F1ADS201909,2019-08-21D08:55:00.017112000,f,2019-08-21D08:55:00.017107673,,36,A, ,,,204,,0,1
// 2019-08-21,F1ADS201909,2019-08-21D09:00:00.067096000,f,2019-08-21D09:00:00.067090322,,51,A, ,,,203,,0,1

// how things are currently stored

// trades 
/ ------| -----
/ date  | d
/ sym   | s   p
/ time  | p
/ Price | f
/ Qty   | i
/ Volume| i

// books
/ -------------| -----
/ date         | d
/ sym          | s   p
/ time         | p
/ Bid_Px_Lev_0 | f
/ Bid_Px_Lev_1 | f
/ Bid_Px_Lev_2 | f
/ Bid_Px_Lev_3 | f
/ Bid_Px_Lev_4 | f
/ Ask_Px_Lev_0 | f
/ Ask_Px_Lev_1 | f
/ Ask_Px_Lev_2 | f
/ Ask_Px_Lev_3 | f
/ Ask_Px_Lev_4 | f
/ Bid_Qty_Lev_0| f
/ Bid_Qty_Lev_1| f
/ Bid_Qty_Lev_2| f
/ Bid_Qty_Lev_3| f
/ Bid_Qty_Lev_4| f
/ Ask_Qty_Lev_0| f
/ Ask_Qty_Lev_1| f



/  *************   40001  ****************

// testing trades handling

system "l E:/testroot"; 

extracted_trades_file: "E:/csv_data_from_py/trades/2018.11.13.csv";
date_string: 2018.11.13;
used_kdb_path: "E:/testroot";

extracted_trades_file: "E:/csv_data_from_py/trades/2019.08.21.csv";
date_string: 2019.08.21;

tradesTestroot:("DSZZZZIIFIIISIIIISSI";enlist ",") 0: hsym `$extracted_trades_file;
// count[tradesTestroot]

unique_syms : distinct[ { 5#string[x] }  each (select distinct sym from tradesTestroot)[`sym] ];
core_group: ("FBTP";"FBTS";"FDAX";"FDXM";"FESB";"FESX";"FGBL";"FGBM";"FGBS";"FGBX";"FOAT";"FSMI");

// only removes about 5% of trades, still could be interesting to investigate one day..
tradesTestroot: tradesTestroot[ where { [sym]  :any[ { [sym;coresym] :any[(5#string[sym]) ss coresym]; }[sym;] each core_group ];  }  each tradesTestroot[`sym] ];

// I know we also have the B/S by trade ("aggr" column) but that is not fully populated and I trust our analysis more...
tradesTestroot : select date: date, sym: sym, time:`timestamp$time, Price:price, Qty:size, Volume:0i from tradesTestroot;
tradesTestroot: update sym: { `$1_x } each string[sym] from tradesTestroot;
tradesTestroot: `time xasc tradesTestroot;

// rebase to zero volume (we do not have full day volume data - so that will be different when we do daily summaries!!)
tradesTestroot: {x,y} over {  :update Volume:0+\Qty from (select from tradesTestroot where sym=x); } each exec distinct sym from tradesTestroot;
trades: `time xasc tradesTestroot;

hsym[ `$ used_kdb_path,"/trades"] set .Q.en[hsym[ `$ used_kdb_path]] trades;
.Q.dpft[hsym[ `$ used_kdb_path];date_string;`sym;`trades];
delete trades from `.;
delete tradesTestroot from `.;

select from trades where i<100
select distinct date from trades
select date from books

// testing book quotes handling (based on quotes)

system "l E:/testroot"; 

extracted_quotes_file: "E:/csv_data_from_py/books/2019.08.21.csv";
date_string: 2019.08.21;
used_kdb_path: "E:/testroot";

// date,sym,time,bidQs,bidPs,askPs,askQs,spread,smid,lmid,totalBidQ,totalAskQ,wmid,suspect
// 2019-03-25,FFBTP201906,2019-03-25D08:01:02.505149000,4,129.57,129.64,3,0.07,129.605,129.6376,34,14,129.61,0

quotesTestroot:("DSZIFFIFFFIIFI";enlist ",") 0: hsym `$extracted_quotes_file;
// count[quotesTestroot]

unique_syms : distinct[ { 5#string[x] }  each (select distinct sym from quotesTestroot)[`sym] ];
core_group: ("FBTP";"FBTS";"FDAX";"FDXM";"FESB";"FESX";"FGBL";"FGBM";"FGBS";"FGBX";"FOAT";"FSMI");

// only removes about 5% of quotes, still could be interesting to investigate one day..
quotesTestroot: select from quotesTestroot where bidQs>0, askQs>0;
quotesTestroot: quotesTestroot[ where { [sym]  :any[ { [sym;coresym] :any[(5#string[sym]) ss coresym]; }[sym;] each core_group ];  }  each quotesTestroot[`sym] ];

// removing everything but TOB
quotesTestroot : select date: date, sym: sym, time:`timestamp$time, 
                        Bid_Px_Lev_0:bidPs, Bid_Px_Lev_1:0n, Bid_Px_Lev_2:0n, Bid_Px_Lev_3:0n, Bid_Px_Lev_4:0n, 
                        Ask_Px_Lev_0:askPs, Ask_Px_Lev_1:0n, Ask_Px_Lev_2:0n, Ask_Px_Lev_3:0n, Ask_Px_Lev_4:0n, 
                        Bid_Qty_Lev_0: bidQs, Bid_Qty_Lev_1:0i, Bid_Qty_Lev_2:0i, Bid_Qty_Lev_3:0i, Bid_Qty_Lev_4:0i,
                        Ask_Qty_Lev_0: askQs, Ask_Qty_Lev_1:0i, Ask_Qty_Lev_2:0i, Ask_Qty_Lev_3:0i, Ask_Qty_Lev_4:0i from quotesTestroot;

quotesTestroot: update sym: { `$1_x } each string[sym] from quotesTestroot;
books: `time xasc quotesTestroot;

hsym[ `$ used_kdb_path,"/books"] set .Q.en[hsym[ `$ used_kdb_path]] books;
.Q.dpft[hsym[ `$ used_kdb_path];date_string;`sym;`books];
delete books from `.;
delete quotesTestroot from `.;


select from books where i<100 




///  *************   40000  ****************
 
system "l E:/beetroot"; 
// select distinct date from trades

show meta[books]

// distinct[ { 4#string[x] }  each (select distinct sym from trades)[`sym] ];

not[date_string in (select distinct date from trades)[`date]]
date_string: 2018.11.07;

extracted_trades_file: "E:/csv_data_from_py/trades/2018.11.07.csv";
used_kdb_path: "E:/beetroot";

tradesTestroot:("DSZZZZIIFIIISIIIISSI";enlist ",") 0: hsym `$extracted_trades_file;
// count[tradesTestroot]

unique_syms : distinct[ { 5#string[x] }  each (select distinct sym from tradesTestroot)[`sym] ];
core_group: ("FBTP";"FBTS";"FDAX";"FDXM";"FESB";"FESX";"FGBL";"FGBM";"FGBS";"FGBX";"FOAT";"FSMI");

// only removes about 5% of trades, still could be interesting to investigate one day..
tradesTestroot: tradesTestroot[ where { [sym]  :any[ { [sym;coresym] :any[(5#string[sym]) ss coresym]; }[sym;] each core_group ];  }  each tradesTestroot[`sym] ];

// I know we also have the B/S by trade ("aggr" column) but that is not fully populated and I trust our analysis more...
tradesTestroot : select date: date, sym: sym, time:`timestamp$time, Price:price, Qty:size, Volume:0i from tradesTestroot;
tradesTestroot: update sym: { `$1_x } each string[sym] from tradesTestroot;
tradesTestroot: `time xasc tradesTestroot;

// rebase to zero volume (we do not have full day volume data - so that will be different when we do daily summaries!!)
tradesTestroot: {x,y} over {  :update Volume:0+\Qty from (select from tradesTestroot where sym=x); } each exec distinct sym from tradesTestroot;

trades: `time xasc tradesTestroot;

hsym[ `$ used_kdb_path,"/trades"] set .Q.en[hsym[ `$ used_kdb_path]] trades;
.Q.dpft[hsym[ `$ used_kdb_path];date_string;`sym;`trades];
delete trades from `.;
delete tradesTestroot from `.;

