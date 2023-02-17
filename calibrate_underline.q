\l /Users/fangxia/Data/kdb

start: 07:30;
end: 17:15;

dateToUse: 2017.05.02;
activeContracts: active_contracts_day[dateToUse];

tmp: exec first[sym] from `Volume xdesc activeContracts;
symToUse: value tmp;

tdWithDir: TradesWithQuotes[dateToUse;symToUse];

bk: select from books where date=dateToUse, sym=symToUse, time within (start, end);
td: select from trades where date=dateToUse, sym=symToUse, time within (start, end);

bktd: bk uj tdWithDir;
bktd: `time xasc bktd;

// pick up quote before trade and add the price, qty to the quote of the trade
beforeTradeQuote: select  from bktd where (null (next Price))=0b, (null Price)=1b;
tmp: beforeTradeQuote uj tdWithDir;
tmp: `time xasc tmp;
tmp: update Price: (next Price), Qty: (next Qty), dir: (next dir) from tmp;
beforeTradeQuote: select from tmp where (null Price)=0b;

beforeTradeQuote: update Ask_Qty_Lev_0: Ask_Qty_Lev_0 - Qty from beforeTradeQuote where dir=`up;
beforeTradeQuote: update Bid_Qty_Lev_0: Bid_Qty_Lev_0 - Qty from beforeTradeQuote where dir=`down;
beforeTradeQuote: update Bid_Qty_Lev_1: Bid_Qty_Lev_1 + Bid_Qty_Lev_0 from beforeTradeQuote where Bid_Qty_Lev_0 < 0;
beforeTradeQuote: update Ask_Qty_Lev_1: Ask_Qty_Lev_1 + Ask_Qty_Lev_0 from beforeTradeQuote where Ask_Qty_Lev_0 < 0;
beforeTradeQuote: update Bid_Qty_Lev_0: 0.0 from beforeTradeQuote where Bid_Qty_Lev_0 < 0;
beforeTradeQuote: update Ask_Qty_Lev_0: 0.0 from beforeTradeQuote where Ask_Qty_Lev_0 < 0;


// pick pu quote after trade
afterTradeQuote: select from tmp where (null (prev Price))=0, (null Price)=1b;

// calculate micro price
mcpAfterTrade: microprice[afterTradeQuote];
mcpBeforeTrade: microprice[beforeTradeQuote];
mcp: microprice[bk];

