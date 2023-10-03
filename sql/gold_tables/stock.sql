select distinct
    stock.symbol,
    quo.shortName,
    quo.longName,
    quo.logourl
from stock 
left join stock_quotes quo on quo.symbol = stock.symbol
where (quo.regularMarketTime = (select max(quo_.regularMarketTime) from stock_quotes quo_ where quo_.symbol = stock.symbol) or quo.regularMarketTime is null)