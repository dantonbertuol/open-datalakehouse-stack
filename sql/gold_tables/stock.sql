select distinct
    stock.symbol,
    quo.shortName,
    quo.longName,
    quo.logourl
from stock 
left join stock_quotes quo on quo.symbol = stock.symbol