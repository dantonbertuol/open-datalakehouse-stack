select
    substr(div.symbol, 1, length(div.symbol) - 3) as symbol_wihtout_sa,
    div.*
from stock_dividends div