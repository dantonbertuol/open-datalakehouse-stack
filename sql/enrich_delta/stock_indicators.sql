select
    substr(ind.symbol, 1, length(ind.symbol) - 3) as symbol_wihtout_sa,
    ind.*
from stock_indicators ind