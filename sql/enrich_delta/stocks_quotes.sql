select 
st.symbol as `symbol`,
stq.longName as `name`,
stq.shortName as `shortName`,
stq.currency as `currency`,
stq.marketCap as `marketCap`,
stq.regularMarketPrice as `price`,
stq.regularMarketVolume as `volume`,
stq.regularMarketTime as `time`
from
stock st
inner join stock_quotes stq on stq.symbol = st.symbol