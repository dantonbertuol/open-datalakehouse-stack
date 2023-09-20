import yfinance as yf


class Indicators():
    '''
    Class to extract indicators from yfinance
    '''

    def get_indicators(self, tickers: str) -> dict:
        '''
        Get indicators from yfinance

        Args:
            tickers (str): tickers to get indicators

        Returns:
            dict: dict of indicators
        '''
        tickers_data = yf.Tickers(tickers)

        data: dict = {}
        website: str = ''
        country: str = ''
        industry: str = ''
        sector: str = ''
        employes: int = 0
        dy: float = 0.0
        peg_ratio: float = 0.0
        recomendation: str = ''
        ebitda: float = 0.0
        debt: float = 0.0
        earnings_growth: float = 0.0

        for ticker in tickers_data.tickers.keys():
            try:
                ticker_infos = tickers_data.tickers[ticker]
                website = ticker_infos.info.get('website')
                country = ticker_infos.info.get('country')
                industry = ticker_infos.info.get('industry')
                sector = ticker_infos.info.get('sector')
                employes = ticker_infos.info.get('fullTimeEmployees')
                dy = ticker_infos.info.get('dividendYield')
                peg_ratio = ticker_infos.info.get('pegRatio')
                recomendation = ticker_infos.info.get('recommendationKey')
                ebitda = ticker_infos.info.get('ebitda')
                debt = ticker_infos.info.get('totalDebt')
                earnings_growth = ticker_infos.info.get('earningsGrowth')

                data[ticker] = {
                    'website': website,
                    'country': country,
                    'industry': industry,
                    'sector': sector,
                    'employes': employes,
                    'symbol': ticker,
                    'dy': dy,
                    'pegRatio': peg_ratio,
                    'recomendation': recomendation,
                    'ebitda': ebitda,
                    'debt': debt,
                    'earningsGrowth': earnings_growth
                }
            except Exception:
                pass

        return data

    def get_dividends(self, tickers: str) -> dict:
        '''
        Get dividends from yfinance

        Args:
            tickers (str): tickers to get dividends

        Returns:
            dict: dict of dividends
        '''
        tickers_data = yf.Tickers(tickers)
        data: dict = {}

        for ticker in tickers_data.tickers.keys():
            payments = tickers_data.tickers[ticker].dividends
            paymentDates = list(payments.index.strftime('%Y-%m-%d'))
            amounts = list(payments.values)

            data[ticker] = {
                'paymentDate': paymentDates,
                'amount': amounts
            }

        return data


if __name__ == '__main__':
    indicators = Indicators()
    indicators.get_indicators('AAPL34.SA MSFT34.SA')
    indicators.get_dividends('AAPL34.SA MSFT34.SA')
