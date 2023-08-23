from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))
from src.api.api_consumer import Consumer  # noqa: E402


class BrapiAPI(Consumer):
    '''
    Code to extract data from brapi.dev
    inherits from Consumer class
    '''
