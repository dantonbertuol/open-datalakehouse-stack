CREATE TABLE stock_dividends (
    id INT AUTO_INCREMENT,
    symbol VARCHAR(10) NOT NULL,
    paymentDate VARCHAR(50),
    amount FLOAT,
    PRIMARY KEY (id),
    UNIQUE (symbol, paymentDate)
)