CREATE TABLE stock_indicators(
    id INT AUTO_INCREMENT,
    symbol VARCHAR(10) NOT NULL,
    website VARCHAR(100),
    country VARCHAR(100),
    industry VARCHAR(100),
    sector VARCHAR(100),
    employes INT,
    dy FLOAT,
    pegRatio FLOAT,
    recomendation VARCHAR(10),
    ebitda FLOAT,
    debt FLOAT,
    earningsGrowth FLOAT,
    PRIMARY KEY (id),
    UNIQUE (symbol)
)