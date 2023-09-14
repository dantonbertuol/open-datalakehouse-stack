CREATE TABLE stock (
    id INT NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(10) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (symbol)
)