CREATE TABLE MONTH (
    ID INT PRIMARY KEY,
    NAME VARCHAR(160)
);

CREATE TABLE EVENT_PLAN (
    NAME VARCHAR(160),
    MONTH INT REFERENCES MONTH(ID),
    YEAR INT,
    GAME VARCHAR(160) REFERENCES GAMES(NAME)
)