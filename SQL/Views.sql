CREATE VIEW Game_Stats AS
    SELECT GAMES.NAME,
           (COUNT(DISTINCT EVENT.EVENTID) / GAMES.WEIGHT::NUMERIC) AS WEIGHTEDCOUNT,
           COUNT(DISTINCT PERSON_GAMES.PERSONID) AS NUMBERINTERESTED
    FROM GAMES LEFT JOIN PERSON_GAMES ON GAMES.name = PERSON_GAMES.gamesid
        LEFT JOIN EVENT ON GAMES.name = EVENT.game
    GROUP BY GAMES.NAME
    ORDER BY WEIGHTEDCOUNT, NUMBERINTERESTED DESC;

CREATE VIEW X AS
    SELECT PERSONID, EVENTPLANID, WEEK, COLUMNNAME AS DAY, ROWNAME AS HOUR
    FROM person_eventplan_availability JOIN availability on dayhour = availability.id
    JOIN availability_column on availability.columnid = availability_column.columnid
    JOIN availability_row on availability.rowid = availability_row.rowid
    ORDER BY WEEK, AVAILABILITY_COLUMN.COLUMNID, AVAILABILITY_ROW.ROWID;


