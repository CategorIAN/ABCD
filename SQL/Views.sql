CREATE VIEW X AS
    SELECT PERSONID, EVENTPLANID, WEEK, COLUMNNAME AS DAY, ROWNAME AS HOUR
    FROM person_eventplan_availability JOIN availability on dayhour = availability.id
    JOIN availability_column on availability.columnid = availability_column.columnid
    JOIN availability_row on availability.rowid = availability_row.rowid
    ORDER BY WEEK, AVAILABILITY_COLUMN.COLUMNID, AVAILABILITY_ROW.ROWID;

