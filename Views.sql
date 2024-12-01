CREATE VIEW Call_List_3 AS
    Select Person.Name, Redeem, New, CompletedSurvey, ExpectedAttendance, ExpectedInvite
    FROM Person Left Outer Join Person_Games on Person.Name = Person_Games.PersonID
    Left Outer Join Person_Timespan on Person.name = Person_Timespan.personid
    Left Outer Join Person_Redeem on Person.name = Person_Redeem.name
    Left Outer Join Person_CompletedSurvey on Person.name = Person_CompletedSurvey.Name
    Left Outer Join Person_Expected on Person.name = Person_Expected.Name
    Left Outer Join Person_Due on Person.name = Person_Due.name
    Where (Redeem or (EventDue and InviteDue)) and (Person.Name != 'Ian Kessler') and
          (TIMESTAMP is NULL OR (GamesID = 'Catan' and TimeSpan = 'Saturday from 2:00 PM to 4:00 PM'))
    Order By Redeem Desc, New Desc, CompletedSurvey Desc, ExpectedAttendance, ExpectedInvite, Person.Name;

CREATE VIEW Person_CompletedSurvey AS
    Select Name, EXISTS (SELECT 1
                         FROM form_submissions where person = Name and form = 'ABCD General Survey') as CompletedSurvey
    From Person;

CREATE VIEW Person_LatestInvite AS
    Select Person as Name, Max(timestamp) as LatestInvite
    from invitation
    group by Name;

Create View Person_LatestAttendance AS
    Select Person as Name, Max(event.timestamp) as LatestAttendance
    from invitation join event on invitation.event = event.eventid
    where invitation.result = 'Going'
    group by Name;

Create View Person_Expected AS
    Select Person.Name,
           LatestInvite + (Frequency || 'week')::Interval as ExpectedInvite,
           LatestAttendance + (Frequency || 'week')::Interval as ExpectedAttendance
    From Person Left Join Person_LatestInvite On Person.name = Person_LatestInvite.Name
    Left Join Person_LatestAttendance on Person.name = Person_LatestAttendance.Name;

Create View Person_Due AS
    Select Name,
           (ExpectedInvite <= Now()) or (ExpectedInvite is NULL) as InviteDue,
           (ExpectedAttendance <= Now() + Interval '2 weeks') or (ExpectedAttendance is NULL) as EventDue,
           (ExpectedInvite is NULL) as New
    From Person_Expected;

Create View Person_Updated AS
    SELECT NAME, FORM, MAX(FORM_SUBMISSIONS.TIMESTAMP) AS UPDATED
    FROM PERSON JOIN FORM_SUBMISSIONS ON PERSON.NAME = FORM_SUBMISSIONS.PERSON
    GROUP BY NAME, FORM;

CREATE VIEW Person_LatestRequest AS
    SELECT NAME, FORM, MAX(FORM_REQUESTS.TIMESTAMP) AS LATESTREQUEST
    FROM PERSON JOIN FORM_REQUESTS ON PERSON.name = FORM_REQUESTS.person
    GROUP BY NAME, FORM;

CREATE VIEW Person_GeneralDue AS
    SELECT PERSON.NAME, (((UPDATED + INTERVAL '1 year' < now()) OR (UPDATED IS NULL)) AND
                 ((LATESTREQUEST + INTERVAL '3 months' < now()) OR (LATESTREQUEST IS NULL))) as GeneralDue
    FROM PERSON LEFT JOIN (SELECT * FROM PERSON_UPDATED WHERE FORM = 'ABCD General Survey') AS PERSON_UPDATED
        ON PERSON.NAME = PERSON_UPDATED.NAME
        LEFT JOIN (SELECT * FROM PERSON_LATESTREQUEST WHERE FORM = 'ABCD General Survey') AS PERSON_LATESTREQUEST
            ON PERSON.NAME = PERSON_LATESTREQUEST.NAME
        ORDER BY GeneralDue DESC;

CREATE VIEW Game_Stats AS
    SELECT GAMES.NAME,
           (COUNT(DISTINCT EVENT.EVENTID) / GAMES.WEIGHT) AS WEIGHTEDCOUNT,
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


