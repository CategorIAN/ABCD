CREATE VIEW Person_CompletedSurvey AS
    Select Name, EXISTS (SELECT 1
                         FROM form_submissions where person = Name and form = 'ABCD General Survey') as CompletedSurvey
    From Person;

CREATE VIEW Person_CompletedEPA as
    Select person.name as Name, event_plan.name as eventplanid, exists (select 1 from person_eventplan_availability
                    where personid = person.name and
                          person_eventplan_availability.eventplanid = event_plan.name) as submitted_epa
    from person cross join event_plan;

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

CREATE VIEW Person_General_Requested AS
    SELECT PERSON.NAME, person_latestrequest.latestrequest as requested
    FROM PERSON LEFT JOIN PERSON_LATESTREQUEST
        ON PERSON.NAME = PERSON_LATESTREQUEST.NAME
        where ((person_latestrequest.form = 'ABCD General Survey') or (person_latestrequest.form is NULL))
        and person.name != 'Ian Kessler'
        ORDER BY latestrequest nulls first;

CREATE VIEW Person_General_Updated AS
    SELECT PERSON.NAME, Updated
    FROM PERSON LEFT JOIN PERSON_UPDATED
        ON PERSON.NAME = PERSON_UPDATED.NAME
        where ((person_updated.form = 'ABCD General Survey') or (person_updated.form is NULL))
        and person.name != 'Ian Kessler'
        ORDER BY Updated nulls first;

CREATE VIEW Person_General_Due as
    SELECT person_general_requested.name,
           ((requested is NULL) and (updated is null)) as request,
               ((UPDATED + INTERVAL '1 year' < now()) and (updated is not null) and
                ((requested < updated) or (requested is NULL))) as renew,
            (
                (requested + interval '3 months' < now()) and (requested is not null) and
                ((updated < requested) or (updated is NULL))
            ) as request_again,
            requested, updated
    from person_general_requested join person_general_updated on person_general_requested.name = person_general_updated.name
    order by request desc, renew desc, request_again desc, requested nulls first, updated nulls first;


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


