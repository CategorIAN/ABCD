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
    Select Name, Timestamp is not NULL as CompletedSurvey
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

