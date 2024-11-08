CREATE VIEW View_3 AS
    Select Name, GamesID, Timespan
    FROM Person Left Outer Join Person_Games on Person.Name = Person_Games.PersonID
    Left Outer Join Person_Timespan on Person.name = Person_Timespan.personid
    WHERE TIMESTAMP is NULL OR (GamesID = 'Catan' and TimeSpan = 'Saturday from 2:00 PM to 4:00 PM');


CREATE VIEW Person_Timespan AS
    Select PersonID, Timespan
    From Person_Availability Join Availability_Timespan on
        Person_Availability.availabilityid = Availability_Timespan.availabilityid
    Group By  Availability_Timespan.timespan, Person_Availability.personid
    Having COUNT(Distinct Availability_Timespan.availabilityid) = (
        SELECT COUNT(*)
        FROM Availability_Timespan as Availability_Timespan_inner
        WHERE Availability_Timespan_Inner.timespan = Availability_Timespan.timespan
        );