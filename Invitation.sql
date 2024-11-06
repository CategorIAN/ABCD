Create TYPE invitation_results as ENUM ('Going', 'Passed', 'Flaked', 'Waiting', 'To Redeem');



Create Table Invitation
(
    Timestamp Timestamp,
    Person Varchar(160) References Person(Name),
    Event Int References Event(EventID),
    Response Boolean,
    Plus_Ones Int,
    Result invitation_results
);

Insert Into Invitation (Timestamp, Person, Event, Response, Plus_Ones, Result) VALUES
        ('2024-10-26 18:00:00', '', 2, false, 0, 'Passed');