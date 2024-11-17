Create TYPE invitation_results as ENUM ('Going', 'Passed', 'Flaked', 'Waiting', 'To Redeem');

Insert Into Invitation (Timestamp, Person, Event, Response, Plus_Ones, Result) VALUES
        ('2024-11-09 18:00:00', 'Dorene McDonald', 4, NULL, 0, 'Passed');


Alter table person
Add Column frequency int default '1';
