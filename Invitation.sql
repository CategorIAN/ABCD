Create TYPE invitation_results as ENUM ('Going', 'Passed', 'Flaked', 'Waiting', 'To Redeem');

Insert Into Invitation (Timestamp, Person, Event, Response, Plus_Ones, Result) VALUES
        ('2024-11-03 14:00:00', 'Dorene McDonald', 3, NULL, 0, 'Passed');