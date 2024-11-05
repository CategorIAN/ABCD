CREATE TABLE Availability_TimeSpan
(
    AvailabilityID Int References Availability(ID),
    TimeSpan VARCHAR(160) References TimeSpan(Name)
);