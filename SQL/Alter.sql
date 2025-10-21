ALTER TABLE games
ADD COLUMN WEIGHT INT;

ALTER TABLE meals
ADD COLUMN WEIGHT INT;

ALTER TABLE event_plan
ADD COLUMN DURATION INT;

DELETE FROM person_eventplan_availability
WHERE PERSONID = 'Ian Kessler';

ALTER TABLE event
add column event_plan varchar(160) references event_plan(name);

alter table event
add column week int;

alter table person
drop column timestamp;

<<<<<<< HEAD
update invitation
set timestamp = '2024-12-14 00:00:00.000000'
where timestamp = '2014-12-14 00:00:00.000000'

=======
update person
SET Name = 'Shay Williams'
where name = 'Shay Sodexo';

ALTER TABLE FORM_REQUESTS DROP CONSTRAINT form_requests_person_fkey;

ALTER TABLE FORM_REQUESTS
ADD CONSTRAINT form_requests_person_fkey
FOREIGN KEY (person)
REFERENCES person(name)
ON UPDATE CASCADE;

ALTER TABLE person
ALTER COLUMN FREQUENCY SET DEFAULT 0;

UPDATE person
SET FREQUENCY = 0;
>>>>>>> d425cc560fc7bcbd75e9e81c7c54a7086b978915

ALTER TABLE INVITATION
DROP COLUMN RESPONSE
