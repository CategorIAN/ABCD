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

update person
SET Name = 'Shay Williams'
where name = 'Shay Sodexo';

ALTER TABLE FORM_REQUESTS DROP CONSTRAINT form_requests_person_fkey;

ALTER TABLE FORM_REQUESTS
ADD CONSTRAINT form_requests_person_fkey
FOREIGN KEY (person)
REFERENCES person(name)
ON UPDATE CASCADE;