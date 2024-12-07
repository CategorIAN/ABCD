import pandas as pd
import psycopg2
from prettytable import PrettyTable
from tabulate import tabulate
from functools import reduce
import os
import re

class Event_DB:
    def __init__(self):
        self.nextHour = self.nextMap(["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM",
                                        "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM", "7:00 PM", "8:00 PM"])

    def queried_df(self, cursor, query):
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [[str(x) for x in tuple(y)] for y in cursor.fetchall()]
        return pd.DataFrame(data=data, columns=columns)

    def updateNext(self, cursor):
        def update_stmt(i):
            col, row = df.loc[i, ['columnid', 'next']]
            print(col, row)
            index = None if row == "None" else df[(df['columnid'] == col) & (df['rowid'] == row)].index[0]
            return f"UPDATE AVAILABILITY SET Next = '{index}' WHERE ID = '{i}';".replace("'None'", "NULL")
        query = """
            SELECT Availability.ID, Availability.ColumnID, Availability.RowID, Availability_Row.Next
            From Availability Inner Join Availability_Row ON Availability.RowID = Availability_Row.RowID;
            """
        df = self.queried_df(cursor, query).set_index('id')
        for i in df.index:
            cursor.execute(update_stmt(i))

    def nextMap(self, times):
        def go(map, current, remaining):
            if len(remaining) == 0:
                return map
            else:
                next = remaining[0]
                return go(map | {current: next}, next, remaining[1:])
        return go({}, times[0], times[1:]) if len(times) > 0 else {}

    def insertTimeSpanRows(self, duration):
        def f(cursor):
            def go(indices, current, remaining):
                if remaining == 0:
                    return indices
                else:
                    return None if current == "None" else go(indices + [current], df.at[current, 'next'], remaining - 1)
            def timespan(start):
                times = go([start], df.at[start, 'next'], duration - 1)
                if times is None:
                    return None
                else:
                    day, starthour = tuple(df.loc[start, ['columnname', 'rowname']])
                    lasthour = self.nextHour[df.at[times[-1], 'rowname']]
                    name = f"'{day} from {starthour} to {lasthour}'"
                    name_insert = f"INSERT INTO TimeSpan (Name) Values (\n{name}\n);"
                    values = ",\n".join([f"({time}, {name})" for time in times])
                    values_stmt = f"INSERT INTO Availability_TimeSpan (AvailabilityID, TimeSpan) VALUES \n{values}\n;"
                    return [name_insert, values_stmt]

            query = """
                SELECT Availability.ID, Availability.Next, Availability_Column.ColumnName, Availability_Row.RowName
                FROM AVAILABILITY INNER JOIN AVAILABILITY_COLUMN ON AVAILABILITY.ColumnID = Availability_Column.ColumnID
                INNER JOIN Availability_Row ON Availability.RowID = Availability_Row.RowID;
                """

            df = self.queried_df(cursor, query).set_index('id')
            insert_stmts = reduce(lambda l, i: l if timespan(i) is None else l + timespan(i), df.index, [])
            for stmt in insert_stmts:
                print(stmt)
                cursor.execute(stmt)
        return f

    def createPersonTimespan(self, cursor):
        create_stmt = """
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
        """
        cursor.execute(create_stmt)

    def createPersonRedeem(self, cursor):
        create_stmt = """
            CREATE View Person_Redeem AS
            Select Name, Exists (
                Select 1 From invitation
                         Where person = person.name and result = 'To Redeem'
            ) as Redeem From Person
        """
        cursor.execute(create_stmt)

    def createPersonNumberPlayed(self, cursor):
        create_stmt = """
            CREATE VIEW Person_NumberPlayed AS
            Select Name, (
                Select Count(*) From invitation
                Where person = person.name and result = 'Going'
            ) as NumberPlayed From Person;
        """
        cursor.execute(create_stmt)

    def createTimeSpanDuration(self, cursor):
        create_stmt = """
            CREATE VIEW TimeSpan_Duration AS
            Select TimeSpan.name, Count(*) as Duration
            FROM TimeSpan JOIN Availability_Timespan on Timespan.name = Availability_Timespan.timespan
            Group By TimeSpan.name;
        """
        cursor.execute(create_stmt)

    def createCallList(self, event_id):
        def execute(cursor):
            df = self.queried_df(cursor, f"Select Game, Timespan From Event Where EventId = '{event_id}';")
            game, timespan = tuple(df[['game', 'timespan']].iloc[0])
            create_stmt = f"""
            CREATE VIEW Call_List_{event_id} AS
            Select Person.Name, Redeem, New, CompletedSurvey, ExpectedAttendance, ExpectedInvite
            FROM Person Left Outer Join Person_Games on Person.Name = Person_Games.PersonID
            Left Outer Join Person_Timespan on Person.name = Person_Timespan.personid
            Left Outer Join Person_Redeem on Person.name = Person_Redeem.name
            Left Outer Join Person_CompletedSurvey on Person.name = Person_CompletedSurvey.Name
            Left Outer Join Person_Expected on Person.name = Person_Expected.Name
            Left Outer Join Person_Due on Person.name = Person_Due.name
            Where (Redeem or (EventDue and InviteDue)) and 
                   Person.Name != 'Ian Kessler' and Status = 'Active' and
                  (TIMESTAMP is NULL OR (GamesID = '{game}' and TimeSpan = '{timespan}'))
            Order By Redeem Desc, New Desc, CompletedSurvey Desc, ExpectedAttendance, ExpectedInvite, Person.Name;
            """
            cursor.execute(create_stmt)
        return execute

    def getCallList(self, event_id):
        def execute(cursor):
            df = self.queried_df(cursor, f"SELECT * FROM CALL_LIST_{event_id}")
            checkboxes = df.shape[0] * [{col: False for col in ['I', 'G', 'P', 'W', 'D', 'F']}]
            appended_df = pd.concat([df['name'], pd.DataFrame(checkboxes), df.loc[:, df.columns != 'name']], axis=1)
            appended_df.to_csv("\\".join([os.getcwd(), 'call_lists', f"call_list_{event_id}.csv"]), index=False)
        return execute

    def createAvailability(self, game, duration, newb = True):
        def execute(cursor):
            create_stmt = f"""
            CREATE VIEW Availability_{re.sub("[ :]","_",game)}_{duration}Hrs{"_Newb" if newb else ""} AS
                SELECT Person_Timespan.Timespan, Count(*) as NumberAvailable
                FROM PERSON_TimeSpan JOIN Person_Games on Person_Timespan.personid = Person_Games.personid
                                     JOIN Timespan_Duration on Person_Timespan.timespan = Timespan_Duration.name
                                     JOIN Person_Numberplayed on Person_Timespan.personid = Person_Numberplayed.name
                WHERE Person_Games.gamesid = '{game}' and Timespan_Duration.duration = '{duration}' and
                {"Person_Numberplayed.numberplayed = '0'" if newb else "True"} 
                Group By Person_Timespan.Timespan Order By NumberAvailable Desc;
            """
            cursor.execute(create_stmt)
        return execute

    def createTimeSpanGameCount(self, cursor):
        create_stmt = f"""
        CREATE VIEW TimeSpan_GameCount AS
            SELECT GAMESID, TIMESPAN, COUNT(*) AS NUMBERAVAILABLE
            FROM PERSON_GAMES JOIN PERSON_TIMESPAN ON PERSON_GAMES.PERSONID = PERSON_TIMESpan.PERSONID
            GROUP BY GAMESID, TIMESPAN;
        """
        cursor.execute(create_stmt)

    def createMealPreference(self, event_id):
        def execute(cursor):
            create_stmt = f"""
            CREATE VIEW MealPreference_{event_id} AS
                SELECT MEALS.NAME, Count(DISTINCT PERSON_MEALS.personid) as NUMBERINTERESTED, 
                (COUNT(DISTINCT EVENT.EVENTID) / MEALS.WEIGHT::NUMERIC) AS WEIGHTEDCOUNT
                FROM MEALS JOIN PERSON_MEALS ON MEALS.NAME = PERSON_MEALS.mealsid
                LEFT JOIN EVENT ON MEALS.name = EVENT.meal
                JOIN INVITATION ON PERSON_MEALS.personid = INVITATION.person
                WHERE INVITATION.EVENT = '{event_id}' and RESULT = 'Going'
                GROUP BY MEALS.NAME ORDER BY NUMBERINTERESTED DESC, WEIGHTEDCOUNT;
            """
            cursor.execute(create_stmt)
        return execute

    def invite(self, timestamp, event_id):
        def execute(cursor):
            columns = ["Person", "Response", "Plus_Ones", "Result"]
            values = []
            for column in columns:
                value = input(f"{column}: ")
                values.append(value)
            invite_stmt = (f"Insert INTO Invitation (Timestamp, Event, Person, Response, Plus_Ones, Result) VALUES "
                    f"{(timestamp, event_id) + tuple(values)};".replace("''", "NULL"))
            cursor.execute(invite_stmt)
        return execute

    def addPerson(self, cursor):
        name = input("Name: ")
        stmt = f"Insert INTO Person (Name, Status) Values ('{name}', 'Active');"
        cursor.execute(stmt)

    def readSQL(self, commands):
        def execute(cursor):
            for command in commands:
                df = self.queried_df(cursor, command)
                pretty_df = tabulate(df, headers='keys', tablefmt='pretty')
                print(pretty_df)
                print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)
        return execute

    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "abcd.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
            cursor = connection.cursor()
            for command in commands:
                command(cursor)
            connection.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")