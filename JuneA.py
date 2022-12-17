import pandas as pd
import numpy as np
from People import People

class JuneA:
    def __init__(self, df):
        df = df.rename(self.rename_columns, axis=1)
        df = df.fillna("")
        df = self.setDF(df)
        df = self.removeDuplicates(df)
        df = df.drop(['Timestamp'], axis=1)
        df = self.addNames(df)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)

        june = {}
        total = {'Name': 'Total'}
        june['Email'] = df['Email']
        june['Name'] = df['Name']
        for week in ['Week_1', 'Week_2', 'Week_3', 'Week_4']:
            for i in range(len(self.dates[week])):
                t = 0
                a = []
                for j in range(df.shape[0]):
                    if (not pd.isnull(df.at[j, week]) and self.days[i] in df.at[j, week]):
                        a.append(1)
                        t += 1
                    else: a.append(0)
                june[self.dates[week][i]] = a
                total[self.dates[week][i]] = t

        grid = pd.DataFrame(june)
        grid = grid.append(total, ignore_index = True)

        df = self.stringDF(df)
        self.df = df
        self.grid = grid


    def setDF(self, df) -> pd.DataFrame:
        for column in df.columns:
            if (column not in {'Email', 'Name'}):
                df[column] = df[column].apply(lambda x: self.notnull(self.toSet)(x))
        return df

    def notnull(self, f):
        def g(x):
            if (not pd.isnull(x)):
                return f(x)
            else:
                return x
        return g

    def toSet(self, string):
        s = set()
        for e in string.split(","):
            s.add(e.strip())
        return s

    def stringDF(self, df) -> pd.DataFrame:
        for column in df.columns:
            if (column not in {'Email', 'Name'}):
                df[column] = df[column].apply(self.notnull(self.toString))
        return df

    def toString(self, ss):
        string = ""
        for s in ss:
            string = string + s + ", "
        return string.strip(", ")

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        return df

    def addNames(self, df):
        P = People()
        names = []
        for i in range(df.shape[0]):
            names.append(P.lookup[df.at[i, 'Email']])
        df.insert(1, "Name", names)
        return df

    time_stamp = "Timestamp"
    email = "Email Address"
    week1 = "What days are you probably available for PKT events in June? [Week 1 (June 3rd - June 5th)]"
    week2 = "What days are you probably available for PKT events in June? [Week 2 (June 10th - June 12th)]"
    week3 = "What days are you probably available for PKT events in June? [Week 3 (June 17th - June 19th)]"
    week4 = "What days are you probably available for PKT events in June? [Week 4 (June 24th - June 26th)]"
    rename_columns = {time_stamp: 'Timestamp', email: 'Email', week1: 'Week_1', week2: 'Week_2', week3: 'Week_3',
                      week4: 'Week_4'}

    dates = {'Week_1':['June 3rd', 'June 4th', 'June 5th'], 'Week_2':['June 10th', 'June 11th', 'June 12th'],
             'Week_3':['June 17th', 'June 18th', 'June 19th'], 'Week_4':['June 24th', 'June 25th', 'June 26th']}


    week1_dates = ['June 3rd', 'June 4th', 'June 5th']
    week2_dates = ['June 10th', 'June 11th', 'June 12th']
    week3_dates = ['June 17th', 'June 18th', 'June 19th']
    week4_dates = ['June 24th', 'June 25th', 'June 26th']
    days = ['Friday', 'Saturday', 'Sunday']

