import pandas as pd
import numpy as np


class Interest:

    def __init__(self, df):
        df = df.rename(self.rename_columns, axis=1)
        df = df.fillna("")
        df = self.setDF(df)
        for column in ["Own", "Lend", "Borrow", "Rent"]:
            df[column] = df[column].apply(self.notnull(self.deviceF))
        df = self.removeDuplicates(df)
        df = df[df.Interest == 'Yes']
        df = df.drop(['Timestamp', 'Interest'], axis=1)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        df = self.stringDF(df)
        self.df = df

    def myReplace(oldString, newString):
        def f(xx):
            yy = set()
            for x in xx:
                yy.add(x.replace(oldString, newString))
            return yy

        return f



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

    def toString(self, ss):
        string = ""
        for s in ss:
            string = string + s + ", "
        return string.strip(", ")

    def setDF(self, df) -> pd.DataFrame:
        for column in df.columns:
            if (column not in {'Email', 'Interest', 'Name'}):
                df[column] = df[column].apply(lambda x: self.notnull(self.toSet)(x))
        return df

    def stringDF(self, df) -> pd.DataFrame:
        for column in df.columns:
            if (column not in {'Email', 'Interest', 'Name'}):
                df[column] = df[column].apply(self.notnull(self.toString))
        return df

    def deviceF(self, xx):
        devices = {'tube', 'board', 'kayak', 'sailboat', 'raft'}
        yy = set()
        for device in devices:
            for x in xx:
                if device in x.lower():
                    yy.add(device)
        return yy

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        return df

    time_stamp = 'Timestamp'
    email = 'Email Address'
    name = 'What is your name?'
    interest = "Would you be interested in attending group events focused on paddle boarding, kayaking, tubing, etc.? " \
               "(If no, then you can skip the rest of the survey.)"
    own = "Which floating device do you own? (Skip If None)"
    lend = "If you own more than one floating device, would you be willing or able to lend a device to another for a " \
           "group event you are attending?"
    borrow = "If you do not own a good floating device, would you be willing to borrow one from a lender?"
    rent = "If you do not own a good floating device, would you be willing to rent one."
    places = "Which of these places would you be willing to go for an event?"
    months = "Which months are you possibly available to go to an event?"
    days = "Which days are you possibly available to go to an event?"

    rename_columns = {time_stamp: 'Timestamp', email: 'Email', name: 'Name', interest: 'Interest', own: 'Own',
                      lend: 'Lend', borrow: 'Borrow', rent: 'Rent', places: 'Places', months: 'Months', days: 'Days'}




