import pandas as pd
import numpy as np
from People import People
from Form import Form

class Time (Form):
    def __init__(self, df):
        df = self.clean(self.rename_columns, df)
        df = df.fillna("")
        df = self.setDF(df)

        times = {}
        total = {'Name': 'Total'}
        times['Email'] = df['Email']
        times['Name'] = df['Name']
        for hour in self.hours:
                t = 0
                a = []
                for j in range(df.shape[0]):
                    if not pd.isnull(df.at[j, 'Hours']):
                        for span in df.at[j, 'Hours']:
                            if hour == span.split(" to ")[0]:
                                a.append(1)
                                t += 1
                                break
                        else: a.append(0)
                    else: a.append(0)
                times[hour] = a
                total[hour] = t
        grid = pd.DataFrame(times)
        grid = grid.append(total, ignore_index = True)
        df = self.stringDF(df)
        self.df = df
        self.grid = grid

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
    question = "What hours are you available to go paddle boarding, kayaking, or tubing at Hyalite on Saturday, " \
            "June 25th? (Pick between 4 to 8 hours)"
    rename_columns = {time_stamp: 'Timestamp', email: 'Email', question: 'Hours'}
    hours = ['9:00 AM', '10:00 AM', '11:00 AM', '12:00 PM', '1:00 PM', '2:00 PM', '3:00 PM', '4:00 PM', '5:00 PM',
             '6:00 PM']
