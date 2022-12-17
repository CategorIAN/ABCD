from People import People
import pandas as pd



class Form:
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


    def clean(self, columns, df, names = True):
        df = df.rename(columns, axis=1)
        df = self.removeDuplicates(df)
        df = df.drop(['Timestamp'], axis=1)
        if names: df = self.addNames(df)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        return df

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


