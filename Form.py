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
        df = df.drop(['Timestamp'], axis=1)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        return df

    def addNames(self, df):
        P = People()
        names = []
        for i in range(df.shape[0]):
            names.append(P.lookup[df.at[i, 'Email']])
        df.insert(1, "Name", names)
        return df

    def setDF(self, df, features) -> pd.DataFrame:
        def toSet(string):
            s = set()
            for e in string.split(","):
                if len(e) > 0 and e != "set()":
                    s.add(e.strip(" {}'"))
            return s
        for column in features:
            df[column] = df[column].apply(lambda x: toSet(x))
        return df

    def stringDF(self, df, features) -> pd.DataFrame:
        def toString(ss):
            string = ""
            for s in ss:
                string = string + s + ", "
            return string.strip(", ")
        for column in features:
                df[column] = df[column].apply(toString)
        return df




