import pandas as pd
import numpy as np
from Interest import Interest
from JuneA import JuneA
from JulyA import JulyA
from Time import Time
from Inventory import Inventory
from ABCD import ABCD
from People import People
from GuestGame import GuestGame
import os


def toSet(string):
    s = set()
    for e in string.split(","):
        s.add(e.strip())
    return s

def setDF(df) -> pd.DataFrame:
    for column in df.columns:
        if (column not in {'Email', 'Interest', 'Name'}):
            df[column] = df[column].apply(lambda x: notnull(toSet)(x))
    return df

def notnull(f):
    def g(x):
        if (not pd.isnull(x)):
            return f(x)
        else:
            return x
    return g

def cleanInterest():
    df = pd.read_csv("PKT_Event_Interest.csv")
    I = Interest(df)
    I.df.to_csv("PKT_Event_Interest_cleaned.csv")

def june():
    df = pd.read_csv("June_Availability.csv")
    J = JuneA(df)
    J.df.to_csv("June_Availability_cleaned.csv")
    J.grid.to_csv("June_Availability_grid.csv")

def july():
    df = pd.read_csv("July_Availability.csv")
    J = JulyA(df)
    J.df.to_csv("July_Availability_cleaned.csv")
    J.grid.to_csv("July_Availability_grid.csv")

def master():
    df1 = pd.read_csv("PKT_Event_Interest.csv")
    I = Interest(df1)
    interest_df = I.df
    df2 = pd.read_csv("June_Availability.csv")
    J = JuneA(df2)
    juneA_df = J.grid
    master = interest_df.merge(right = juneA_df, how = 'left', on = ('Email', 'Name'))
    df3 = pd.read_csv("July_Availability.csv")
    J = JulyA(df3)
    julyA_df = J.grid
    master = master.merge(right=julyA_df, how='left', on=('Email', 'Name'))
    master.to_csv("Master.csv")

def check():
    master = pd.read_csv("Master.csv")
    date = master.loc[(master['Places'].str.contains('Hyalite')) & (master['June 25th'] == 1)]
    print(date[['Name', 'Lend', 'Own']])

def times():
    df = pd.read_csv("June 25th @ Hyalite.csv")
    T = Time(df)
    T.df.to_csv("June 25th @ Hyalite cleaned.csv")
    T.grid.to_csv("June 25th @ Hyalite grid.csv")

def createNewInventory():
    df = pd.read_csv("PKT_Event_Interest_cleaned.csv")
    I = Inventory(df, all_going=False)
    I.yes('Ian Kessler')
    I.yes('Andrea Jones')
    I.yes('Catherine Koenen')
    I.yes('Erika Johnson')
    I.yes('Caleb Baker')
    I.save()

def updateInventory():
    df = pd.read_csv("Inventory.csv")
    library = pd.read_csv("Library.csv")
    I = Inventory(df, library, all_going=False)
    I.save()

def Gaming():
    df = pd.read_csv("ABCD_General.csv")
    A = ABCD(df)
    A.df.to_csv("ABCD_cleaned.csv")
    A.av.to_csv("ABCD_availability.csv")
    A.g.to_csv("ABCD_games.csv")
    A.gt.to_csv("ABCD_game_types.csv")
    A.gg.to_csv("ABCD_guestgames.csv")

def GuestGames():
    df = pd.read_csv(os.getcwd() + '\\' + 'Raw Data' + '\\' + "GG_rawdata.csv")
    GG = GuestGame(df)
    GG.availability("Erika Johnson", "23.01")



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Gaming()







# See PyCharm help at https://www.jetbrains.com/help/pycharm/
