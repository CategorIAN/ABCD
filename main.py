#==========Python Packages==========================
import pandas as pd
import os
#==========My Classes===============================
from General import General
from Availability_General import Availability_General
from Availability_Specific import Availability_Specific

def GuestGames():
    df = pd.read_csv(os.getcwd() + '\\' + 'Raw Data' + '\\' + "GG_rawdata.csv")
    #GG = GuestGame(df)
    #GG.availability("Maria Gesior", "23.06")

def availability():
    df = pd.read_csv("\\".join([os.getcwd(), 'ABCD', "ABCD_availability.csv"]), index_col = 0)
    Av = Availability_General()
    Av.dayAvailability()
    for i in [1, 2, 3]:
        Av.gameAv(i)
    for game in ["Codenames", "Catan"]:
        Av.newbAv(game)

def newb_availability():
    df = pd.read_csv("\\".join([os.getcwd(), 'ABCD', "ABCD_availability.csv"]), index_col=0)
    Av = Availability_General()
    print(Av.newbAv("War of the Ring"))

def game_availability():
    Av = Availability_Specific()
    Av.availability(["Steve Aubrecht"], "23.06", 3)


if __name__ == '__main__':
    General()


