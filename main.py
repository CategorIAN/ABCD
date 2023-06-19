#==========Python Packages==========================
import pandas as pd
import os
#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from Availability_Specific import Availability_Specific

def GuestGames():
    GG = GuestGame()
    GG.availability("Hannah Harris", "23.02")

def availability():
    G = General()
    for i in [1, 2, 3]:
        G.gameAv(i)
    for game in G.game_duration.keys():
        G.newbAv(game)

def game_availability():
    Av = Availability_Specific()
    Av.availability(["Stephen Johnson"], "23.07", 3)


if __name__ == '__main__':
    availability()
    #General()


