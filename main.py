#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from EventAvailability import EventAvailability
from MeetingAvailability import MeetingAvailability
from General_DB import General_DB


def availabilityGG():
    GG = GuestGame()
    GG.availability("Hannah Harris", "23.02")

def availabilityGeneral():
    G = General()
    for i in [1, 2, 3]:
        G.gameAv(i)
    for game in G.game_duration.keys():
        G.newbAv(game)

def availabilityEvent():
    Av = EventAvailability()
    Av.availability("24.06", 3)

def availabilityMeeting():
    Av = MeetingAvailability()
    Av.availability(1)

def createPersonDB():
    G = General_DB()
    G.executeSQL(G.createPersonTable())

def createCheckBoxJoinTables():
    G = General_DB()
    G.executeSQL(G.createCheckBoxJoinTables())

def create():
    G = General_DB()
    G.getDDL(G.createPersonTable())
    G.getDDL(G.createCheckBoxTables())
    G.getDDL(G.createCheckBoxJoinTables())
    G.getDDL(G.createGridColumnTables())
    G.getDDL(G.createGridRowTables())
    G.getDDL(G.createGridTables())
    G.getDDL(G.createGridJoinTables())

if __name__ == '__main__':
    pass





