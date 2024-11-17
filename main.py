#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from EventAvailability import EventAvailability
from MeetingAvailability import MeetingAvailability
from General_DB import General_DB
from Event_DB import Event_DB


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

def update():
    G = General_DB()
    G.updateResults(11,12,2024)

def read():
    G = General_DB()
    G.readSQL(G.readPersonalFile("Paul"))

def f():
    E = Event_DB()
    E.executeSQL([E.updatePersonTimespan])

if __name__ == '__main__':
    update()




