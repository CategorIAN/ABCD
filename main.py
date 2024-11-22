#==========My Classes===============================
from FormManagement_DB import FormManagement_DB
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
    G.updateResults(11,18,2024)

def read():
    G = General_DB()
    G.readSQL(G.readPersonalFile("Paul"))

def call_list(id):
    E = Event_DB()
    E.executeSQL([E.createCallList(id), E.getCallList(id)])

def f():
    E = Event_DB()
    E.executeSQL([E.updatePersonTimespan])

def request():
    F = FormManagement_DB()
    F.executeSQL([F.request('2024-11-18', 'ABCD General Survey')])

def invite():
    E = Event_DB()
    E.executeSQL([E.invite('2014-11-16', 5)])

if __name__ == '__main__':
    E = Event_DB()
    E.executeSQL([E.createMealPreference(5)])




