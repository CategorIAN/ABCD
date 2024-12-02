#==========My Classes===============================
from EventPlanAvailability_DB import EventPlanAvailability_DB
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

def addPerson():
    E = Event_DB()
    E.executeSQL([E.addPerson])

def update_general():
    G = General_DB()
    G.updateResults(11,29,2024)

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
    F.executeSQL([F.request('ABCD General Survey')])

def invite():
    E = Event_DB()
    E.executeSQL([E.invite('2014-11-16', 5)])

def submission():
    F = FormManagement_DB()
    F.executeSQL([F.submission('ABCD General Survey')])

def update_epa():
    E = EventPlanAvailability_DB()
    E.executeSQL([E.updateData(12, 1, 2024)])

if __name__ == '__main__':
    E = EventPlanAvailability_DB()
    E.executeSQL([E.createPersonEventplanTimespanCount])





