#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from EventAvailability import EventAvailability
from MeetingAvailability import MeetingAvailability

def availabilityGG():
    GG = GuestGame()
    GG.availability("Hannah Harris", "23.02")

def availabilityGeneral():
    G = General()
    for i in [1, 2, 3]:
        G.gameAv(i)
    for game in G.game_duration.keys():
        G.newbAv(game)

def availabilityGame():
    Av = EventAvailability()
    Av.availability("24.04", 3)

def availabilityMeeting():
    Av = MeetingAvailability()
    Av.availability(1)

if __name__ == '__main__':
    G = General()
    #G.updateResults(5, 19, 2024)
    #my_dict = G.updateResults(1, 22, 2024)




