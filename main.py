#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from GameAvailability import GameAvailability
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
    Av = GameAvailability()
    Av.availability("24.04", 3)

def availabilityMeeting():
    Av = MeetingAvailability()
    Av.availability(1)


if __name__ == '__main__':
    availabilityMeeting()



