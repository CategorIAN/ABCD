#==========My Classes===============================
from General import General
from GuestGame import GuestGame
from GameAvailability import GameAvailability

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
    Av.availability({"Stephen Johnson"}, "23.07", 3)


if __name__ == '__main__':
    pass


