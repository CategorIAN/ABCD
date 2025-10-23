#==========My Classes===============================
from EventPlanAvailability_DB import EventPlanAvailability_DB
from General_DB import General_DB
from Event_DB import Event_DB

#=======Hard=============================================
def update_general():
    G = General_DB()
    G.executeSQL([G.updateData(9, 23,2025)])

def update_epa():
    E = EventPlanAvailability_DB()
    E.executeSQL([E.updateData(9, 13, 2025)])

def addTimeSpan(duration):
    E = Event_DB()
    E.executeSQL([E.insertTimeSpanRows(duration)])

if __name__ == '__main__':
    pass





