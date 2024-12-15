#==========My Classes===============================
from EventPlanAvailability_DB import EventPlanAvailability_DB
from FormManagement_DB import FormManagement_DB
from General_DB import General_DB
from Event_DB import Event_DB

#============Easy========================================
def addPerson():
    E = Event_DB()
    E.executeSQL([E.addPerson])

def request():
    F = FormManagement_DB()
    F.executeSQL([F.request('ABCD General Survey')])

def invite():
    E = Event_DB()
    E.executeSQL([E.invite('2014-11-30', 6)])

#=========Medium=========================================
def call_list(id):
    E = Event_DB()
    E.executeSQL([E.createCallList(id), E.getCallList(id)])

def meal_preference(id):
    E = Event_DB()
    E.executeSQL([E.createMealPreference(id)])

def read_personal_file(name):
    G = General_DB()
    G.executeSQL([G.readPersonalFile(name)])

#=======Hard=============================================
def update_general():
    G = General_DB()
    G.updateResults(12,6,2024)

def update_epa():
    E = EventPlanAvailability_DB()
    E.executeSQL([E.updateData(12, 9, 2024)])

if __name__ == '__main__':
    request





