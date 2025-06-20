#==========My Classes===============================
from EventPlanAvailability_DB import EventPlanAvailability_DB
from FormManagement_DB import FormManagement_DB
from General_DB import General_DB
from Event_DB import Event_DB

#~~~~~~~~~~~~~~~~In Web Application~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#============Easy========================================
def addPerson():
    E = Event_DB()
    E.executeSQL([E.addPerson])

def request():
    F = FormManagement_DB()
    F.executeSQL([F.request('ABCD General Survey')])

def invite():
    E = Event_DB()
    E.executeSQL([E.invite('2014-12-14', 7)])
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#=========Medium=========================================
def call_list(id):
    E = Event_DB()
    E.executeSQL([E.createCallList(id), E.getCallList(id)])

def call_list_csv(id):
    E = Event_DB()
    E.executeSQL([E.getCallList(id)])

def meal_preference(id):
    E = Event_DB()
    E.executeSQL([E.createMealPreference(id)])

def read_personal_file(name):
    G = General_DB()
    G.executeSQL([G.readPersonalFile(name)])

#=======Hard=============================================
def update_general():
    G = General_DB()
    G.executeSQL([G.updateData(6, 11,2025)])

def update_epa():
    E = EventPlanAvailability_DB()
    E.executeSQL([E.updateData(12, 15, 2024)])

def delete_person(name):
    G = General_DB()
    G.executeSQL([G.deletePerson(name)])

if __name__ == '__main__':
    meal_preference(14)





