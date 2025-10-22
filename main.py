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

def delete_person(name):
    G = General_DB()
    G.executeSQL([G.deletePerson(name)])
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#=========Medium=========================================
def read_personal_file(name):
    G = General_DB()
    G.executeSQL([G.readPersonalFile(name)])

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
    read_personal_file("Ian Kessler")





