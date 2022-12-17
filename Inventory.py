import pandas as pd


class Inventory:
    def __init__(self, df, library = None, all_going = False):
        df = df.fillna("")
        df = df.set_index('Name')
        if library is None:
            library = pd.DataFrame.from_dict({'Lender':[], 'Borrower':[], 'Item':[], 'Status':[]})
            library.index.name = 'ID'
        else:
            library = library.set_index('ID')
        if 'Have' not in df.columns:
            df = df[['Own', 'Lend', 'Borrow', 'Rent']]
            df = df.rename({'Own': 'Have'}, axis=1)
            df['Cost'] = [0] * df.shape[0]
            df['Going'] = [all_going] * df.shape[0]
            df['Lenders'] = [""] * df.shape[0]
        df = self.setDF(df, {'Have', 'Lend', 'Borrow', 'Rent', 'Lenders'})
        self.df = df
        self.library = library
        self.updateLenders()
        self.addRecords()



    def lend(self, lender, borrower, item, record = True):
        if item not in self.df.at[lender, 'Lend']:
            print("Lender does not have item.")
        elif item in self.df.at[borrower, 'Have']:
            print("Borrower already has item.")
        else:
            self.df.at[lender, 'Lend'].remove(item)
            self.df.at[borrower, 'Have'].add(item)
            self.updateLenders()
            if record:
                newRow = pd.DataFrame.from_dict({'Lender':[lender], 'Borrower':[borrower], 'Item':[item],
                                                 'Status':['Lent']})
                self.library = pd.concat([self.library, newRow], ignore_index=True)

    def send_back(self, index):
        self.library.at[index, 'Status'] = 'Returned'

    def rent(self, renter, item, cost = 0):
        if item in self.df.at[renter, 'Have']:
            print("Renter already has item.")
        else:
            self.df.at[renter, 'Have'].add(item)
            self.df.at[renter, 'Cost'] += cost
            self.updateLenders()

    def updateLenders(self):
        for borrower in self.df.index:
            if not self.df.at[borrower, 'Have'] and self.df.at[borrower, 'Going']:
                may_borrow = self.df.at[borrower, 'Borrow']
                for lender in self.df.index:
                    if may_borrow.intersection(self.df.at[lender, 'Lend']) and self.df.at[lender, 'Going']:
                        self.df.at[borrower, 'Lenders'].add(lender)
            else:
                self.df.at[borrower, 'Lenders'].clear()

    def save(self):
        self.df.to_csv("Inventory.csv")
        self.library.to_csv("Library.csv")
        self.df.loc[self.df['Going'] == True].to_csv("Going.csv")

    def yes(self, person):
        self.df.at[person, 'Going'] = True
        self.updateLenders()

    def no(self, person):
        self.df.at[person, 'Going'] = False
        self.updateLenders()

    def addRecords(self):
        for i in self.library.index:
            if self.library.at[i, 'Status'] == 'Lent':
                lender = self.library.at[i, 'Lender']
                borrower = self.library.at[i, 'Borrower']
                item = self.library.at[i, 'Item']
                self.lend(lender, borrower, item, False)

    def toSet(self, string):
        s = set()
        for e in string.split(","):
            if len(e) > 0 and e != "set()":
                s.add(e.strip(" {}'"))
        return s

    def toString(self, ss):
        string = ""
        for s in ss:
            string = string + s + ", "
        return string.strip(", ")

    def setDF(self, df, labels) -> pd.DataFrame:
        for column in labels:
                df[column] = df[column].apply(lambda x: self.notnull(self.toSet)(x))
        return df

    def stringDF(self, df) -> pd.DataFrame:
        for column in df.columns:
            if (column not in {'Email', 'Interest', 'Name'}):
                df[column] = df[column].apply(self.notnull(self.toString))
        return df

    def notnull(self, f):
        def g(x):
            if (not pd.isnull(x)):
                return f(x)
            else:
                return x
        return g

