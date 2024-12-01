import os
import pandas as pd
from functools import reduce
import psycopg2
import numpy as np
from itertools import product
from prettytable import PrettyTable

class General_DB:
    def __init__(self):
        self.name = "General_DB"
        self.keys = ["Email"]
        self.df = self.createDF()
        form_types = ["Keys", "Text", "LinScale", "MultChoice"]
        sql_types = ["VARCHAR(160) PRIMARY KEY", "VARCHAR(160)", "INT", "VARCHAR(160)"]
        self.type_dict = dict(zip(form_types, sql_types))
    #=================================================================================================================
    def removeDuplicates(self, df):
        def appendDF(df_keys, index):
            current_df, key_set, df_row = df_keys + (df.loc[[index], :],)
            keys = tuple(df_row.loc[index, self.keys])
            return (current_df, key_set) if keys in key_set else (pd.concat([df_row, current_df]), key_set | {keys})
        initial_df = pd.DataFrame(columns=df.columns)
        return reduce(appendDF, reversed(df.index), (initial_df, set()))[0].reset_index(drop=True)

    def trim_paren(self, v):
        return v.partition(" (")[0]

    def q_map(self, q):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        col_mapping = dict(list(zip(form_columns["Question"], form_columns["Name"])))
        return col_mapping.get(self.trim_paren(q), self.trim_paren(q))

    def r_map(self, r):
        return r.partition(' to')[0]

    def prod_func(self, column):
        question, _, row = column.partition(" [")
        if len(row) == 0:
            return self.q_map(question)
        else:
            return "{} [{}]".format(self.q_map(question), self.r_map(row.strip("]")))

    def df_map(self, features, my_func):
        def f(df):
            transformColumn = lambda df_dict, column: df_dict | {column: [my_func(x) for x in df_dict[column]]}
            return pd.DataFrame(reduce(transformColumn, features, df.to_dict('series')))
        return f

    def save(self, df, file):
        df.to_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(file)]))

    def hashTime(self, s):
        f = lambda weights, s, sep: sum([w * int(t) for (w, t) in zip(weights, s.split(sep))])
        h = lambda l: 1000000 * f([100, 1, 10000], l[0], "/") + f([10000, 100, 1], l[1], ":")
        return h(s.split(" "))

    def createDF(self):
        ren = lambda df: df.rename(self.prod_func, axis=1)
        f = lambda df: df.fillna("")
        rem = lambda df: self.removeDuplicates(df)
        df = rem(f(ren(pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)])))))
        self.save(df, "original")
        return df
    # =================================================================================================================
    def transform(self, form_type, df):
        '''
        :param form_type:
        :param df:
        :return:
        '''
        def f(id):
            if form_type in {"MultChoice", "CheckBox"}:
                sub_df = df[df["ID"] == id]
                transform_dict = dict(zip(sub_df["OldValue"], sub_df["NewValue"]))
                return lambda v: transform_dict.get(self.trim_paren(v), self.trim_paren(v))
            else:
                return lambda v: v
        return f

    def typeDF(self, form_type):
        '''
        :param form_type: Either 'CheckBox', 'CheckBoxGrid', 'LinScale', 'MultChoice', or 'Text'
        :return: A pandas dataframe showing transformations of the question and options for that type.
        '''
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        type_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "{}.csv".format(form_type)]))
        return pd.merge(form_columns, type_columns, how="inner", left_on="ID", right_on="QID")

    def getTypeNames(self, form_type, with_sql_type = False):
        '''
        :param form_type: Either 'CheckBox', 'CheckBoxGrid', 'LinScale', 'MultChoice', or 'Text'.
        :param with_sql_type: Determines whether to append the appropriate SQL type.
        :return: A dictionary mapping question names to transformation function for answers to the question.
        '''
        df = self.typeDF(form_type)
        sql_type = " " + self.type_dict[form_type] if with_sql_type else ""
        name = lambda id: "{}{}".format(df[df["ID"] == id]["Name"].iat[0], sql_type)
        transform_func = self.transform(form_type, df)
        return dict([(name(col_id), transform_func(col_id)) for col_id in set(df.ID)])

    def getDBDict(self, with_sql_type = False):
        '''
        :param with_sql_type: Determines whether to append the appropriate SQL type.
        :return:
        '''
        return reduce(lambda d, t: d | self.getTypeNames(t, with_sql_type), self.type_dict.keys(), {})

    # =================================================================================================================
    def personRow(self, db_dict, db_cols, name):
        person = self.df[self.df["Name"] == name].iloc[0]
        return str(tuple([db_dict[col](person[col]) for col in db_cols])).replace("''", "NULL")

    def insertPersonRows(self, names = None):
        names = list(self.df["Name"].unique()) if names is None else names
        db_dict = self.getDBDict(with_sql_type=False)
        db_cols = list(db_dict.keys())
        rows = ",\n".join(reduce(lambda rows, name: rows + [self.personRow(db_dict, db_cols, name)], names, []))
        return "INSERT INTO person ({}) VALUES\n{};".format(", ".join(db_cols), rows)

    def createPersonTable(self):
        create_stmt = "CREATE TABLE person (\n{}\n);".format(",\n".join(self.getDBDict(with_sql_type = True).keys()))
        return [create_stmt, self.insertPersonRows()]

    def createCheckBoxTables(self):
        df = self.typeDF("CheckBox")
        name = lambda id: df[df["ID"] == id]["Name"].iat[0]
        db_cols = ",\n".join(["Name VARCHAR(160) PRIMARY KEY", "Other BOOLEAN"])
        values = lambda id: ",\n".join([str((name, False)) for name in df[df["ID"] == id]["NewValue"]])
        create_stmt = lambda id: "CREATE TABLE {} (\n{}\n);".format(name(id), db_cols)
        insert_stmt = lambda id: "INSERT INTO {} (Name, Other) VALUES \n{}\n;".format(name(id), values(id))
        return reduce(lambda l, col_id: l + [create_stmt(col_id), insert_stmt(col_id)], set(df.ID), [])

    def personCheckBox(self, df, q_name, tr_func, opts, name):
        person = self.df[self.df["Name"] == name].iloc[0]
        items = lambda qid: set([tr_func(qid)(x) for x in person[q_name(qid)].split(", ")])
        values = lambda qid: ",\n".join([str((name, x)) for x in items(qid) if x in opts(qid)])
        insert_stmt = lambda qid: f"INSERT INTO Person_{q_name(qid)} VALUES \n{values(qid)}\n;"
        return [insert_stmt(qid) for qid in set(df.ID) if len(values(qid)) > 0]

    def insertCheckBoxJoinTables(self, names = None):
        names = list(self.df["Name"].unique()) if names is None else names
        df = self.typeDF("CheckBox")
        q_name = lambda qid: df[df["ID"] == qid]["Name"].iat[0]
        tr_func = self.transform("CheckBox", df)
        opts = lambda qid: set(df[df["ID"] == qid]["NewValue"])
        return reduce(lambda l, name: l + self.personCheckBox(df, q_name, tr_func, opts, name), names, [])

    def createCheckBoxJoinTables(self):
        df = self.typeDF("CheckBox")
        q_name = lambda qid: df[df["ID"] == qid]["Name"].iat[0]
        cascade = "ON DELETE CASCADE ON UPDATE CASCADE"
        db_cols = lambda qid: ",\n".join([f"PersonID VARCHAR(160) REFERENCES Person {cascade}",
                        f"{q_name(qid)}ID VARCHAR(160) REFERENCES {q_name(qid)} "])
        create_stmt = lambda qid: "CREATE TABLE PERSON_{} (\n{}\n);".format(q_name(qid), db_cols(qid))
        create_stmts = reduce(lambda l, qid: l + [create_stmt(qid)], set(df.ID), [])
        return create_stmts + self.insertCheckBoxJoinTables()
    #=================================================================================================================
    def createGridColumnTables(self):
        df = self.typeDF("GridColumn")
        q_df = lambda qid: df[df["ID"] == qid]
        name = lambda qid: q_df(qid)["Name"].iat[0]
        db_cols = ",\n".join(["ColumnID INT PRIMARY KEY", "ColumnName VARCHAR(160)"])
        values = lambda qid: ",\n".join([str(tuple(df.loc[i, ["ColumnID", "ColumnName"]])) for i in q_df(qid).index])
        create_stmt = lambda qid: "CREATE TABLE {}_Column (\n{}\n);".format(name(qid), db_cols)
        insert_stmt = lambda qid: "INSERT INTO {}_Column (ColumnID, ColumnName) VALUES \n{}\n;".format(name(qid), values(qid))
        return reduce(lambda l, qid: l + [create_stmt(qid), insert_stmt(qid)], set(df.ID), [])

    def createGridRowTables(self):
        df = self.typeDF("GridRow")
        q_df = lambda qid: df[df["ID"] == qid]
        name = lambda qid: q_df(qid)["Name"].iat[0]
        db_cols = ",\n".join(["RowID INT PRIMARY KEY", "RowName VARCHAR(160)"])
        values = lambda qid: ",\n".join([str(tuple(df.loc[i, ["RowID", "RowName"]])) for i in q_df(qid).index])
        create_stmt = lambda qid: "CREATE TABLE {}_Row (\n{}\n);".format(name(qid), db_cols)
        insert_stmt = lambda qid: "INSERT INTO {}_Row (RowID, RowName) VALUES \n{}\n;".format(name(qid), values(qid))
        return reduce(lambda l, qid: l + [create_stmt(qid), insert_stmt(qid)], set(df.ID), [])

    def createGridTables(self):
        questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
        grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
        append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
        df = append_df(append_df(questions, grid_columns), grid_rows)
        q_df = lambda qid: df[df["ID"] == qid].reindex()
        name = lambda qid: q_df(qid)["Name"].iat[0]
        cascade = "ON DELETE CASCADE ON UPDATE CASCADE"
        db_cols = lambda qid: ",\n".join(["ID INT PRIMARY KEY",
                            f"ColumnID INT REFERENCES {name(qid)}_Column {cascade}",
                              f"RowID INT REFERENCES {name(qid)}_Row {cascade}"])
        values = lambda qid: ",\n".join([str((i,) + tuple(df.loc[i, ["ColumnID", "RowID"]])) for i in q_df(qid).index])
        create_stmt = lambda qid: "CREATE TABLE {} (\n{}\n);".format(name(qid), db_cols(qid))
        insert_stmt = lambda qid: "INSERT INTO {} (ID, ColumnID, RowID) VALUES \n{}\n;".format(name(qid), values(qid))
        return reduce(lambda l, qid: l + [create_stmt(qid), insert_stmt(qid)], set(df.ID), [])

    def personGrid(self, df, q_name, opts, name):
        person = self.df[self.df["Name"] == name].iloc[0]
        cols = lambda qid, row: set(person[f"{q_name(qid)} [{row}]"].split(", "))
        f = lambda qid, col, row: df[(df["ID"] == qid) & (df["ColumnName"] == col) & (df["RowName"] == row)].index[0]
        values = lambda qid: ",\n".join([str((person["Name"], f(qid, col, row)))
                                         for col, row in opts(qid) if col in cols(qid, row)])
        insert_stmt = lambda qid: f"INSERT INTO Person_{q_name(qid)} VALUES \n{values(qid)}\n;"
        return [insert_stmt(qid) for qid in set(df.ID) if len(values(qid)) > 0]

    def insertGridJoinTables(self, names = None):
        names = list(self.df["Name"].unique()) if names is None else names
        questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
        grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
        append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
        df = append_df(append_df(questions, grid_columns), grid_rows)
        q_df = lambda qid: df[df["ID"] == qid].reindex()
        q_name = lambda qid: q_df(qid)["Name"].iat[0]
        opts = lambda qid: set(product(df[df["ID"] == qid]["ColumnName"], df[df["ID"] == qid]["RowName"]))
        return reduce(lambda l, name: l + self.personGrid(df, q_name, opts, name), names, [])

    def createGridJoinTables(self):
        questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
        grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
        append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
        df = append_df(append_df(questions, grid_columns), grid_rows)
        q_df = lambda qid: df[df["ID"] == qid].reindex()
        q_name = lambda qid: q_df(qid)["Name"].iat[0]
        cascade = "ON DELETE CASCADE ON UPDATE CASCADE"
        db_cols = lambda qid: ",\n".join([f"PersonID VARCHAR(160) REFERENCES Person {cascade}",
                                         f"{q_name(qid)}ID INT REFERENCES {q_name(qid)} {cascade}"])
        create_stmt = lambda qid: "CREATE TABLE PERSON_{} (\n{}\n);".format(q_name(qid), db_cols(qid))
        create_stmts = [create_stmt(qid) for qid in set(df.ID)]
        return create_stmts + self.insertGridJoinTables()

    def create(self):
        create_stmts = [self.createPersonTable(), self.createCheckBoxTables(), self.createCheckBoxJoinTables(),
                self.createGridColumnTables(), self.createGridRowTables(), self.createGridTables(),
                self.createGridJoinTables()]
        return reduce(lambda l, stmts: l + stmts, create_stmts, [])

    def insertSubmission(self, name, timestamp):
        def execute(cursor):
            insert_stmt = f"""
                INSERT INTO FORM_SUBMISSIONS(PERSON, FORM, TIMESTAMP) VALUES {(name, 'ABCD General Survey', timestamp)};
                """
            cursor.execute(insert_stmt)
        return execute

    # =================================================================================================================
    def readText(self, name = None):
        df = self.typeDF("Text")
        cols = ", ".join(list(["Name"] + list(set(df["Name"]))))
        filter = f"WHERE Name LIKE '%{name}%'" if name is not None else ""
        return [f"SELECT {cols} FROM PERSON " + filter + ";"]

    def readLinScale(self, name = None):
        df = self.typeDF("LinScale")
        cols = ", ".join(list(["Name"] + list(set(df["Name"]))))
        filter = f"WHERE Name LIKE '%{name}%'" if name is not None else ""
        return [f"SELECT {cols} FROM PERSON " + filter + ";"]

    def readMultChoice(self, name = None):
        df = self.typeDF("MultChoice")
        cols = ", ".join(list(["Name"] + list(set(df["Name"]))))
        filter = f"WHERE Name LIKE '%{name}%'" if name is not None else ""
        return [f"SELECT {cols} FROM PERSON " + filter + ";"]

    def readCheckBox(self, name = None):
        df = self.typeDF("CheckBox")
        q_df = lambda qid: df[df["ID"] == qid].reindex()
        q_name = lambda qid: q_df(qid)["Name"].iat[0]
        filter = f"WHERE Name LIKE '%{name}%'" if name is not None else ""
        values = lambda qid: (f"SELECT NAME, {q_name(qid)}ID FROM PERSON INNER JOIN PERSON_{q_name(qid)} "
                              f"ON PERSON.NAME = PERSON_{q_name(qid)}.PERSONID ")
        return [values(qid) + filter + ";" for qid in set(df.ID)]

    def readGrid(self, name = None):
        questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
        grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
        append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
        df = append_df(append_df(questions, grid_columns), grid_rows)
        q_df = lambda qid: df[df["ID"] == qid].reindex()
        q_name = lambda qid: q_df(qid)["Name"].iat[0]
        filter = f"WHERE Name LIKE '%{name}%'" if name is not None else ""
        order_by = lambda qid: f"ORDER BY {q_name(qid)}ID ASC"
        values = lambda qid: (f"SELECT NAME, COLUMNNAME, ROWNAME FROM PERSON INNER JOIN PERSON_{q_name(qid)} "
                              f"ON PERSON.NAME = PERSON_{q_name(qid)}.PERSONID INNER JOIN {q_name(qid)} "
                              f"ON PERSON_{q_name(qid)}.{q_name(qid)}ID = {q_name(qid)}.ID INNER JOIN "
                              f"{q_name(qid)}_COLUMN ON {q_name(qid)}.COLUMNID = {q_name(qid)}_COLUMN.COLUMNID INNER JOIN "
                              f"{q_name(qid)}_ROW ON {q_name(qid)}.ROWID = {q_name(qid)}_ROW.ROWID")
        return [" ".join([values(qid), filter, order_by(qid), ";"]) for qid in set(df.ID)]

    def readPersonalFile(self, name = None):
        fxns = [self.readText, self.readLinScale, self.readMultChoice, self.readCheckBox, self.readGrid]
        return reduce(lambda scripts, fxn: scripts + fxn(name), fxns, [])

# =================================================================================================================
    def updatePersonRow(self, name, in_db):
        if in_db:
            db_dict = self.getDBDict(with_sql_type=False)
            db_cols = list(set(db_dict.keys()).difference({"Name"}))
            person = self.df[self.df["Name"] == name].iloc[0]
            update = ",\n".join([f"{q_name} = '{db_dict[q_name](person[q_name])}'".replace("''", "NULL") for q_name in db_cols])
            return ["\n".join(["UPDATE PERSON", "SET " + update, f"WHERE name = '{name}';"])]
        else:
            return [self.insertPersonRows([name])]

    def updatePersonCheckBox(self, name, in_db):
        if in_db:
            df = self.typeDF("CheckBox")
            q_name = lambda qid: df[df["ID"] == qid]["Name"].iat[0]
            delete_stmt = lambda qid: f"DELETE FROM PERSON_{q_name(qid)} WHERE PERSONID = '{name}';"
            delete_stmts = [delete_stmt(qid) for qid in set(df.ID)]
            return delete_stmts + self.insertCheckBoxJoinTables([name])
        else:
            return self.insertCheckBoxJoinTables([name])

    def updatePersonGrid(self, name, in_db):
        if in_db:
            questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
            grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
            grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
            append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
            df = append_df(append_df(questions, grid_columns), grid_rows)
            q_name = lambda qid: df[df["ID"] == qid]["Name"].iat[0]
            delete_stmt = lambda qid: f"DELETE FROM PERSON_{q_name(qid)} WHERE PERSONID = '{name}';"
            delete_stmts = [delete_stmt(qid) for qid in set(df.ID)]
            return delete_stmts + self.insertGridJoinTables([name])
        else:
            return self.insertGridJoinTables([name])

    def updateData(self, month, day, year):
        hashed_time = self.hashTime("/".join([str(x) for x in [month, day, year]]) + " 0:00")
        update_index = [i for i in self.df.index if self.hashTime(self.df.at[i, "Timestamp"]) >= hashed_time]
        names = list(self.df.loc[update_index, "Name"])
        def execute(cursor):
            for name in names:
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM PERSON WHERE NAME = '{name}');")
                in_db = cursor.fetchone()[0]
                for update in [self.updatePersonRow, self.updatePersonCheckBox, self.updatePersonGrid]:
                    commands = update(name, in_db)
                    for command in commands:
                        cursor.execute(command)
                        print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)
        return execute

# =================================================================================================================
    def readSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "database-1.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
            cursor = connection.cursor()
            for command in commands:
                cursor.execute(command)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                data = [{col: x for col, x in zip(columns, row)} for row in results]

                if len(data) > 0:
                    table = PrettyTable()
                    table.field_names = data[0].keys()
                    for entry in data:
                        table.add_row(entry.values())
                    print(table)
                print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)
            connection.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")

    def updateResults(self, month, day, year):
        hashed_time = self.hashTime("/".join([str(x) for x in [month, day, year]]) + " 0:00")
        update_index = [i for i in self.df.index if self.hashTime(self.df.at[i, "Timestamp"]) >= hashed_time]
        update_df = list(self.df.loc[update_index, ["Name", "Timestamp"]])
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "abcd.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
            cursor = connection.cursor()
            for i in update_index:
                name, timestamp = self.df.loc[i, ["Name", "Timestamp"]]
                self.insertSubmission(name, timestamp)(cursor)
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM PERSON WHERE NAME = '{name}');")
                in_db = cursor.fetchone()[0]
                for update in [self.updatePersonRow, self.updatePersonCheckBox, self.updatePersonGrid]:
                    commands = update(name, in_db)
                    for command in commands:
                        cursor.execute(command)
                        print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)
            connection.commit()
        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")

    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "database-1.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
            cursor = connection.cursor()
            for command in commands:
                command(cursor)
            connection.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")






