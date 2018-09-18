from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import pymysql, time, datetime
import re

# Google sheet configurations
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
SPREADSHEET_ID = '10fj1w8G5Pu7YuPBNPDW2IrEpXvsTHN3oe58YjdbGhGc'
SPREADSHEET_LIMIT = 11
SPREADSHEETS = [
    ["Projects", "AU"],
    ["Ref Postes", "F"],
    ["Charte", "F"],
    ["Techniciens", "BL"],
    ["Plateau", "AU"],
    ["Constantes", "B"],
    ["Semaines", "T"],
    ["Punch", "DC"],
    ["Langue", "D"],
    ["Users", "L"],
    ["Check 2", "E"],

    ["Map-Projects", "D"],
    ["Map-Ref Postes", "D"],
    ["Map-Charte", "D"],
    ["Map-Techniciens", "D"],
    ["Map-Plateau", "D"],
    ["Map-Constantes", "D"],
    ["Map-Semaines", "D"],
    ["Map-Punch", "D"],
    ["Map-Langue", "D"],
    ["Map-Users", "D"],
    ["Map-Check 2", "D"]
]

# DB configurations
DB_HOST = "localhost"
UNIX_SOCKET = "/Applications/XAMPP/xamppfiles/var/mysql/mysql.sock"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "sp1A"

def queryReplacer(query):
    return query.replace('#', '').replace('-', '').replace('?', '').replace('/', '').replace('.', '')

def configDB(tableName, columns, columns_info):
    mydb = pymysql.connect(
        host = DB_HOST,
        unix_socket = UNIX_SOCKET,
        user = DB_USER,
        passwd = DB_PASS,
        database = DB_NAME
    )

    mycursor = mydb.cursor()

    query = "SHOW TABLE STATUS LIKE '" + tableName + "';"
    res = mycursor.execute(query)
    if(not res):
        query = "CREATE TABLE `" + tableName + "` ("
        key_info = ""
        index = 0
        for column in columns:
            index += 1
            column_info = columns_info[index]
            if "PK" in column_info[2]:
                key_info += "PRIMARY KEY (`" + column + "`), "
            if "FK" in column_info[2]:
                column_key_info = column_info[2].split("FK: ")[1].split(".")
                key_info += "FOREIGN KEY (`" + column + "`) REFERENCES `" + column_key_info[0] + "`(`" + column_key_info[1] + "`), "
            if "Always null" in column_info[2]:
                query += ("`" + column + "` " + column_info[1] + " null, ")
            elif "Null" in column_info[0]:
                query += ("`" + column + "` Varchar(1), ")
            else:
                query += ("`" + column + "` " + column_info[1] + ", ")
        query = query + key_info
        query = query[:-2]
        query += ");"
        # query = queryReplacer(query)
        mycursor.execute(query)

    mydb.close()

def getData():
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))

    sheetData = []
    for SPREADSHEET in SPREADSHEETS:
        RANGE_NAME = SPREADSHEET[0] + '!A1:' + SPREADSHEET[1]
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME).execute()
        values = result.get('values', [])
        sheetData.append([SPREADSHEET[0], values])

    index = 0
    for tableData in sheetData:
        if index < SPREADSHEET_LIMIT:
            tableName = tableData[0]
            columns = tableData[1][0]
            columns_info = sheetData[index + SPREADSHEET_LIMIT][1]
            configDB(tableName, columns, columns_info)
            index += 1

    return sheetData

def pushData(sheetData):
    mydb = pymysql.connect(
        host = DB_HOST,
        unix_socket = UNIX_SOCKET,
        user = DB_USER,
        passwd = DB_PASS,
        database = DB_NAME
    )

    mycursor = mydb.cursor()

    query = "SET FOREIGN_KEY_CHECKS = 0;"
    mycursor.execute(query)
    mydb.commit()

    sheets = sheetData[:11]
    err_in_query_run = []
    table_index = 0
    for tableData in sheets:
        column_info = sheetData[table_index + SPREADSHEET_LIMIT][1]
        columns = ""
        query = ""
        tableName = tableData[0]
        tableContent = tableData[1]
        columnNames = tableContent[0]
        columnContents = tableContent[1:]
        columnCount = 0

        index = 0
        for columnName in columnNames:
            index += 1
            if "Always Null" not in column_info[index][2] and "Null" not in column_info[index][0]:
                columnCount += 1
                columns += ("`" + columnName + "`, ")
        columns = columns[:-2]

        for columnContent in columnContents:
            index = 0
            values = ""
            for value in columnContent:
                index += 1
                if "Always Null" not in column_info[index][2] and "Null" not in column_info[index][0]:
                    if "Varchar" in column_info[index][1]:
                        checkValue = str(value).replace(' ', '')
                        if checkValue:
                            values += ('"' + str(value) + '", ')
                        else:
                            values += 'null, '
                    elif "Int" in column_info[index][1]:
                        checkValue = str(value).replace(' ', '')
                        if checkValue:
                            insertValue = str(int(value.replace(',', '')))
                        else:
                            insertValue = 'null'
                        values += (insertValue + ', ')
                    elif "Decimal" in column_info[index][1]:
                        checkValue = str(value).replace(' ', '')
                        if checkValue:
                            insertValue = str(float(value.replace(',', '')))
                        else:
                            insertValue = 'null'
                        values += (insertValue + ', ')
                    elif "Datetime" in column_info[index][1]:
                        try:
                            datetimeStr = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        except:
                            try:
                                datetimeStr = datetime.datetime.strptime(value, '%m/%d/%Y %H:%M:%S')
                            except:
                                try:
                                    datetimeStr = datetime.datetime.strptime(value, '%d/%m/%Y %H:%M:%S')
                                except:
                                    datetimeStr = ''
                        values += ('"' + str(datetimeStr) + '", ')
                    else:
                        try:
                            dateStr = datetime.datetime.strptime(value, '%Y-%m-%d')
                        except:
                            try:
                                dateStr = datetime.datetime.strptime(value, '%m/%d/%Y')
                            except:
                                try:
                                    dateStr = datetime.datetime.strptime(value, '%d/%m/%Y')
                                except:
                                    dateStr = ''
                        values += ('"' + str(dateStr) + '", ')
            if index < columnCount:
                while index < columnCount: 
                    values += 'null, '
                    index += 1
            values = values[:-2]
            query = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + values + ");"
            try:
                mycursor.execute(query)
                mydb.commit()
            except Exception as e:
                if not "Duplicate entry" in str(e):
                    err_in_query_run.append(["==========", query, e, columnContent])
        table_index += 1

    print(err_in_query_run, "===", len(err_in_query_run))
    mydb.close()

def main():
    sheetData = getData()
    pushData(sheetData)

if __name__ == '__main__':
    main()