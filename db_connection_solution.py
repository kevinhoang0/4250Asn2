#-------------------------------------------------------------------------
# AUTHOR: Kevin Hoang
# FILENAME: db_connection.py
# SPECIFICATION: Database connection and functions
# FOR: CS 4250- Assignment #1
# TIME SPENT: 4 hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
import string

def connectDataBase():

    # Create a database connection object using psycopg2
       conn = psycopg2.connect(
        database="Testing",
        port="5432",
        user="postgres",
        host="localhost",
        password="123")
       return conn

def createCategory(cur,catId, catName):

    # Insert a category in the database
    cur.execute("INSERT INTO category (id, name) VALUES (%s, %s)", (catId, catName))

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT id FROM category WHERE name = %s", (docCat,))
    catId = cur.fetchone()[0]
    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    cur.execute("INSERT INTO document (id, text, title, date, category) VALUES (%s, %s, %s, %s, %s)",
    (docId, docText, docTitle, docDate, catId))

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    terms = set(docText.lower().translate(str.maketrans('', '', string.punctuation)).split())
    for term in terms:
        cur.execute("INSERT INTO term (term) VALUES (%s) ON CONFLICT DO NOTHING", (term,))

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    for term in terms:
        cur.execute("INSERT INTO inverted_index (term, document_id, count) VALUES (%s, %s, %s) "
                "ON CONFLICT (term, document_id) DO UPDATE SET count = inverted_index.count + 1",
                (term, docId, 1))

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("SELECT term FROM inverted_index WHERE document_id = %s", (docId,))
    terms = [row[0] for row in cur.fetchall()]
    for term in terms:
        cur.execute("DELETE FROM inverted_index WHERE term = %s AND document_id = %s", (term, docId))
    for term in terms:
        cur.execute("SELECT COUNT(*) FROM inverted_index WHERE term = %s", (term,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("DELETE FROM term WHERE term = %s", (term,))

    # 2 Delete the document from the database
    cur.execute("DELETE FROM document WHERE id = %s", (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    cur.execute("SELECT term, array_agg(document_id || ':' || count) FROM inverted_index GROUP BY term ORDER BY term")
    index = {row[0]: row[1] for row in cur.fetchall()}
    return index