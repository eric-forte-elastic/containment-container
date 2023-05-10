import sqlite3
import structlog
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import StreamingResponse
from io import BytesIO

DB_NAME = "sqlite3.db"
TABLE_NAME = "blob_data"

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("startup")
    try:
        # Initialize database
        db = sqlite3.connect(DB_NAME)
        db.execute("CREATE TABLE IF NOT EXISTS blob_data ( blob_name TEXT PRIMARY KEY, blob_contents BLOB NOT NULL)")
        db.close()
        log.debug("Connected to db")
    except:
        log.error("Failed to connect to db")

    yield

    log.info("shutdown")


app = FastAPI(lifespan=lifespan)


def insert_blob(blob_name: str, blob_contents: bytes):
    try:
        sqliteConnection = sqlite3.connect(DB_NAME)
        cursor = sqliteConnection.cursor()
        log.debug("Connected to SQLite")
        sqlite_insert_blob_query = """ INSERT OR REPLACE INTO blob_data
                                  (blob_name, blob_contents) VALUES (?, ?)"""

        data_tuple = (blob_name, blob_contents)
        cursor.execute(sqlite_insert_blob_query, data_tuple)
        sqliteConnection.commit()
        log.debug(f"{blob_name} inserted successfully")
        cursor.close()

    except sqlite3.Error as error:
        log.error(f"Failed to insert {blob_name}", error=error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            log.debug("the sqlite connection is closed")


def read_blob_data(blob_name) -> bytes:
    try:
        sqliteConnection = sqlite3.connect(DB_NAME)
        cursor = sqliteConnection.cursor()
        log.debug("Connected to SQLite")

        sql_fetch_blob_query = """SELECT * from blob_data where blob_name = ?"""
        cursor.execute(sql_fetch_blob_query, (blob_name,))
        record = cursor.fetchone()
        if len(record) == 2:
            return record[1]
        else:
            log.error(f"Invalid number of rows. Expected one record with two rows, got {len(record)} rows")

        cursor.close()

    except sqlite3.Error as error:
        log.error("Failed to read blob data from sqlite table", error=error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            log.debug("sqlite connection is closed")


# GET endpoint for downloading a blob
# NOTE this returns the result as a stream in chunks
@app.get("/blobs/{blob_name:str}", response_class=StreamingResponse)
def read_file(blob_name: str) -> bytes:
    data = read_blob_data(blob_name)
    out_bytes = BytesIO(data)
    out_bytes.seek(0)
    return StreamingResponse(out_bytes)


# TODO support large files by reading in chunks
# PUT endpoint for uploading a file
# (the file will be in the body of the request)
@app.put("/upload")
def upload_file(file: UploadFile = File(...)):
    try:
        blob_contents = file.file.read()
        insert_blob(file.filename, blob_contents)
    except Exception as error:
        log.error(f"There was an error uploading the file", error=error)
        return {"message": f"There was an error uploading the file"}
    finally:
        file.file.close()

    log.debug(f"Successfully uploaded {file.filename}")
    return {"message": f"Successfully uploaded {file.filename}"}
