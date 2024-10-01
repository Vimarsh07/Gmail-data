# main.py

from fastapi import FastAPI, HTTPException
import psycopg2
from email_utils import (
    connect_db,
    create_tables,
    process_email_message,
    get_gmail_service
)
import psycopg2.extras
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    print("Starting up...")
    try:
        app.state.conn = connect_db()
        create_tables(app.state.conn)
        app.state.cur = app.state.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print("Database connected and cursor created.")
        yield
    finally:
        # Shutdown code
        print("Shutting down...")
        app.state.cur.close()
        app.state.conn.close()
        print("Database connection closed.")

app = FastAPI(lifespan=lifespan)

@app.post("/process_emails")
async def process_emails():
    try:
        service = get_gmail_service()
        print("Gmail service initialized.")

        query = 'has:attachment'
        response = service.users().messages().list(userId='me', q=query, maxResults=500).execute()
        messages = response.get('messages', [])
        print(f"Initial messages fetched: {len(messages)}")

        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = service.users().messages().list(
                userId='me',
                q=query,
                maxResults=500,
                pageToken=page_token
            ).execute()
            messages.extend(response.get('messages', []))
            print(f"Messages fetched after pagination: {len(messages)}")

        if not messages:
            print("No messages found matching the query.")
            return {"status": "success", "processed_emails": 0}

        processed_count = 0

        for msg in messages:
            success = process_email_message(service, msg, app.state.cur)
            if success:
                processed_count += 1

        app.state.conn.commit()
        print(f"Total emails processed and stored: {processed_count}")

        return {"status": "success", "processed_emails": processed_count}
    except Exception as e:
        app.state.conn.rollback()
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
