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
import uvicorn
import os
from pydantic import BaseModel
from typing import List

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    print("Starting up...")
    try:
        app.state.conn = connect_db()
        create_tables(app.state.conn)
        app.state.cur = app.state.conn.cursor()
        print("Database connected and cursor created.")
        yield
    finally:
        # Shutdown code
        print("Shutting down...")
        if hasattr(app.state, 'cur') and app.state.cur:
            app.state.cur.close()
        if hasattr(app.state, 'conn') and app.state.conn:
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

class AttachmentInfo(BaseModel):
    attachment_id: int
    email_id: int
    attachment_name: str
    sender: str
    subject: str

@app.get("/attachments", response_model=List[AttachmentInfo])
async def get_attachments():
    try:
        # Query the database to get the attachment details
        sql = """
        SELECT
            attachments.id AS attachment_id,
            attachments.email_id,
            attachments.filename AS attachment_name,
            emails.sender,
            emails.subject
        FROM attachments
        JOIN emails ON attachments.email_id = emails.id
        ORDER BY attachments.id;
        """
        # Use a new cursor to avoid conflicts
        with app.state.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql)
            results = cur.fetchall()

        # Convert the results to a list of AttachmentInfo instances
        attachments = []
        for row in results:
            attachments.append(AttachmentInfo(
                attachment_id=row['attachment_id'],
                email_id=row['email_id'],
                attachment_name=row['attachment_name'],
                sender=row['sender'],
                subject=row['subject']
            ))

        return attachments
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")




if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8000))
    uvicorn.run("main:app", host='0.0.0.0', port=port, reload=True)
