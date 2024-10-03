# main.py

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import psycopg2
import psycopg2.extras
import os
from email_utils import (
    create_tables,
    process_email_message,
    get_gmail_service
)

app = FastAPI()

# Database dependency
def get_db_conn():
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set.")
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        yield conn
    finally:
        conn.close()

# Ensure tables are created at startup
@app.on_event("startup")
def startup_event():
    try:
        # Create a temporary connection to create tables
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set.")
        conn = psycopg2.connect(DATABASE_URL)
        create_tables(conn)
        conn.close()
        print("Startup: Tables created or verified successfully.")
    except Exception as e:
        print(f"Startup error: {e}")
        raise e

# Define Pydantic model for attachments
class AttachmentInfo(BaseModel):
    attachment_id: int
    email_id: int
    attachment_name: str
    sender: str
    subject: str

@app.post("/process_emails")
def process_emails(conn=Depends(get_db_conn)):
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
        cur = conn.cursor()

        for msg in messages:
            success = process_email_message(service, msg, conn)
            if success:
                processed_count += 1

        conn.commit()
        cur.close()
        print(f"Total emails processed and stored: {processed_count}")

        return {"status": "success", "processed_emails": processed_count}
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/attachments", response_model=List[AttachmentInfo])
def get_attachments(conn=Depends(get_db_conn)):
    try:
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
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

        attachments = [
            AttachmentInfo(
                attachment_id=row['attachment_id'],
                email_id=row['email_id'],
                attachment_name=row['attachment_name'],
                sender=row['sender'],
                subject=row['subject']
            )
            for row in results
        ]

        return attachments
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")
    finally:
        conn.close()
