# email_utils.py

import os
import base64
from bs4 import BeautifulSoup
import psycopg2
import re
import psycopg2.extras
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "email-db"
DB_USER = "postgres"
DB_PASS = "vimarsh1234"

def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Database connected successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise e

def create_tables(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    id SERIAL PRIMARY KEY,
                    message_id TEXT UNIQUE,
                    subject TEXT,
                    sender TEXT,
                    body TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS attachments (
                    id SERIAL PRIMARY KEY,
                    email_id INTEGER REFERENCES emails(id),
                    filename TEXT,
                    mime_type TEXT,
                    data BYTEA
                );
            """)
            conn.commit()
            print("Tables created or verified successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
        raise e

def get_attachment(service, user_id, msg_id, attachment_id, filename, mime_type, cur, email_id):
    """Get and store an attachment from a message into the database."""
    try:
        attachment = service.users().messages().attachments().get(
            userId=user_id, messageId=msg_id, id=attachment_id
        ).execute()

        data = attachment.get('data', '')
        file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))

        # Insert the attachment into the database
        sql = """
            INSERT INTO attachments (email_id, filename, mime_type, data)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(sql, (email_id, filename, mime_type, psycopg2.Binary(file_data)))
        print(f"Attachment {filename} stored in database.")
    except Exception as e:
        print(f"An error occurred while storing attachment {filename}: {e}")

def process_email_message(service, msg, cur):
    try:
        # Get the message details
        txt = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        print(f"Processing message ID: {msg['id']}")

        # Extract payload and headers
        payload = txt.get('payload', {})
        headers = payload.get('headers', [])

        # Initialize variables
        subject = ''
        sender = ''
        email_body = ''

        # Extract subject and sender from headers
        for d in headers:
            if d.get('name') == 'Subject':
                subject = d.get('value', '')
            elif d.get('name') == 'From':
                sender = d.get('value', '')

        # Process message parts
        body_parts = []
        attachments_info = []

        def process_parts(parts):
            for part in parts:
                mime_type = part.get('mimeType')
                filename = part.get('filename')
                body_data = part.get('body', {}).get('data')
                attachment_id = part.get('body', {}).get('attachmentId')

                if 'parts' in part:
                    process_parts(part['parts'])
                elif filename and attachment_id:
                    # This is an attachment
                    # Check if attachment is pdf or doc
                    if filename.lower().endswith(('.pdf', '.doc', '.docx')):
                        # Collect attachment info
                        attachments_info.append({
                            'filename': filename,
                            'mime_type': mime_type,
                            'attachment_id': attachment_id
                        })
                        print(f"Found matching attachment: {filename}")
                elif body_data:
                    # This is the email body
                    body_data = body_data.replace("-", "+").replace("_", "/")
                    decoded_data = base64.b64decode(body_data)
                    if mime_type == 'text/plain':
                        body_parts.append(decoded_data.decode('utf-8'))
                    elif mime_type == 'text/html':
                        soup = BeautifulSoup(decoded_data, "html.parser")
                        body_parts.append(soup.get_text())

        if 'parts' in payload:
            process_parts(payload['parts'])
        else:
            body_data = payload.get('body', {}).get('data')
            if body_data:
                body_data = body_data.replace("-", "+").replace("_", "/")
                decoded_data = base64.b64decode(body_data)
                email_body += decoded_data.decode('utf-8')

        # Combine email body parts
        if body_parts:
            email_body = ''.join(body_parts)

        # Check for keywords in email body
        if re.search(r'\b(resume|cv)\b', email_body, re.IGNORECASE):
            if attachments_info:
                # Insert email into database
                sql = """
                INSERT INTO emails (message_id, subject, sender, body)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id) DO NOTHING
                RETURNING id;
                """
                cur.execute(sql, (msg['id'], subject, sender, email_body))
                email_id_row = cur.fetchone()
                if email_id_row:
                    email_id = email_id_row[0]
                    print(f"Email inserted with ID: {email_id}")
                else:
                    # Email already exists, fetch its id
                    cur.execute("SELECT id FROM emails WHERE message_id = %s", (msg['id'],))
                    email_id = cur.fetchone()[0]
                    print(f"Email already exists with ID: {email_id}")

                # Now store attachments
                for attachment in attachments_info:
                    get_attachment(
                        service,
                        'me',
                        msg['id'],
                        attachment['attachment_id'],
                        attachment['filename'],
                        attachment['mime_type'],
                        cur,
                        email_id
                    )
                return True  # Email was processed and stored
            else:
                print(f"Email ID {msg['id']} has no PDF or DOC attachments; skipping.")
                return False
        else:
            print(f"Email ID {msg['id']} does not contain the keywords; skipping.")
            return False
    except Exception as e:
        print(f"An error occurred while processing message ID {msg['id']}: {e}")
        return False

def get_gmail_service():
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = None

    try:
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                print("Loaded credentials from token.pickle.")
        else:
            print("token.pickle not found.")

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                print("Credentials refreshed.")
            else:
                print("No valid credentials available.")
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                print("New credentials obtained.")

            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
                print("Credentials saved to token.pickle.")

        service = build('gmail', 'v1', credentials=creds)
        print("Gmail API service built successfully.")
        return service
    except Exception as e:
        print(f"An error occurred while setting up Gmail service: {e}")
        raise e