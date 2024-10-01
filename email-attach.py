# Import required libraries
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os.path
import base64
from bs4 import BeautifulSoup
import psycopg2
import re  # For regex matching

# Define the SCOPES
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Database connection parameters
DB_HOST = "dpg-cru4bvtumphs73ehka9g-a"
DB_NAME = "email_db_jrfg"
DB_USER = "local"
DB_PASS = "mL6vCsYl9MPYZrpOkaWdFKQRUfLcUZy1"

def connect_db():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

def create_tables(conn):
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

def getEmails():
    creds = None

    # Authentication and token management
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    # Connect to the Gmail API
    service = build('gmail', 'v1', credentials=creds)

    # Connect to the PostgreSQL database and create the tables
    conn = connect_db()
    create_tables(conn)
    cur = conn.cursor()

    try:
        # Fetch emails that have attachments
        query = 'has:attachment'
        response = service.users().messages().list(userId='me', q=query, maxResults=500).execute()
        messages = response.get('messages', [])
        print(f"Total messages fetched with attachments: {len(messages)}")

        # Handle pagination
        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = service.users().messages().list(userId='me', q=query, maxResults=500, pageToken=page_token).execute()
            messages.extend(response.get('messages', []))

        print(f"Total messages after pagination: {len(messages)}")

        # Iterate through the messages
        for msg in messages:
            # Get the message details
            txt = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()

            try:
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
                        else:
                            # Email already exists, fetch its id
                            cur.execute("SELECT id FROM emails WHERE message_id = %s", (msg['id'],))
                            email_id = cur.fetchone()[0]
                        conn.commit()

                        # Now store attachments
                        for attachment in attachments_info:
                            get_attachment(service, 'me', msg['id'], attachment['attachment_id'], attachment['filename'], attachment['mime_type'], cur, email_id)
                        conn.commit()
                        print(f"Inserted email with subject: {subject}")
                    else:
                        print(f"Email ID {msg['id']} has no PDF or DOC attachments; skipping.")
                else:
                    print(f"Email ID {msg['id']} does not contain the keywords; skipping.")

            except Exception as e_inner:
                print(f"An error occurred while processing message ID {msg['id']}: {e_inner}")
                conn.rollback()
                # Proceed to the next message

    except Exception as e_outer:
        print(f"An error occurred while fetching messages: {e_outer}")

    finally:
        # Close database connection
        cur.close()
        conn.close()

getEmails()
