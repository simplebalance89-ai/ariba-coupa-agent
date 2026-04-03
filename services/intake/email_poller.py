"""
email_poller.py — Microsoft Graph API email polling service.
Polls mailbox for new PO emails, extracts attachments, classifies source.
"""

import os
import base64
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

import aiohttp
from services.intake.email_classifier import classify_email, EmailClassification


# ── Configuration ────────────────────────────────────────────────────────────

GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
MAILBOX = os.environ.get("GRAPH_MAILBOX", "orders@enproinc.com")

# Support both naming conventions (AZURE_* and GRAPH_*)
TENANT_ID = os.environ.get("AZURE_TENANT_ID") or os.environ.get("GRAPH_TENANT_ID", "")
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID") or os.environ.get("GRAPH_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET") or os.environ.get("GRAPH_CLIENT_SECRET", "")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL", "60"))

# Folders
INBOX_FOLDER = "inbox"
PROCESSED_FOLDER = "Processed-PO"


# ── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class EmailAttachment:
    name: str
    content_type: str
    content_bytes: bytes
    size: int


@dataclass
class EmailMessage:
    message_id: str
    subject: str
    sender_email: str
    sender_name: str
    received_at: datetime
    body_preview: str
    has_attachments: bool
    attachments: List[EmailAttachment]
    classification: Optional[EmailClassification] = None


# ── Microsoft Graph Client ────────────────────────────────────────────────────

class GraphClient:
    """Async Microsoft Graph API client."""
    
    def __init__(self):
        self.access_token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        await self._ensure_token()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _ensure_token(self) -> str:
        """Get or refresh OAuth2 token."""
        if self.access_token and self.token_expires and datetime.utcnow() < self.token_expires:
            return self.access_token
        
        if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
            raise ValueError("Missing Azure AD credentials. Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET")
        
        token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
        
        async with self.session.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default"
            }
        ) as resp:
            if resp.status != 200:
                raise Exception(f"Token request failed: {resp.status}")
            
            data = await resp.json()
            self.access_token = data["access_token"]
            expires_in = data.get("expires_in", 3600)
            self.token_expires = datetime.utcnow() + timedelta(seconds=expires_in - 300)
            
            return self.access_token
    
    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated request to Graph API."""
        await self._ensure_token()
        
        url = f"{GRAPH_API_BASE}{endpoint}" if not endpoint.startswith("http") else endpoint
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        async with self.session.request(method, url, headers=headers, **kwargs) as resp:
            if resp.status == 204:
                return {}
            if resp.status >= 400:
                text = await resp.text()
                raise Exception(f"Graph API error {resp.status}: {text}")
            return await resp.json()
    
    async def get_messages(self, folder: str = "inbox", filter_query: Optional[str] = None, 
                          top: int = 10) -> List[Dict[str, Any]]:
        """Get messages from mailbox folder."""
        # For shared mailbox access: /users/{mailbox}/mailFolders/{folder}/messages
        endpoint = f"/users/{MAILBOX}/mailFolders/{folder}/messages"
        
        params = {"$top": top, "$orderby": "receivedDateTime desc"}
        if filter_query:
            params["$filter"] = filter_query
        
        # Build query string
        query = "&".join(f"{k}={v}" for k, v in params.items())
        full_endpoint = f"{endpoint}?{query}"
        
        data = await self._request("GET", full_endpoint)
        return data.get("value", [])
    
    async def get_attachments(self, message_id: str) -> List[Dict[str, Any]]:
        """Get attachments for a message."""
        endpoint = f"/users/{MAILBOX}/messages/{message_id}/attachments"
        data = await self._request("GET", endpoint)
        return data.get("value", [])
    
    async def download_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Download attachment content."""
        endpoint = f"/users/{MAILBOX}/messages/{message_id}/attachments/{attachment_id}/$value"
        await self._ensure_token()
        
        url = f"{GRAPH_API_BASE}{endpoint}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with self.session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Download failed: {resp.status}")
            return await resp.read()
    
    async def move_message(self, message_id: str, destination_folder: str) -> Dict[str, Any]:
        """Move message to another folder."""
        # First, get the folder ID
        folder_endpoint = f"/users/{MAILBOX}/mailFolders"
        folders = await self._request("GET", folder_endpoint)
        
        dest_folder_id = None
        for folder in folders.get("value", []):
            if folder["displayName"].lower() == destination_folder.lower():
                dest_folder_id = folder["id"]
                break
        
        if not dest_folder_id:
            # Create folder if not exists
            create_resp = await self._request("POST", folder_endpoint, 
                                             json={"displayName": destination_folder})
            dest_folder_id = create_resp["id"]
        
        # Move message
        move_endpoint = f"/users/{MAILBOX}/messages/{message_id}/move"
        return await self._request("POST", move_endpoint, 
                                   json={"destinationId": dest_folder_id})


# ── Email Poller Service ─────────────────────────────────────────────────────

class EmailPoller:
    """Polls mailbox for PO emails, processes them, moves to processed folder."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.processed_ids: set = set()
    
    async def poll_once(self, client: GraphClient) -> List[EmailMessage]:
        """Single poll iteration. Returns list of new PO emails."""
        self.logger.info(f"Polling {MAILBOX} inbox...")
        
        # Get unread messages or messages from last hour
        since = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
        filter_query = f"receivedDateTime ge {since}"
        
        messages = await client.get_messages(folder=INBOX_FOLDER, 
                                            filter_query=filter_query, 
                                            top=20)
        
        po_emails: List[EmailMessage] = []
        
        for msg in messages:
            msg_id = msg["id"]
            
            # Skip already processed
            if msg_id in self.processed_ids:
                continue
            
            # Parse sender
            from_data = msg.get("from", {}).get("emailAddress", {})
            sender_email = from_data.get("address", "")
            sender_name = from_data.get("name", "")
            
            subject = msg.get("subject", "")
            body_preview = msg.get("bodyPreview", "")
            received_str = msg.get("receivedDateTime", "")
            received_at = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
            has_attachments = msg.get("hasAttachments", False)
            
            # Classify source
            attachment_names = []
            if has_attachments:
                attachments_meta = await client.get_attachments(msg_id)
                attachment_names = [a["name"] for a in attachments_meta if "name" in a]
            
            classification = classify_email(sender_email, subject, attachment_names)
            
            # Skip if clearly not a PO (unless testing)
            if classification.source == "direct" and classification.confidence < 0.7:
                self.logger.debug(f"Skipping non-PO email: {subject}")
                continue
            
            # Download attachments
            attachments: List[EmailAttachment] = []
            if has_attachments:
                attachments_meta = await client.get_attachments(msg_id)
                for att_meta in attachments_meta:
                    if att_meta.get("@odata.type") == "#microsoft.graph.fileAttachment":
                        att_name = att_meta.get("name", "")
                        content_type = att_meta.get("contentType", "application/octet-stream")
                        
                        # Download content
                        try:
                            if "contentBytes" in att_meta:
                                content = base64.b64decode(att_meta["contentBytes"])
                            else:
                                content = await client.download_attachment(msg_id, att_meta["id"])
                            
                            attachments.append(EmailAttachment(
                                name=att_name,
                                content_type=content_type,
                                content_bytes=content,
                                size=len(content)
                            ))
                        except Exception as e:
                            self.logger.error(f"Failed to download attachment {att_name}: {e}")
            
            email_msg = EmailMessage(
                message_id=msg_id,
                subject=subject,
                sender_email=sender_email,
                sender_name=sender_name,
                received_at=received_at,
                body_preview=body_preview,
                has_attachments=has_attachments,
                attachments=attachments,
                classification=classification
            )
            
            po_emails.append(email_msg)
            self.processed_ids.add(msg_id)
            
            self.logger.info(f"Found PO email: {subject} | Source: {classification.source} | "
                           f"Format: {classification.format} | Confidence: {classification.confidence:.2f}")
        
        return po_emails
    
    async def run_continuous(self, callback=None):
        """Run continuous polling loop."""
        self.running = True
        
        async with GraphClient() as client:
            while self.running:
                try:
                    emails = await self.poll_once(client)
                    
                    for email in emails:
                        if callback:
                            await callback(email, client)
                        else:
                            await self._default_process(email, client)
                    
                except Exception as e:
                    self.logger.error(f"Poll error: {e}")
                
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    
    async def _default_process(self, email: EmailMessage, client: GraphClient):
        """Default processing: log and move to processed folder."""
        self.logger.info(f"Processing {email.subject}")
        
        # TODO: Route to parser based on format (cXML, PDF, text)
        
        # Move to processed folder
        try:
            await client.move_message(email.message_id, PROCESSED_FOLDER)
            self.logger.info(f"Moved to {PROCESSED_FOLDER}: {email.subject}")
        except Exception as e:
            self.logger.error(f"Failed to move message: {e}")
    
    def stop(self):
        """Stop polling."""
        self.running = False


# ── CLI Test ──────────────────────────────────────────────────────────────────

async def test_poll():
    """Test single poll (requires env vars)."""
    logging.basicConfig(level=logging.INFO)
    
    poller = EmailPoller()
    
    async with GraphClient() as client:
        emails = await poller.poll_once(client)
        
        print(f"\nFound {len(emails)} PO emails:\n")
        for email in emails:
            print(f"Subject: {email.subject}")
            print(f"From: {email.sender_email}")
            print(f"Source: {email.classification.source}")
            print(f"Format: {email.classification.format}")
            print(f"Attachments: {[a.name for a in email.attachments]}")
            print("-" * 50)


if __name__ == "__main__":
    asyncio.run(test_poll())
